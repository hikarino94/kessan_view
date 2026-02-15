"""KessanView - 決算分析補助ツール

シングルページ構成のStreamlitアプリ。
左右2カラムレイアウトでランキングと詳細を同時閲覧可能。
"""
import json
import logging
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# パス設定
sys.path.insert(0, str(Path(__file__).resolve().parent))

import config
from db.database import get_session, init_db
from models.schemas import (
    AIAnalysisResult,
    DailyPrice,
    EarningsScore,
    FinancialStatement,
    Stock,
    TDnetDisclosure,
)
from services.financial_analysis import FinancialAnalyzer
from services.scoring import ScoringService

# ── ログ設定 ────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── ページ設定 ──────────────────────────────
st.set_page_config(
    page_title="KessanView - 決算分析補助ツール",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── カスタムCSS ─────────────────────────────
st.markdown("""
<style>
    .main .block-container {
        padding-top: 0.5rem;
        max-width: 1600px;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        border-radius: 10px;
        padding: 10px;
        border-left: 3px solid #667eea;
    }
    .section-header {
        background: linear-gradient(90deg, #667eea, #764ba2);
        color: white;
        padding: 6px 14px;
        border-radius: 8px;
        margin: 8px 0 6px 0;
        font-size: 16px;
        font-weight: bold;
    }
    /* ランキング選択行のハイライト */
    .selected-stock {
        background: #e8f0fe;
        border-radius: 6px;
        padding: 4px 8px;
        margin: 4px 0;
    }
</style>
""", unsafe_allow_html=True)

# ── DB初期化 ────────────────────────────────
init_db()

# ── セッションステート初期化 ────────────────
if "selected_code" not in st.session_state:
    st.session_state.selected_code = None


# ═══════════════════════════════════════════
# ヘルパー関数
# ═══════════════════════════════════════════
def get_forecast_progress_batch(codes: list, dt) -> dict:
    """複数銘柄の通期予想進捗率を一括取得"""
    session = get_session()
    try:
        stmts = (
            session.query(FinancialStatement)
            .filter(FinancialStatement.code.in_(codes), FinancialStatement.disclosed_date == dt)
            .all()
        )
        results = {}
        for stmt in stmts:
            period = stmt.type_of_current_period or ""
            standard = {"1Q": 25, "2Q": 50, "3Q": 75, "FY": 100}.get(period, 0)
            prog = {}
            for label, af, ff in [
                ("売上", "net_sales", "forecast_net_sales"),
                ("営利", "operating_profit", "forecast_operating_profit"),
                ("純利", "profit", "forecast_profit"),
            ]:
                actual = getattr(stmt, af, None)
                forecast = getattr(stmt, ff, None)
                if actual is not None and forecast and forecast != 0:
                    prog[label] = round(actual / forecast * 100, 1)
            results[stmt.code] = {"period": period, "standard": standard, **prog}
        return results
    finally:
        session.close()


def get_tdnet_map(dt) -> dict:
    """対象日のTDnet開示情報をcode→doc辞書で一括取得"""
    session = get_session()
    try:
        docs = (
            session.query(TDnetDisclosure)
            .filter(TDnetDisclosure.disclosed_date == dt, TDnetDisclosure.is_earnings_report == 1)
            .all()
        )
        result = {}
        for d in docs:
            if d.code:
                result[d.code] = {
                    "document_url": d.document_url or "",
                    "pdf_local_path": d.pdf_local_path or "",
                    "company_name": d.company_name or "",
                    "title": d.title or "",
                }
        return result
    finally:
        session.close()


def run_single_ai_analysis(code: str, dt_str: str):
    """単一銘柄のAI分析を実行"""
    from services.ai_analyzer import AIAnalyzer
    session = get_session()
    try:
        disclosure = (
            session.query(TDnetDisclosure)
            .filter(
                TDnetDisclosure.code == code,
                TDnetDisclosure.disclosed_date == datetime.strptime(dt_str, "%Y-%m-%d").date(),
                TDnetDisclosure.is_earnings_report == 1,
            )
            .first()
        )
        if not disclosure or not disclosure.pdf_local_path:
            return {"is_error": True, "error": "PDFが見つかりません"}
        pdf_path = disclosure.pdf_local_path
        company_name = disclosure.company_name or ""
    finally:
        session.close()

    analyzer = AIAnalyzer()
    return analyzer.analyze_and_save(
        pdf_path=pdf_path, code=code, disclosed_date=dt_str,
        disclosure_number="", company_name=company_name,
    )


def parse_ai_display(ai_result) -> dict:
    """AI分析結果を表示用に整形。壊れたJSON保存データもリカバリ"""
    summary_text = ai_result.summary or ""

    if summary_text.startswith("分析エラー:"):
        return {"is_error": True, "error": summary_text}

    display = {"summary": summary_text, "key_points": [], "keywords": [],
               "signal_words": [], "sentiment": ai_result.sentiment or "neutral"}

    if summary_text.lstrip().startswith(("{", "```")):
        # JSONが直接summaryに格納されたケースのリカバリ
        fence_m = re.search(r'```(?:json)?\s*\n?(\{.*)', summary_text, re.DOTALL)
        json_candidate = fence_m.group(1).rstrip('`').strip() if fence_m else summary_text.strip().lstrip('`').strip()
        if json_candidate.count("{") > json_candidate.count("}"):
            json_candidate += "}" * (json_candidate.count("{") - json_candidate.count("}"))
        try:
            parsed = json.loads(json_candidate)
            display["summary"] = parsed.get("summary", summary_text[:300])
            display["key_points"] = parsed.get("key_points", [])
            display["keywords"] = parsed.get("keywords", [])
            display["signal_words"] = parsed.get("signal_words", [])
            display["sentiment"] = parsed.get("sentiment", display["sentiment"])
        except (json.JSONDecodeError, AttributeError):
            sum_m = re.search(r'"summary"\s*:\s*"([^"]*)"', summary_text)
            if sum_m:
                display["summary"] = sum_m.group(1)
    else:
        # 正常データ
        for field, key in [("key_points", "key_points"), ("keywords", "keywords"), ("signal_words", "signal_words")]:
            raw = getattr(ai_result, field, None)
            if raw:
                try:
                    display[key] = json.loads(raw)
                except json.JSONDecodeError:
                    pass

    display["is_error"] = False
    return display


# ═══════════════════════════════════════════
# サイドバー: 設定
# ═══════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ 設定")

    st.subheader("📅 対象日付")
    try:
        default_date = datetime.strptime(config.DEV_TEST_DATE, "%Y-%m-%d").date()
    except Exception:
        default_date = date.today()

    target_date = st.date_input("分析対象日", value=default_date)
    target_date_str = target_date.strftime("%Y-%m-%d")

    st.divider()
    st.subheader("🔑 API設定")
    st.caption(f"J-Quants: {'✅' if config.JQUANTS_API_KEY else '❌'} | Gemini: {'✅' if config.GEMINI_API_KEY else '❌'}")

    st.divider()
    st.subheader("📊 スコアリング重み")
    w = dict(config.DEFAULT_SCORING_WEIGHTS)
    w["yoy_sales"] = st.slider("売上高YoY", 0.0, 1.0, w["yoy_sales"], 0.05)
    w["yoy_operating_income"] = st.slider("営業利益YoY", 0.0, 1.0, w["yoy_operating_income"], 0.05)
    w["yoy_profit"] = st.slider("純利益YoY", 0.0, 1.0, w["yoy_profit"], 0.05)
    w["qoq_acceleration"] = st.slider("QoQ加速度", 0.0, 1.0, w["qoq_acceleration"], 0.05)
    w["revision_flag"] = st.slider("業績修正", 0.0, 1.0, w["revision_flag"], 0.05)
    w["turnaround_flag"] = st.slider("赤黒転換", 0.0, 1.0, w["turnaround_flag"], 0.05)
    total_w = sum(w.values())
    if total_w > 0:
        w = {k: v / total_w for k, v in w.items()}

    st.divider()
    st.subheader("🔄 データ同期")
    sync_type = st.selectbox("同期タイプ", ["銘柄マスタ", "決算情報 (日付指定)", "株価 (日付指定)", "TDnet開示情報", "全て"])

    if st.button("▶️ 同期実行", type="primary", use_container_width=True):
        try:
            if sync_type in ["銘柄マスタ", "全て"]:
                from services.sync import SyncService
                with st.spinner("銘柄マスタ同期中..."):
                    st.success(f"銘柄マスタ: {SyncService().sync_listed_info()}件同期完了")
            if sync_type in ["決算情報 (日付指定)", "全て"]:
                from services.sync import SyncService
                with st.spinner(f"決算情報同期中... ({target_date_str})"):
                    st.success(f"決算情報: {SyncService().sync_statements_by_date(target_date_str)}件同期完了")
            if sync_type in ["株価 (日付指定)", "全て"]:
                from services.sync import SyncService
                with st.spinner(f"株価同期中... ({target_date_str})"):
                    st.success(f"株価: {SyncService().sync_daily_prices_by_date(target_date_str)}件同期完了")
            if sync_type in ["TDnet開示情報", "全て"]:
                from services.tdnet import TDnetClient
                tdnet = TDnetClient()
                with st.spinner(f"TDnet同期中... ({target_date_str})"):
                    disclosures = tdnet.get_disclosures_by_date(target_date_str)
                    st.success(f"TDnet: {tdnet.save_disclosures_to_db(disclosures, target_date_str)}件同期完了")
        except Exception as e:
            st.error(f"同期エラー: {e}")

    if st.button("📥 決算短信PDF一括DL", use_container_width=True):
        try:
            from services.tdnet import TDnetClient
            with st.spinner(f"PDFダウンロード中..."):
                results = TDnetClient().download_all_earnings_pdfs(target_date_str)
                st.success(f"PDF: {sum(1 for r in results if r['success'])}/{len(results)}件DL完了")
        except Exception as e:
            st.error(f"PDFダウンロードエラー: {e}")

    if st.button("🤖 AI分析一括実行", use_container_width=True):
        try:
            from services.ai_analyzer import AIAnalyzer
            analyzer = AIAnalyzer()
            session = get_session()
            try:
                dt_tmp = datetime.strptime(target_date_str, "%Y-%m-%d").date()
                items = [
                    {"pdf_path": d.pdf_local_path, "code": d.code,
                     "disclosed_date": target_date_str, "company_name": d.company_name}
                    for d in session.query(TDnetDisclosure).filter(
                        TDnetDisclosure.disclosed_date == dt_tmp,
                        TDnetDisclosure.is_earnings_report == 1,
                        TDnetDisclosure.pdf_local_path != "",
                    ).all()
                ]
            finally:
                session.close()
            if items:
                pb = st.progress(0, text="AI分析中...")
                results = analyzer.batch_analyze(items, progress_callback=lambda c, t: pb.progress(c / t, text=f"AI分析中... {c}/{t}"))
                st.success(f"AI分析: {sum(1 for r in results if r.get('success'))}/{len(results)}件完了")
            else:
                st.warning("分析対象のPDFがありません")
        except Exception as e:
            st.error(f"AI分析エラー: {e}")

    if st.button("📊 スコアリング実行", use_container_width=True):
        try:
            with st.spinner("スコアリング中..."):
                results = ScoringService(weights=w).score_all_for_date(target_date_str)
                st.success(f"スコアリング: {len(results)}件完了")
                st.rerun()
        except Exception as e:
            st.error(f"スコアリングエラー: {e}")


# ═══════════════════════════════════════════
# ヘッダー + サマリー
# ═══════════════════════════════════════════
st.markdown(f"## 📊 KessanView — {target_date_str}")

dt = datetime.strptime(target_date_str, "%Y-%m-%d").date()

session = get_session()
try:
    total_statements = session.query(FinancialStatement).filter(FinancialStatement.disclosed_date == dt).count()
    total_scores = session.query(EarningsScore).filter(EarningsScore.disclosed_date == dt).count()
    attention_count = session.query(EarningsScore).filter(EarningsScore.disclosed_date == dt, EarningsScore.category == "注目").count()
    check_count = session.query(EarningsScore).filter(EarningsScore.disclosed_date == dt, EarningsScore.category == "要確認").count()
    ai_count = session.query(AIAnalysisResult).filter(AIAnalysisResult.disclosed_date == dt).count()
    tdnet_count = session.query(TDnetDisclosure).filter(TDnetDisclosure.disclosed_date == dt, TDnetDisclosure.is_earnings_report == 1).count()
finally:
    session.close()

mc = st.columns(6)
mc[0].metric("📋 決算", f"{total_statements}件")
mc[1].metric("🏆 注目", f"{attention_count}件")
mc[2].metric("👁️ 要確認", f"{check_count}件")
mc[3].metric("📄 TDnet", f"{tdnet_count}件")
mc[4].metric("🤖 AI済", f"{ai_count}件")
mc[5].metric("📊 スコア", f"{total_scores}件")

# ═══════════════════════════════════════════
# データの一括取得
# ═══════════════════════════════════════════
tdnet_map = get_tdnet_map(dt)

# ランキング/一覧データの構築
ranking_rows = []
ranking_codes = []

if total_scores > 0:
    session = get_session()
    try:
        scores_data = (
            session.query(EarningsScore, Stock.name, Stock.sector_33_name)
            .outerjoin(Stock, EarningsScore.code == Stock.code)
            .filter(EarningsScore.disclosed_date == dt)
            .order_by(EarningsScore.total_score.desc())
            .all()
        )
    finally:
        session.close()

    all_codes = [s.code for s, _, _ in scores_data]
    progress_map = get_forecast_progress_batch(all_codes, dt)

    for score, name, sector in scores_data:
        prog = progress_map.get(score.code, {})
        prog_profit = prog.get("純利")
        tdoc = tdnet_map.get(score.code, {})
        ranking_rows.append({
            "score": round(score.total_score, 1),
            "category": score.category or "通常",
            "code": score.code,
            "name": name or "",
            "sector": (sector or "")[:6],
            "yoy_sales": f"{score.yoy_sales_change:+.1f}" if score.yoy_sales_change is not None else "-",
            "yoy_op": f"{score.yoy_op_change:+.1f}" if score.yoy_op_change is not None else "-",
            "yoy_profit": f"{score.yoy_profit_change:+.1f}" if score.yoy_profit_change is not None else "-",
            "progress": f"{prog_profit:.0f}" if prog_profit is not None else "-",
            "pdf": "✅" if tdoc.get("pdf_local_path") and Path(tdoc["pdf_local_path"]).exists() else "",
        })
        ranking_codes.append(score.code)
elif tdnet_count > 0:
    # スコアリング未実施: TDnetデータを表示
    for code, info in sorted(tdnet_map.items()):
        ranking_rows.append({
            "score": "-",
            "category": "-",
            "code": code,
            "name": info["company_name"] or "",
            "sector": "",
            "yoy_sales": "-",
            "yoy_op": "-",
            "yoy_profit": "-",
            "progress": "-",
            "pdf": "✅" if info["pdf_local_path"] and Path(info["pdf_local_path"]).exists() else "",
        })
        ranking_codes.append(code)


# ═══════════════════════════════════════════
# メインレイアウト: 左=ランキング / 右=詳細
# ═══════════════════════════════════════════
left_col, right_col = st.columns([2, 3], gap="medium")

# ───────────────────────────────────────────
# 左カラム: ランキング一覧 (2行カード形式)
# ───────────────────────────────────────────
with left_col:
    st.markdown('<div class="section-header">🏆 一覧</div>', unsafe_allow_html=True)

    if not ranking_rows:
        st.info("データがありません。サイドバーからデータを同期してください。")
    else:
        # フィルタ (コンパクト)
        if total_scores > 0:
            fc1, fc2 = st.columns(2)
            with fc1:
                min_score = st.slider("最低スコア", 0, 100, 0, 5, key="filter_score")
            with fc2:
                category_filter = st.multiselect("区分", ["注目", "要確認", "通常"], default=["注目", "要確認"], key="filter_cat")

            # フィルタ適用
            filtered_rows = []
            filtered_codes = []
            for row, code in zip(ranking_rows, ranking_codes):
                if row["score"] != "-" and row["score"] < min_score:
                    continue
                if category_filter and row["category"] not in category_filter:
                    continue
                filtered_rows.append(row)
                filtered_codes.append(code)
        else:
            filtered_rows = ranking_rows
            filtered_codes = ranking_codes

        # セレクトボックス (実際の選択用)
        code_label_map = {code: f"{row['code']} {row['name']}" for row, code in zip(filtered_rows, filtered_codes)}
        default_idx = 0
        if st.session_state.selected_code in filtered_codes:
            default_idx = filtered_codes.index(st.session_state.selected_code)

        selected = st.selectbox(
            f"銘柄選択 ({len(filtered_rows)}件)",
            options=filtered_codes,
            index=default_idx,
            format_func=lambda c: code_label_map.get(c, c),
            key="stock_selector",
        )
        st.session_state.selected_code = selected

        # 2行カードのHTML表示
        cat_colors = {"注目": "#e74c3c", "要確認": "#f39c12", "通常": "#95a5a6", "-": "#bbb"}
        cards_html = '<div style="max-height:580px;overflow-y:auto;border:1px solid #e0e0e0;border-radius:8px;">'
        for row, code in zip(filtered_rows, filtered_codes):
            is_sel = code == selected
            bg = "#e8f0fe" if is_sel else "#fff"
            border_l = "border-left:4px solid #667eea;" if is_sel else "border-left:4px solid transparent;"
            cat_c = cat_colors.get(row["category"], "#95a5a6")
            score_display = f'{row["score"]}' if row["score"] != "-" else "-"
            cards_html += f'''<div style="padding:5px 8px;border-bottom:1px solid #f0f0f0;background:{bg};{border_l}">
  <div style="display:flex;justify-content:space-between;align-items:center;line-height:1.3;">
    <span style="font-size:13px;"><b>{row["code"]}</b> {row["name"][:8]} <span style="color:#999;font-size:11px;">{row["sector"]}</span></span>
    <span style="display:flex;gap:3px;align-items:center;">
      <span style="background:{cat_c};color:white;padding:0 5px;border-radius:6px;font-size:11px;">{row["category"]}{score_display}</span>
      <span style="font-size:11px;">{row["pdf"]}</span>
    </span>
  </div>
  <div style="display:flex;gap:6px;font-size:11px;color:#555;line-height:1.3;">
    <span>売<b>{row["yoy_sales"]}</b></span>
    <span>営<b>{row["yoy_op"]}</b></span>
    <span>純<b>{row["yoy_profit"]}</b></span>
    <span>進捗<b>{row["progress"]}%</b></span>
  </div>
</div>'''
        cards_html += '</div>'
        st.markdown(cards_html, unsafe_allow_html=True)

        # 選択中の銘柄に対するアクションボタン
        if st.session_state.selected_code:
            sel = st.session_state.selected_code
            tdoc = tdnet_map.get(sel, {})

            ac1, ac2, ac3 = st.columns(3)
            with ac1:
                if tdoc.get("document_url"):
                    st.link_button("📄 TDnet", tdoc["document_url"], use_container_width=True)
                else:
                    st.button("📄 TDnet", disabled=True, use_container_width=True, key="left_tdnet_dis")
            with ac2:
                pp = tdoc.get("pdf_local_path", "")
                if pp and Path(pp).exists():
                    st.download_button("📥 PDF", data=Path(pp).read_bytes(),
                                       file_name=Path(pp).name, mime="application/pdf",
                                       use_container_width=True, key="left_pdf_dl")
                else:
                    st.button("📥 PDF", disabled=True, use_container_width=True, key="left_pdf_dis")
            with ac3:
                if st.button("🤖 AI分析", use_container_width=True, key="left_ai_btn"):
                    with st.spinner(f"{sel} AI分析中..."):
                        result = run_single_ai_analysis(sel, target_date_str)
                        if result.get("is_error"):
                            st.error(result.get("error", ""))
                        else:
                            st.success("完了!")
                    st.rerun()


# ───────────────────────────────────────────
# 右カラム: 銘柄詳細
# ───────────────────────────────────────────
with right_col:
    st.markdown('<div class="section-header">🔍 銘柄詳細</div>', unsafe_allow_html=True)

    selected_code = st.session_state.selected_code

    if not selected_code:
        st.info("← ランキングから銘柄を選択してください")
    else:
        # 銘柄情報取得
        session = get_session()
        try:
            stock = session.query(Stock).filter_by(code=selected_code).first()
            stock_name = stock.name if stock else ""
            stock_sector = stock.sector_33_name if stock else ""
            if not stock_name:
                ti = tdnet_map.get(selected_code, {})
                stock_name = ti.get("company_name", "")
        finally:
            session.close()

        st.markdown(f"### {selected_code} {stock_name}")
        if stock_sector:
            st.caption(f"セクター: {stock_sector}")

        # ── タブで整理 ──
        tab_financial, tab_ai, tab_docs = st.tabs(["📈 業績", "🤖 AI分析", "📄 開示資料"])

        # ─── タブ1: 業績 ───
        with tab_financial:
            session = get_session()
            try:
                all_stmts = (
                    session.query(FinancialStatement).filter_by(code=selected_code)
                    .order_by(FinancialStatement.current_period_end_date.asc()).all()
                )
                session.expunge_all()
            finally:
                session.close()

            if all_stmts:
                # チャート
                chart_data = []
                for s in all_stmts:
                    lbl = ""
                    if s.current_fiscal_year_end_date and s.type_of_current_period:
                        lbl = f"{s.current_fiscal_year_end_date.strftime('%Y')} {s.type_of_current_period}"
                    chart_data.append({"期間": lbl, "売上高": s.net_sales, "営業利益": s.operating_profit, "純利益": s.profit})

                cdf = pd.DataFrame(chart_data)
                if not cdf.empty and cdf["期間"].any():
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=cdf["期間"], y=cdf["売上高"], name="売上高", marker_color="#667eea"))
                    fig.add_trace(go.Scatter(x=cdf["期間"], y=cdf["営業利益"], name="営業利益",
                                             mode="lines+markers", line=dict(color="#e74c3c", width=3), yaxis="y2"))
                    fig.add_trace(go.Scatter(x=cdf["期間"], y=cdf["純利益"], name="純利益",
                                             mode="lines+markers", line=dict(color="#2ecc71", width=2, dash="dot"), yaxis="y2"))
                    fig.update_layout(
                        height=300, margin=dict(l=10, r=10, t=20, b=10),
                        yaxis=dict(title="売上高", side="left"),
                        yaxis2=dict(title="利益", overlaying="y", side="right"),
                        legend=dict(orientation="h", y=-0.2), hovermode="x unified",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # YoY/QoQ比較
                fa = FinancialAnalyzer()
                yoy = fa.compare_year_over_year(selected_code)
                qoq = fa.compare_quarter_over_quarter(selected_code)

                comp_col1, comp_col2 = st.columns(2)
                with comp_col1:
                    st.markdown("**前年同期比 (YoY)**")
                    for label, key in [("売上高", "yoy_net_sales"), ("営業利益", "yoy_operating_profit"), ("純利益", "yoy_profit")]:
                        v = yoy.get(key)
                        st.markdown(f"- {label}: **{v:+.1f}%**" if v is not None else f"- {label}: -")
                with comp_col2:
                    st.markdown("**前四半期比 (QoQ)**")
                    for label, key in [("売上高", "qoq_net_sales"), ("営業利益", "qoq_operating_profit"), ("純利益", "qoq_profit")]:
                        v = qoq.get(key)
                        st.markdown(f"- {label}: **{v:+.1f}%**" if v is not None else f"- {label}: -")

                # 通期予想進捗
                prog = get_forecast_progress_batch([selected_code], dt).get(selected_code, {})
                if prog and prog.get("standard"):
                    st.markdown("**📊 通期予想進捗**")
                    pc1, pc2, pc3 = st.columns(3)
                    for col, label in zip([pc1, pc2, pc3], ["売上", "営利", "純利"]):
                        val = prog.get(label)
                        std = prog["standard"]
                        if val is not None:
                            color = "🟢" if val >= std else ("🟡" if val >= std * 0.8 else "🔴")
                            col.metric(label, f"{color} {val:.0f}%", delta=f"標準{std}%")
                        else:
                            col.metric(label, "N/A")

                # シグナル
                signals = fa.detect_signals(selected_code)
                if signals:
                    st.markdown("**⚡ 検出シグナル**")
                    for sig in signals:
                        st.markdown(f"- {sig}")
            else:
                st.info("決算データなし（J-Quants決算情報を同期してください）")

        # ─── タブ2: AI分析 ───
        with tab_ai:
            session = get_session()
            try:
                ai_result = (
                    session.query(AIAnalysisResult).filter_by(code=selected_code)
                    .order_by(AIAnalysisResult.analyzed_at.desc()).first()
                )
                if ai_result:
                    session.expunge(ai_result)
            finally:
                session.close()

            if ai_result and ai_result.summary:
                display = parse_ai_display(ai_result)

                if display.get("is_error"):
                    st.warning(f"前回の分析でエラー: {display['error'][:100]}")
                    st.info("上のアクションバーから「🤖 AI分析」で再実行してください。")
                else:
                    sentiment_emoji = {"positive": "🟢 ポジティブ", "negative": "🔴 ネガティブ", "neutral": "🟡 ニュートラル"}
                    st.markdown(f"**センチメント:** {sentiment_emoji.get(display['sentiment'], '❓')}")

                    st.markdown("**📝 要約:**")
                    st.markdown(display["summary"])

                    if display["key_points"]:
                        st.markdown("**🔑 注目ポイント:**")
                        for kp in display["key_points"]:
                            st.markdown(f"- {kp}")

                    if display["keywords"]:
                        st.markdown("**🏷️ キーワード:**")
                        st.markdown(" ".join(
                            f'<span style="background:#667eea;color:white;padding:2px 8px;border-radius:10px;margin:2px;display:inline-block;font-size:12px">{kw}</span>'
                            for kw in display["keywords"]
                        ), unsafe_allow_html=True)

                    if display["signal_words"]:
                        st.markdown("**⚡ シグナルワード:**")
                        for sw in display["signal_words"]:
                            st.markdown(f"- {sw}")

                    st.caption(f"モデル: {ai_result.model_used} | 分析: {ai_result.analyzed_at}")
            else:
                st.info("AI分析結果なし。左側のアクションバーから「🤖 AI分析」で実行してください。")

        # ─── タブ3: 開示資料 ───
        with tab_docs:
            session = get_session()
            try:
                all_docs = (
                    session.query(TDnetDisclosure)
                    .filter(TDnetDisclosure.code == selected_code, TDnetDisclosure.disclosed_date == dt)
                    .all()
                )
                docs_data = [{"title": d.title, "document_url": d.document_url, "pdf_local_path": d.pdf_local_path} for d in all_docs]
            finally:
                session.close()

            if docs_data:
                for di, doc in enumerate(docs_data):
                    st.markdown(f"**{doc['title'] or '書類'}**")
                    dc1, dc2 = st.columns(2)
                    if doc["document_url"]:
                        dc1.link_button("🔗 TDnetで開く", doc["document_url"], use_container_width=True)
                    lp = doc.get("pdf_local_path", "")
                    if lp and Path(lp).exists():
                        dc2.download_button("📥 保存済PDF", data=Path(lp).read_bytes(),
                                            file_name=Path(lp).name, mime="application/pdf",
                                            use_container_width=True, key=f"doc_pdf_{di}")
                    else:
                        dc2.caption("未ダウンロード")
                    st.divider()
            else:
                st.info("TDnet開示データがありません")


# ═══════════════════════════════════════════
# フッター
# ═══════════════════════════════════════════
st.divider()
st.caption("KessanView — データ: J-Quants API / TDnet WEB-API | AI: Google Gemini")
