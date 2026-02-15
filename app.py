"""KessanView - 決算分析補助ツール

シングルページ構成のStreamlitアプリ。
スクロールで全情報を閲覧可能。設定はサイドバーに配置。
"""
import json
import logging
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
        padding-top: 1rem;
        max-width: 1400px;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        border-radius: 12px;
        padding: 16px;
        border-left: 4px solid #667eea;
    }
    .section-header {
        background: linear-gradient(90deg, #667eea, #764ba2);
        color: white;
        padding: 8px 16px;
        border-radius: 8px;
        margin: 16px 0 8px 0;
        font-size: 18px;
        font-weight: bold;
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
            .filter(
                FinancialStatement.code.in_(codes),
                FinancialStatement.disclosed_date == dt,
            )
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
            .filter(
                TDnetDisclosure.disclosed_date == dt,
                TDnetDisclosure.is_earnings_report == 1,
            )
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
        pdf_path=pdf_path,
        code=code,
        disclosed_date=dt_str,
        disclosure_number="",
        company_name=company_name,
    )


# ═══════════════════════════════════════════
# サイドバー: 設定
# ═══════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ 設定")

    st.subheader("📅 対象日付")
    try:
        default_date = datetime.strptime(config.DEV_TEST_DATE, "%Y-%m-%d").date()
    except:
        default_date = date.today()

    target_date = st.date_input("分析対象日", value=default_date, help="決算発表日を指定")
    target_date_str = target_date.strftime("%Y-%m-%d")

    st.divider()

    st.subheader("🔑 API設定")
    st.caption(f"J-Quants プラン: **{config.JQUANTS_PLAN.upper()}**")
    st.caption(f"レート制限: {config.JQUANTS_RATE_LIMITS.get(config.JQUANTS_PLAN, '?')} req/min")
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

    sync_type = st.selectbox(
        "同期タイプ",
        ["銘柄マスタ", "決算情報 (日付指定)", "株価 (日付指定)", "TDnet開示情報", "全て"],
    )

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
            with st.spinner(f"PDFダウンロード中... ({target_date_str})"):
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
st.title("📊 KessanView")
st.caption("決算分析補助ツール — 決算短信の効率的スクリーニング")

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

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("📅 対象日", target_date_str)
c2.metric("📋 決算発表", f"{total_statements}件")
c3.metric("🏆 注目", f"{attention_count}件")
c4.metric("👁️ 要確認", f"{check_count}件")
c5.metric("📄 TDnet", f"{tdnet_count}件")
c6.metric("🤖 AI分析済", f"{ai_count}件")


# ═══════════════════════════════════════════
# TDnet情報を一括取得 (全セクションで共有)
# ═══════════════════════════════════════════
tdnet_map = get_tdnet_map(dt)


# ═══════════════════════════════════════════
# セクション1: 重要度スコアランキング
# ═══════════════════════════════════════════
st.markdown('<div class="section-header">🏆 重要度スコアランキング</div>', unsafe_allow_html=True)

if total_scores == 0 and tdnet_count > 0:
    # ── スコアリング未実施: TDnet開示一覧を表示 ──
    st.info("📊 スコアリングデータなし。TDnet開示情報を一覧表示しています。")

    tdnet_rows = []
    for code, info in sorted(tdnet_map.items()):
        tdnet_rows.append({
            "コード": code,
            "企業名": info["company_name"],
            "タイトル": info["title"][:50],
            "PDF": "✅" if info["pdf_local_path"] and Path(info["pdf_local_path"]).exists() else "❌",
            "TDnet": "🔗" if info["document_url"] else "",
        })

    if tdnet_rows:
        tdnet_df = pd.DataFrame(tdnet_rows)
        st.caption(f"📄 TDnet開示情報（決算短信）: {len(tdnet_rows)}件 — 行を選択して詳細表示")

        event = st.dataframe(
            tdnet_df,
            width="stretch",
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            height=min(600, 35 * len(tdnet_rows) + 40),
            key="tdnet_ranking",
        )

        # 選択された行の銘柄を取得 + アクションボタン表示
        if event and event.selection and event.selection.rows:
            sel_idx = event.selection.rows[0]
            if sel_idx < len(tdnet_rows):
                sel_code = tdnet_rows[sel_idx]["コード"]
                st.session_state.selected_code = sel_code
                sel_info = tdnet_map.get(sel_code, {})

                st.markdown(f"**選択中: {sel_code} {sel_info.get('company_name', '')}**")
                btn_c1, btn_c2, btn_c3 = st.columns(3)
                with btn_c1:
                    if sel_info.get("document_url"):
                        st.link_button("📄 TDnetで開く", sel_info["document_url"], use_container_width=True)
                    else:
                        st.button("📄 TDnet未取得", disabled=True, use_container_width=True)
                with btn_c2:
                    pp = sel_info.get("pdf_local_path", "")
                    if pp and Path(pp).exists():
                        st.download_button("📥 PDFダウンロード", data=Path(pp).read_bytes(),
                                           file_name=Path(pp).name, mime="application/pdf",
                                           use_container_width=True, key="tdnet_sel_dl")
                    else:
                        st.button("📥 PDF未DL", disabled=True, use_container_width=True)
                with btn_c3:
                    if st.button("🤖 AI分析実行", use_container_width=True, key="tdnet_sel_ai"):
                        with st.spinner(f"{sel_code} AI分析中..."):
                            result = run_single_ai_analysis(sel_code, target_date_str)
                            if result.get("is_error"):
                                st.error(f"AI分析エラー: {result.get('error', '')}")
                            else:
                                st.success("AI分析完了!")
                        st.rerun()

elif total_scores == 0:
    st.info("📊 データがありません。サイドバーから「データ同期」を実行してください。")

else:
    # ── スコアランキング (st.dataframe) ──
    # フィルタ
    fc1, fc2, fc3 = st.columns([1, 1, 2])
    with fc1:
        min_score = st.slider("最低スコア", 0, 100, 0, 5)
    with fc2:
        category_filter = st.multiselect("カテゴリ", ["注目", "要確認", "通常"], default=["注目", "要確認"])
    with fc3:
        session = get_session()
        try:
            sectors = [r[0] for r in session.query(Stock.sector_33_name).distinct().all() if r[0]]
        finally:
            session.close()
        sector_filter = st.multiselect("セクター", sectors)

    # データ一括取得
    session = get_session()
    try:
        query = (
            session.query(EarningsScore, Stock.name, Stock.sector_33_name)
            .outerjoin(Stock, EarningsScore.code == Stock.code)
            .filter(EarningsScore.disclosed_date == dt, EarningsScore.total_score >= min_score)
        )
        if category_filter:
            query = query.filter(EarningsScore.category.in_(category_filter))
        scores_data = query.order_by(EarningsScore.total_score.desc()).all()
    finally:
        session.close()

    # 進捗率を一括取得
    all_codes = [s.code for s, _, _ in scores_data]
    progress_map = get_forecast_progress_batch(all_codes, dt)

    # DataFrameを構築
    rows = []
    row_codes = []
    for score, name, sector in scores_data:
        if sector_filter and sector not in sector_filter:
            continue
        prog = progress_map.get(score.code, {})
        prog_profit = prog.get("純利")
        std = prog.get("standard", 0)
        tdoc = tdnet_map.get(score.code, {})

        rows.append({
            "スコア": round(score.total_score, 1),
            "区分": score.category or "通常",
            "コード": score.code,
            "銘柄名": name or "",
            "セクター": (sector or "")[:8],
            "売上YoY%": f"{score.yoy_sales_change:+.1f}" if score.yoy_sales_change is not None else "-",
            "営利YoY%": f"{score.yoy_op_change:+.1f}" if score.yoy_op_change is not None else "-",
            "純利YoY%": f"{score.yoy_profit_change:+.1f}" if score.yoy_profit_change is not None else "-",
            "修正": "↑" if score.revision_flag == 1 else ("↓" if score.revision_flag == -1 else "-"),
            "転換": "黒" if score.turnaround_flag == 1 else ("赤" if score.turnaround_flag == -1 else "-"),
            "進捗%": f"{prog_profit:.0f}" if prog_profit is not None else "-",
            "期": prog.get("period", ""),
            "PDF": "✅" if tdoc.get("pdf_local_path") and Path(tdoc["pdf_local_path"]).exists() else ("❌" if tdoc else ""),
        })
        row_codes.append(score.code)

    if rows:
        df = pd.DataFrame(rows)
        st.caption(f"表示: {len(rows)}件 / 全{total_scores}件 — **行を選択して詳細表示・AI分析**")

        event = st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            height=min(600, 35 * len(rows) + 40),
            column_config={
                "スコア": st.column_config.ProgressColumn("スコア", min_value=0, max_value=100, format="%.1f"),
            },
            key="score_ranking",
        )

        # 選択された行の銘柄を取得 + アクションボタン表示
        if event and event.selection and event.selection.rows:
            sel_idx = event.selection.rows[0]
            if sel_idx < len(row_codes):
                sel_code = row_codes[sel_idx]
                st.session_state.selected_code = sel_code
                sel_tdoc = tdnet_map.get(sel_code, {})
                sel_name = rows[sel_idx].get("銘柄名", "")

                st.markdown(f"**選択中: {sel_code} {sel_name}**")
                btn_c1, btn_c2, btn_c3 = st.columns(3)
                with btn_c1:
                    if sel_tdoc.get("document_url"):
                        st.link_button("📄 TDnetで開く", sel_tdoc["document_url"], use_container_width=True)
                    else:
                        st.button("📄 TDnet未取得", disabled=True, use_container_width=True)
                with btn_c2:
                    pp = sel_tdoc.get("pdf_local_path", "")
                    if pp and Path(pp).exists():
                        st.download_button("📥 PDFダウンロード", data=Path(pp).read_bytes(),
                                           file_name=Path(pp).name, mime="application/pdf",
                                           use_container_width=True, key="score_sel_dl")
                    else:
                        st.button("📥 PDF未DL", disabled=True, use_container_width=True)
                with btn_c3:
                    if st.button("🤖 AI分析実行", use_container_width=True, key="score_sel_ai"):
                        with st.spinner(f"{sel_code} AI分析中..."):
                            result = run_single_ai_analysis(sel_code, target_date_str)
                            if result.get("is_error"):
                                st.error(f"AI分析エラー: {result.get('error', '')}")
                            else:
                                st.success("AI分析完了!")
                        st.rerun()
    else:
        st.info("フィルタ条件に一致するデータがありません")


# ═══════════════════════════════════════════
# セクション2: 銘柄詳細 (選択された銘柄)
# ═══════════════════════════════════════════
st.markdown('<div class="section-header">🔍 銘柄詳細</div>', unsafe_allow_html=True)

# 銘柄選択用のコード一覧を構築 (FinancialStatement + TDnet)
session = get_session()
try:
    codes_from_fs = set(
        r[0] for r in session.query(FinancialStatement.code)
        .filter(FinancialStatement.disclosed_date == dt).distinct().all()
    )
    codes_from_tdnet = set(
        r[0] for r in session.query(TDnetDisclosure.code)
        .filter(TDnetDisclosure.disclosed_date == dt, TDnetDisclosure.is_earnings_report == 1,
                TDnetDisclosure.code != None, TDnetDisclosure.code != "")
        .distinct().all()
    )
    codes_for_date = sorted(codes_from_fs | codes_from_tdnet)
finally:
    session.close()

if codes_for_date:
    # コード → 名前マッピング
    session = get_session()
    try:
        stock_options = {}
        for code in codes_for_date:
            stock = session.query(Stock).filter_by(code=code).first()
            if stock:
                stock_options[f"{code} {stock.name}"] = code
            else:
                tdnet_info = tdnet_map.get(code, {})
                stock_options[f"{code} {tdnet_info.get('company_name', '')}"] = code
    finally:
        session.close()

    options_list = list(stock_options.keys())
    codes_list = list(stock_options.values())
    default_idx = 0
    if st.session_state.selected_code and st.session_state.selected_code in codes_list:
        default_idx = codes_list.index(st.session_state.selected_code)

    selected_label = st.selectbox(
        "銘柄を選択（ランキング行クリックでも選択可能）",
        options=options_list,
        index=default_idx,
    )
    selected_code = stock_options.get(selected_label, "")
    if selected_code != st.session_state.selected_code:
        st.session_state.selected_code = selected_code

    if selected_code:
        # ── アクションバー ──
        acol1, acol2, acol3, acol4 = st.columns(4)

        with acol1:
            if st.button("🤖 この銘柄をAI分析", type="primary", use_container_width=True):
                with st.spinner(f"{selected_code} AI分析中..."):
                    result = run_single_ai_analysis(selected_code, target_date_str)
                    if result.get("is_error"):
                        st.error(f"AI分析エラー: {result.get('error', '不明')}")
                    else:
                        st.success("AI分析完了！")
                st.rerun()

        tdoc = tdnet_map.get(selected_code, {})
        with acol2:
            if tdoc.get("document_url"):
                st.link_button("📄 TDnetで開く", tdoc["document_url"], use_container_width=True)
            else:
                st.button("📄 TDnet未取得", disabled=True, use_container_width=True, key="detail_tdnet_disabled")

        with acol3:
            pdf_path = tdoc.get("pdf_local_path", "")
            if pdf_path and Path(pdf_path).exists():
                st.download_button(
                    "📥 PDFダウンロード",
                    data=Path(pdf_path).read_bytes(),
                    file_name=Path(pdf_path).name,
                    mime="application/pdf",
                    use_container_width=True,
                )
            else:
                st.button("📥 PDF未DL", disabled=True, use_container_width=True, key="detail_pdf_disabled")

        with acol4:
            prog = get_forecast_progress_batch([selected_code], dt).get(selected_code, {})
            if prog:
                pp = prog.get("純利")
                std = prog.get("standard", 0)
                period = prog.get("period", "")
                if pp is not None:
                    color = "🟢" if pp >= std else ("🟡" if pp >= std * 0.8 else "🔴")
                    st.metric(f"通期進捗 ({period})", f"{color} {pp:.0f}%", delta=f"標準{std}%")
                else:
                    st.metric(f"通期進捗 ({period})", "N/A")

        # ── 2カラム ──
        detail_col1, detail_col2 = st.columns([1, 1])

        # ── 左: 業績推移 ──
        with detail_col1:
            st.subheader("📈 四半期業績推移")
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
                    fig.add_trace(go.Scatter(x=cdf["期間"], y=cdf["営業利益"], name="営業利益", mode="lines+markers", line=dict(color="#e74c3c", width=3), yaxis="y2"))
                    fig.add_trace(go.Scatter(x=cdf["期間"], y=cdf["純利益"], name="純利益", mode="lines+markers", line=dict(color="#2ecc71", width=2, dash="dot"), yaxis="y2"))
                    fig.update_layout(
                        height=350, margin=dict(l=20, r=20, t=30, b=20),
                        yaxis=dict(title="売上高", side="left"),
                        yaxis2=dict(title="利益", overlaying="y", side="right"),
                        legend=dict(orientation="h", y=-0.15), hovermode="x unified",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # YoY/QoQ比較
                fa = FinancialAnalyzer()
                yoy = fa.compare_year_over_year(selected_code)
                qoq = fa.compare_quarter_over_quarter(selected_code)

                comp = {
                    "指標": ["売上高", "営業利益", "経常利益", "純利益"],
                    "YoY": [
                        f"{yoy.get('yoy_net_sales'):+.1f}%" if yoy.get("yoy_net_sales") is not None else "-",
                        f"{yoy.get('yoy_operating_profit'):+.1f}%" if yoy.get("yoy_operating_profit") is not None else "-",
                        f"{yoy.get('yoy_ordinary_profit'):+.1f}%" if yoy.get("yoy_ordinary_profit") is not None else "-",
                        f"{yoy.get('yoy_profit'):+.1f}%" if yoy.get("yoy_profit") is not None else "-",
                    ],
                    "QoQ": [
                        f"{qoq.get('qoq_net_sales'):+.1f}%" if qoq.get("qoq_net_sales") is not None else "-",
                        f"{qoq.get('qoq_operating_profit'):+.1f}%" if qoq.get("qoq_operating_profit") is not None else "-",
                        f"{qoq.get('qoq_ordinary_profit'):+.1f}%" if qoq.get("qoq_ordinary_profit") is not None else "-",
                        f"{qoq.get('qoq_profit'):+.1f}%" if qoq.get("qoq_profit") is not None else "-",
                    ],
                }
                st.dataframe(pd.DataFrame(comp), width="stretch", hide_index=True)

                # 進捗テーブル
                if prog and prog.get("standard"):
                    st.markdown("**📊 通期予想進捗**")
                    pdata = {
                        "指標": ["売上高", "営業利益", "純利益"],
                        "進捗率": [
                            f"{prog.get('売上', 0):.1f}%" if prog.get("売上") is not None else "-",
                            f"{prog.get('営利', 0):.1f}%" if prog.get("営利") is not None else "-",
                            f"{prog.get('純利', 0):.1f}%" if prog.get("純利") is not None else "-",
                        ],
                        "標準": [f"{prog['standard']}%"] * 3,
                    }
                    st.dataframe(pd.DataFrame(pdata), width="stretch", hide_index=True)

                # シグナル
                signals = fa.detect_signals(selected_code)
                if signals:
                    st.subheader("⚡ 検出シグナル")
                    for sig in signals:
                        st.markdown(f"- {sig}")
            else:
                st.info("決算データがありません（J-Quants決算情報を同期してください）")

        # ── 右: AI分析 + 開示資料 ──
        with detail_col2:
            st.subheader("🤖 AI分析結果")
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
                summary_text = ai_result.summary
                # エラー結果は再分析を促す
                if summary_text.startswith("分析エラー:"):
                    st.warning(f"前回の分析でエラーが発生しています: {summary_text[:100]}")
                    st.info("「🤖 この銘柄をAI分析」ボタンで再分析してください。")
                else:
                    # summaryにJSON文字列が格納されている場合のリカバリ
                    display_summary = summary_text
                    display_kps = []
                    display_kws = []
                    display_sws = []
                    display_sentiment = ai_result.sentiment or "neutral"

                    if summary_text.lstrip().startswith(("{", "```")):
                        import re
                        # JSONを抽出して正しいフィールドを取得
                        fence_m = re.search(r'```(?:json)?\s*\n?(\{.*)', summary_text, re.DOTALL)
                        json_candidate = fence_m.group(1).rstrip('`').strip() if fence_m else summary_text.strip().lstrip('`').strip()
                        if json_candidate.count("{") > json_candidate.count("}"):
                            json_candidate += "}" * (json_candidate.count("{") - json_candidate.count("}"))
                        try:
                            parsed = json.loads(json_candidate)
                            display_summary = parsed.get("summary", summary_text[:300])
                            display_kps = parsed.get("key_points", [])
                            display_kws = parsed.get("keywords", [])
                            display_sws = parsed.get("signal_words", [])
                            display_sentiment = parsed.get("sentiment", display_sentiment)
                        except (json.JSONDecodeError, AttributeError):
                            # regex抽出にも失敗 — summaryキーだけ取得
                            sum_m = re.search(r'"summary"\s*:\s*"([^"]*)"', summary_text)
                            if sum_m:
                                display_summary = sum_m.group(1)
                    else:
                        # 正常な保存データ
                        try:
                            display_kps = json.loads(ai_result.key_points) if ai_result.key_points else []
                        except json.JSONDecodeError:
                            display_kps = []
                        try:
                            display_kws = json.loads(ai_result.keywords) if ai_result.keywords else []
                        except json.JSONDecodeError:
                            display_kws = []
                        try:
                            display_sws = json.loads(ai_result.signal_words) if ai_result.signal_words else []
                        except json.JSONDecodeError:
                            display_sws = []

                    sentiment_emoji = {"positive": "🟢 ポジティブ", "negative": "🔴 ネガティブ", "neutral": "🟡 ニュートラル"}
                    st.markdown(f"**センチメント:** {sentiment_emoji.get(display_sentiment, '❓')}")
                    st.markdown("**📝 要約:**")
                    st.markdown(display_summary)

                    if display_kps:
                        st.markdown("**🔑 注目ポイント:**")
                        for kp in display_kps:
                            st.markdown(f"- {kp}")

                    if display_kws:
                        st.markdown("**🏷️ キーワード:**")
                        st.markdown(" ".join(
                            f'<span style="background:#667eea;color:white;padding:2px 8px;border-radius:10px;margin:2px;display:inline-block;font-size:12px">{kw}</span>'
                            for kw in display_kws
                        ), unsafe_allow_html=True)

                    if display_sws:
                        st.markdown("**⚡ シグナルワード:**")
                        for sw in display_sws:
                            st.markdown(f"- {sw}")

                    st.caption(f"モデル: {ai_result.model_used} | 分析: {ai_result.analyzed_at}")
            else:
                st.info("AI分析結果なし。「🤖 この銘柄をAI分析」ボタンで実行してください。")

            # 開示資料一覧
            st.subheader("📄 開示資料")
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
                    with st.expander(doc["title"] or "書類"):
                        bc1, bc2 = st.columns(2)
                        if doc["document_url"]:
                            bc1.link_button("🔗 TDnetで開く", doc["document_url"], use_container_width=True)
                        lp = doc.get("pdf_local_path", "")
                        if lp and Path(lp).exists():
                            bc2.download_button(
                                "📥 保存済PDFを取得", data=Path(lp).read_bytes(),
                                file_name=Path(lp).name, mime="application/pdf",
                                use_container_width=True, key=f"dl_{selected_code}_{di}",
                            )
                        else:
                            bc2.caption("未ダウンロード")
            else:
                st.info("TDnet開示データがありません")
else:
    st.info("対象日のデータがありません。サイドバーからデータを同期してください。")


# ═══════════════════════════════════════════
# セクション3: AI分析サマリー一覧
# ═══════════════════════════════════════════
st.markdown('<div class="section-header">🤖 AI分析サマリー一覧</div>', unsafe_allow_html=True)

session = get_session()
try:
    ai_all = (
        session.query(AIAnalysisResult, Stock.name)
        .outerjoin(Stock, AIAnalysisResult.code == Stock.code)
        .filter(AIAnalysisResult.disclosed_date == dt).all()
    )
finally:
    session.close()

if ai_all:
    ai_rows = []
    error_count = 0
    for ai, name in ai_all:
        summary = ai.summary or ""
        # エラー結果はスキップ
        if summary.startswith("分析エラー:"):
            error_count += 1
            continue
        try:
            kws = json.loads(ai.keywords) if ai.keywords else []
        except json.JSONDecodeError:
            kws = []
        sentiment_map = {"positive": "🟢 ポジティブ", "negative": "🔴 ネガティブ", "neutral": "🟡 ニュートラル"}
        ai_rows.append({
            "コード": ai.code,
            "銘柄名": name or "",
            "センチメント": sentiment_map.get(ai.sentiment, ai.sentiment or ""),
            "要約": summary[:120] + ("..." if len(summary) > 120 else ""),
            "キーワード": ", ".join(kws[:5]) if kws else "",
        })
    if ai_rows:
        st.dataframe(pd.DataFrame(ai_rows), width="stretch", hide_index=True)
    if error_count > 0:
        st.caption(f"⚠️ {error_count}件のエラー結果は非表示（API制限等）")
    if not ai_rows and error_count == 0:
        st.info("AI分析結果がありません")
else:
    st.info("AI分析結果がありません")


# ═══════════════════════════════════════════
# フッター
# ═══════════════════════════════════════════
st.divider()
st.caption("KessanView — データ: J-Quants API / TDnet WEB-API | AI: Google Gemini")
