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
import plotly.express as px
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
    /* メインコンテンツ */
    .main .block-container {
        padding-top: 1rem;
        max-width: 1400px;
    }

    /* スコアバッジ */
    .score-badge-attention {
        background: linear-gradient(135deg, #ff6b6b, #ee5a24);
        color: white;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: bold;
        font-size: 14px;
    }
    .score-badge-check {
        background: linear-gradient(135deg, #feca57, #ff9f43);
        color: #333;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: bold;
        font-size: 14px;
    }
    .score-badge-normal {
        background: linear-gradient(135deg, #dfe6e9, #b2bec3);
        color: #333;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 14px;
    }

    /* シグナル */
    .signal-positive { color: #e74c3c; font-weight: bold; }
    .signal-negative { color: #3498db; font-weight: bold; }

    /* メトリクスカード */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        border-radius: 12px;
        padding: 16px;
        border-left: 4px solid #667eea;
    }

    /* テーブルスタイル */
    .dataframe { font-size: 13px !important; }

    /* セクションヘッダー */
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

    target_date = st.date_input(
        "分析対象日",
        value=default_date,
        help="決算発表日を指定してください",
    )
    target_date_str = target_date.strftime("%Y-%m-%d")

    st.divider()

    st.subheader("🔑 API設定")
    st.caption(f"J-Quants プラン: **{config.JQUANTS_PLAN.upper()}**")
    st.caption(f"レート制限: {config.JQUANTS_RATE_LIMITS.get(config.JQUANTS_PLAN, '?')} req/min")
    jquants_key_set = "✅ 設定済み" if config.JQUANTS_API_KEY else "❌ 未設定"
    gemini_key_set = "✅ 設定済み" if config.GEMINI_API_KEY else "❌ 未設定"
    st.caption(f"J-Quants API キー: {jquants_key_set}")
    st.caption(f"Gemini API キー: {gemini_key_set}")

    st.divider()

    st.subheader("📊 スコアリング重み")
    w = dict(config.DEFAULT_SCORING_WEIGHTS)
    w["yoy_sales"] = st.slider("売上高YoY", 0.0, 1.0, w["yoy_sales"], 0.05)
    w["yoy_operating_income"] = st.slider("営業利益YoY", 0.0, 1.0, w["yoy_operating_income"], 0.05)
    w["yoy_profit"] = st.slider("純利益YoY", 0.0, 1.0, w["yoy_profit"], 0.05)
    w["qoq_acceleration"] = st.slider("QoQ加速度", 0.0, 1.0, w["qoq_acceleration"], 0.05)
    w["revision_flag"] = st.slider("業績修正", 0.0, 1.0, w["revision_flag"], 0.05)
    w["turnaround_flag"] = st.slider("赤黒転換", 0.0, 1.0, w["turnaround_flag"], 0.05)

    # 合計を正規化
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
            if sync_type == "銘柄マスタ" or sync_type == "全て":
                from services.sync import SyncService
                sync = SyncService()
                with st.spinner("銘柄マスタ同期中..."):
                    count = sync.sync_listed_info()
                    st.success(f"銘柄マスタ: {count}件同期完了")

            if sync_type in ["決算情報 (日付指定)", "全て"]:
                from services.sync import SyncService
                sync = SyncService()
                with st.spinner(f"決算情報同期中... ({target_date_str})"):
                    count = sync.sync_statements_by_date(target_date_str)
                    st.success(f"決算情報: {count}件同期完了")

            if sync_type in ["株価 (日付指定)", "全て"]:
                from services.sync import SyncService
                sync = SyncService()
                with st.spinner(f"株価同期中... ({target_date_str})"):
                    count = sync.sync_daily_prices_by_date(target_date_str)
                    st.success(f"株価: {count}件同期完了")

            if sync_type in ["TDnet開示情報", "全て"]:
                from services.tdnet import TDnetClient
                tdnet = TDnetClient()
                with st.spinner(f"TDnet同期中... ({target_date_str})"):
                    disclosures = tdnet.get_disclosures_by_date(target_date_str)
                    count = tdnet.save_disclosures_to_db(disclosures, target_date_str)
                    st.success(f"TDnet: {count}件同期完了")

        except Exception as e:
            st.error(f"同期エラー: {e}")

    if st.button("📥 決算短信PDF一括DL", use_container_width=True):
        try:
            from services.tdnet import TDnetClient
            tdnet = TDnetClient()
            with st.spinner(f"PDFダウンロード中... ({target_date_str})"):
                results = tdnet.download_all_earnings_pdfs(target_date_str)
                success = sum(1 for r in results if r["success"])
                st.success(f"PDF: {success}/{len(results)}件ダウンロード完了")
        except Exception as e:
            st.error(f"PDFダウンロードエラー: {e}")

    if st.button("🤖 AI分析実行", use_container_width=True):
        try:
            from services.ai_analyzer import AIAnalyzer
            analyzer = AIAnalyzer()
            session = get_session()
            try:
                dt = datetime.strptime(target_date_str, "%Y-%m-%d").date()
                disclosures = (
                    session.query(TDnetDisclosure)
                    .filter(
                        TDnetDisclosure.disclosed_date == dt,
                        TDnetDisclosure.is_earnings_report == 1,
                        TDnetDisclosure.pdf_local_path != "",
                    )
                    .all()
                )
                items = [
                    {
                        "pdf_path": d.pdf_local_path,
                        "code": d.code,
                        "disclosed_date": target_date_str,
                        "company_name": d.company_name,
                    }
                    for d in disclosures
                ]
            finally:
                session.close()

            if items:
                progress_bar = st.progress(0, text="AI分析中...")
                def update_progress(current, total):
                    progress_bar.progress(current / total, text=f"AI分析中... {current}/{total}")
                results = analyzer.batch_analyze(items, progress_callback=update_progress)
                success = sum(1 for r in results if r.get("success"))
                st.success(f"AI分析: {success}/{len(results)}件完了")
            else:
                st.warning("分析対象のPDFがありません")
        except Exception as e:
            st.error(f"AI分析エラー: {e}")

    if st.button("📊 スコアリング実行", use_container_width=True):
        try:
            scorer = ScoringService(weights=w)
            with st.spinner("スコアリング中..."):
                results = scorer.score_all_for_date(target_date_str)
                st.success(f"スコアリング: {len(results)}件完了")
                st.rerun()
        except Exception as e:
            st.error(f"スコアリングエラー: {e}")


# ═══════════════════════════════════════════
# ヘッダー
# ═══════════════════════════════════════════
st.title("📊 KessanView")
st.caption("決算分析補助ツール — 決算短信の効率的スクリーニング")

# ── サマリーメトリクス ──────────────────────
session = get_session()
try:
    dt = datetime.strptime(target_date_str, "%Y-%m-%d").date()

    total_statements = session.query(FinancialStatement).filter(
        FinancialStatement.disclosed_date == dt
    ).count()

    total_scores = session.query(EarningsScore).filter(
        EarningsScore.disclosed_date == dt
    ).count()

    attention_count = session.query(EarningsScore).filter(
        EarningsScore.disclosed_date == dt,
        EarningsScore.category == "注目",
    ).count()

    check_count = session.query(EarningsScore).filter(
        EarningsScore.disclosed_date == dt,
        EarningsScore.category == "要確認",
    ).count()

    ai_count = session.query(AIAnalysisResult).filter(
        AIAnalysisResult.disclosed_date == dt,
    ).count()

    tdnet_count = session.query(TDnetDisclosure).filter(
        TDnetDisclosure.disclosed_date == dt,
        TDnetDisclosure.is_earnings_report == 1,
    ).count()
finally:
    session.close()

col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.metric("📅 対象日", target_date_str)
with col2:
    st.metric("📋 決算発表", f"{total_statements}件")
with col3:
    st.metric("🏆 注目", f"{attention_count}件")
with col4:
    st.metric("👁️ 要確認", f"{check_count}件")
with col5:
    st.metric("📄 TDnet", f"{tdnet_count}件")
with col6:
    st.metric("🤖 AI分析済", f"{ai_count}件")


# ═══════════════════════════════════════════
# セクション1: 重要度スコアランキング
# ═══════════════════════════════════════════
st.markdown('<div class="section-header">🏆 重要度スコアランキング</div>', unsafe_allow_html=True)

if total_scores == 0:
    st.info("📊 スコアリングデータがありません。サイドバーから「データ同期」→「スコアリング実行」を行ってください。")
else:
    # フィルタ
    filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 2])
    with filter_col1:
        min_score = st.slider("最低スコア", 0, 100, 0, 5)
    with filter_col2:
        category_filter = st.multiselect(
            "カテゴリ",
            ["注目", "要確認", "通常"],
            default=["注目", "要確認"],
        )
    with filter_col3:
        # セクター一覧を取得
        session = get_session()
        try:
            sectors = [r[0] for r in session.query(Stock.sector_33_name).distinct().all() if r[0]]
        finally:
            session.close()
        sector_filter = st.multiselect("セクター", sectors)

    # スコアデータ取得
    session = get_session()
    try:
        query = (
            session.query(EarningsScore, Stock.name, Stock.sector_33_name, Stock.market_name)
            .outerjoin(Stock, EarningsScore.code == Stock.code)
            .filter(
                EarningsScore.disclosed_date == dt,
                EarningsScore.total_score >= min_score,
            )
        )
        if category_filter:
            query = query.filter(EarningsScore.category.in_(category_filter))

        scores_with_info = query.order_by(EarningsScore.total_score.desc()).all()
    finally:
        session.close()

    if scores_with_info:
        # DataFrameに変換
        rows = []
        for score, name, sector, market in scores_with_info:
            if sector_filter and sector not in sector_filter:
                continue
            rows.append({
                "スコア": score.total_score,
                "カテゴリ": score.category,
                "コード": score.code,
                "銘柄名": name or "",
                "セクター": sector or "",
                "市場": market or "",
                "売上YoY%": f"{score.yoy_sales_change:+.1f}" if score.yoy_sales_change is not None else "-",
                "営利YoY%": f"{score.yoy_op_change:+.1f}" if score.yoy_op_change is not None else "-",
                "純利YoY%": f"{score.yoy_profit_change:+.1f}" if score.yoy_profit_change is not None else "-",
                "QoQ加速": f"{score.qoq_acceleration:+.1f}" if score.qoq_acceleration is not None else "-",
                "修正": "↑" if score.revision_flag == 1 else ("↓" if score.revision_flag == -1 else "-"),
                "転換": "黒" if score.turnaround_flag == 1 else ("赤" if score.turnaround_flag == -1 else "-"),
            })

        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(
                df,
                width="stretch",
                height=min(400, 35 * len(rows) + 40),
                column_config={
                    "スコア": st.column_config.ProgressColumn(
                        "スコア", min_value=0, max_value=100, format="%.1f"
                    ),
                },
            )
            st.caption(f"表示件数: {len(rows)}件 / 全{total_scores}件")
        else:
            st.info("フィルタ条件に一致するデータがありません")
    else:
        st.info("該当データがありません")


# ═══════════════════════════════════════════
# セクション2: 銘柄詳細
# ═══════════════════════════════════════════
st.markdown('<div class="section-header">🔍 銘柄詳細</div>', unsafe_allow_html=True)

# 銘柄選択
session = get_session()
try:
    codes_for_date = [
        r[0]
        for r in session.query(FinancialStatement.code)
        .filter(FinancialStatement.disclosed_date == dt)
        .distinct()
        .all()
    ]
finally:
    session.close()

if codes_for_date:
    # コード+名前のリスト
    session = get_session()
    try:
        stock_options = {}
        for code in codes_for_date:
            stock = session.query(Stock).filter_by(code=code).first()
            name = stock.name if stock else ""
            stock_options[f"{code} {name}"] = code
    finally:
        session.close()

    selected_label = st.selectbox(
        "銘柄を選択",
        options=list(stock_options.keys()),
        index=0,
        help="決算発表銘柄から選択してください",
    )
    selected_code = stock_options.get(selected_label, "")

    if selected_code:
        detail_col1, detail_col2 = st.columns([1, 1])

        # ── 左カラム: 決算情報 ──────
        with detail_col1:
            st.subheader("📈 四半期業績推移")
            session = get_session()
            try:
                all_statements = (
                    session.query(FinancialStatement)
                    .filter_by(code=selected_code)
                    .order_by(FinancialStatement.current_period_end_date.asc())
                    .all()
                )
                session.expunge_all()
            finally:
                session.close()

            if all_statements:
                chart_data = []
                for s in all_statements:
                    period_label = ""
                    if s.current_fiscal_year_end_date and s.type_of_current_period:
                        fy = s.current_fiscal_year_end_date.strftime("%Y")
                        period_label = f"{fy} {s.type_of_current_period}"

                    chart_data.append({
                        "期間": period_label,
                        "売上高": s.net_sales,
                        "営業利益": s.operating_profit,
                        "純利益": s.profit,
                    })

                chart_df = pd.DataFrame(chart_data)

                if not chart_df.empty and chart_df["期間"].any():
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=chart_df["期間"], y=chart_df["売上高"],
                        name="売上高", marker_color="#667eea",
                    ))
                    fig.add_trace(go.Scatter(
                        x=chart_df["期間"], y=chart_df["営業利益"],
                        name="営業利益", mode="lines+markers",
                        line=dict(color="#e74c3c", width=3),
                        yaxis="y2",
                    ))
                    fig.add_trace(go.Scatter(
                        x=chart_df["期間"], y=chart_df["純利益"],
                        name="純利益", mode="lines+markers",
                        line=dict(color="#2ecc71", width=2, dash="dot"),
                        yaxis="y2",
                    ))
                    fig.update_layout(
                        height=350,
                        margin=dict(l=20, r=20, t=30, b=20),
                        yaxis=dict(title="売上高", side="left"),
                        yaxis2=dict(title="利益", overlaying="y", side="right"),
                        legend=dict(orientation="h", y=-0.15),
                        hovermode="x unified",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # 前Q/前Y比較テーブル
                analyzer = FinancialAnalyzer()
                yoy = analyzer.compare_year_over_year(selected_code)
                qoq = analyzer.compare_quarter_over_quarter(selected_code)

                comparison_data = {
                    "指標": ["売上高", "営業利益", "経常利益", "純利益"],
                    "前年同期比(YoY)": [
                        f"{yoy.get('yoy_net_sales', '-'):+.1f}%" if yoy.get("yoy_net_sales") is not None else "-",
                        f"{yoy.get('yoy_operating_profit', '-'):+.1f}%" if yoy.get("yoy_operating_profit") is not None else "-",
                        f"{yoy.get('yoy_ordinary_profit', '-'):+.1f}%" if yoy.get("yoy_ordinary_profit") is not None else "-",
                        f"{yoy.get('yoy_profit', '-'):+.1f}%" if yoy.get("yoy_profit") is not None else "-",
                    ],
                    "前四半期比(QoQ)": [
                        f"{qoq.get('qoq_net_sales', '-'):+.1f}%" if qoq.get("qoq_net_sales") is not None else "-",
                        f"{qoq.get('qoq_operating_profit', '-'):+.1f}%" if qoq.get("qoq_operating_profit") is not None else "-",
                        f"{qoq.get('qoq_ordinary_profit', '-'):+.1f}%" if qoq.get("qoq_ordinary_profit") is not None else "-",
                        f"{qoq.get('qoq_profit', '-'):+.1f}%" if qoq.get("qoq_profit") is not None else "-",
                    ],
                }
                st.dataframe(pd.DataFrame(comparison_data), width="stretch", hide_index=True)

                # シグナル
                signals = analyzer.detect_signals(selected_code)
                if signals:
                    st.subheader("⚡ 検出シグナル")
                    for sig in signals:
                        st.markdown(f"- {sig}")
            else:
                st.info("決算データがありません")

        # ── 右カラム: AI分析結果 ───
        with detail_col2:
            st.subheader("🤖 AI分析結果")
            session = get_session()
            try:
                ai_result = (
                    session.query(AIAnalysisResult)
                    .filter_by(code=selected_code)
                    .order_by(AIAnalysisResult.analyzed_at.desc())
                    .first()
                )
                if ai_result:
                    session.expunge(ai_result)
            finally:
                session.close()

            if ai_result and ai_result.summary:
                # センチメント表示
                sentiment_emoji = {
                    "positive": "🟢 ポジティブ",
                    "negative": "🔴 ネガティブ",
                    "neutral": "🟡 ニュートラル",
                }
                st.markdown(f"**センチメント:** {sentiment_emoji.get(ai_result.sentiment, '❓')}")

                st.markdown("**📝 要約:**")
                st.markdown(ai_result.summary)

                # 注目ポイント
                try:
                    key_points = json.loads(ai_result.key_points) if ai_result.key_points else []
                except json.JSONDecodeError:
                    key_points = []
                if key_points:
                    st.markdown("**🔑 注目ポイント:**")
                    for kp in key_points:
                        st.markdown(f"- {kp}")

                # キーワード
                try:
                    keywords = json.loads(ai_result.keywords) if ai_result.keywords else []
                except json.JSONDecodeError:
                    keywords = []
                if keywords:
                    st.markdown("**🏷️ キーワード:**")
                    # タグ風に横並び表示
                    tags_html = " ".join(
                        f'<span style="background:#667eea;color:white;padding:2px 8px;border-radius:10px;margin:2px;display:inline-block;font-size:12px">{kw}</span>'
                        for kw in keywords
                    )
                    st.markdown(tags_html, unsafe_allow_html=True)

                # シグナルワード
                try:
                    signal_words = json.loads(ai_result.signal_words) if ai_result.signal_words else []
                except json.JSONDecodeError:
                    signal_words = []
                if signal_words:
                    st.markdown("**⚡ シグナルワード:**")
                    for sw in signal_words:
                        st.markdown(f"- {sw}")

                st.caption(f"分析モデル: {ai_result.model_used} | 分析日時: {ai_result.analyzed_at}")
            else:
                st.info("AI分析結果がありません。サイドバーからAI分析を実行してください。")

            # TDnet開示情報（PDF）
            st.subheader("📄 開示資料")
            session = get_session()
            try:
                tdnet_docs = (
                    session.query(TDnetDisclosure)
                    .filter(
                        TDnetDisclosure.code == selected_code,
                        TDnetDisclosure.disclosed_date == dt,
                    )
                    .all()
                )
                if tdnet_docs:
                    for doc in tdnet_docs:
                        with st.expander(doc.title or "書類"):
                            if doc.document_url:
                                st.markdown(f"[📄 書類を開く]({doc.document_url})")
                            if doc.pdf_local_path:
                                st.caption(f"ローカル: {doc.pdf_local_path}")
                else:
                    st.info("TDnet開示データがありません")
            finally:
                session.close()
else:
    st.info("対象日の決算データがありません。サイドバーからデータを同期してください。")


# ═══════════════════════════════════════════
# セクション3: AI分析サマリー一覧
# ═══════════════════════════════════════════
st.markdown('<div class="section-header">🤖 AI分析サマリー一覧</div>', unsafe_allow_html=True)

session = get_session()
try:
    ai_results_all = (
        session.query(AIAnalysisResult, Stock.name)
        .outerjoin(Stock, AIAnalysisResult.code == Stock.code)
        .filter(AIAnalysisResult.disclosed_date == dt)
        .all()
    )
finally:
    session.close()

if ai_results_all:
    ai_rows = []
    for ai, name in ai_results_all:
        try:
            keywords = json.loads(ai.keywords) if ai.keywords else []
        except json.JSONDecodeError:
            keywords = []

        ai_rows.append({
            "コード": ai.code,
            "銘柄名": name or "",
            "センチメント": ai.sentiment or "",
            "要約": (ai.summary or "")[:100] + "..." if ai.summary and len(ai.summary) > 100 else (ai.summary or ""),
            "キーワード": ", ".join(keywords[:5]) if keywords else "",
        })

    ai_df = pd.DataFrame(ai_rows)
    st.dataframe(ai_df, width="stretch", hide_index=True)
else:
    st.info("AI分析結果がありません")


# ═══════════════════════════════════════════
# フッター
# ═══════════════════════════════════════════
st.divider()
st.caption(
    "KessanView — 決算分析補助ツール | "
    "データ: J-Quants API / TDnet WEB-API | "
    "AI: Google Gemini"
)
