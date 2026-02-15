import sys, os, logging
from pathlib import Path
from datetime import date

# パス設定
sys.path.insert(0, str(Path(__file__).resolve().parent))
os.chdir(str(Path(__file__).resolve().parent))

import config
from db.database import init_db, get_session
from models.schemas import TDnetDisclosure, AIAnalysisResult
from services.tdnet import TDnetClient
from services.ai_analyzer import AIAnalyzer

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def test_integration():
    init_db()
    logger.info("=== 統合テスト開始 ===")
    
    # 1. TDnet連携テスト
    # 直近の平日（または指定日）を使用
    target_date = "2026-02-13"  # 直近の平日(金曜)
    logger.info(f"TDnet取得テスト: {target_date}")
    
    tdnet = TDnetClient()
    disclosures = tdnet.get_disclosures_by_date(target_date)
    logger.info(f"取得件数: {len(disclosures)}件")
    
    if not disclosures:
        logger.error("TDnetデータが取得できませんでした。")
        return

    # PDFダウンロードとDB保存
    logger.info("DB保存テスト...")
    count = tdnet.save_disclosures_to_db(disclosures, target_date)
    logger.info(f"DB保存完了: {count}件")
    
    # 決算短信PDFを含む開示情報を取得
    session = get_session()
    target_disclosure = None
    try:
        # 決算短信かつURLがあるものを探す
        candidates = (
            session.query(TDnetDisclosure)
            .filter(
                TDnetDisclosure.disclosed_date == date(2025, 11, 14),
                TDnetDisclosure.is_earnings_report == 1,
                TDnetDisclosure.document_url != "",
            )
            .limit(1)
            .all()
        )
        if candidates:
            target_disclosure = candidates[0]
    finally:
        session.close()
        
    if not target_disclosure:
        logger.error("テスト対象の決算短信が見つかりませんでした")
        return

    logger.info(f"テスト対象: {target_disclosure.code} {target_disclosure.company_name} ({target_disclosure.title})")
    
    # PDFダウンロード（APIのリンク切れのためスキップし、ダミーデータでテスト）
    # pdf_path = tdnet.download_pdf(target_url)
    
    # ダミーPDFテキストを作成してAI分析テスト
    logger.info("ダミーテキストでAI分析テストを実行します...")
    dummy_text = f"""
    {target_disclosure.company_name} 2026年3月期 第2四半期決算短信
    
    1. 経営成績
    当第2四半期連結累計期間の売上高は500億円（前年同期比10.5%増）、営業利益は50億円（同15.2%増）となりました。
    主力事業である半導体部門が好調に推移し、円安の影響も寄与しました。
    
    2. 財政状態
    総資産は前連結会計年度末に比べ20億円増加し、1,200億円となりました。
    自己資本比率は65.0%と引き続き健全な水準を維持しています。
    
    3. 通期業績予想
    通期の業績予想につきましては、前回発表予想から変更ありません。
    売上高1,100億円、営業利益120億円を見込んでおります。
    """
    
    # 2. AI分析テスト
    logger.info("AI分析テスト...")
    analyzer = AIAnalyzer()
    
    # Gemini API呼び出し
    logger.info("Gemini API呼び出し...")
    result = analyzer.analyze_earnings(dummy_text, code=target_disclosure.code, company_name=target_disclosure.company_name)
    
    if not result:
        logger.error("AI分析失敗")
        return
        
    logger.info("AI分析成功")
    logger.info(f"要約: {result.get('summary')[:50]}...")
    logger.info(f"センチメント: {result.get('sentiment')}")
    
    # 結果保存（直接DB保存）
    logger.info("AI結果保存テスト...")
    session = get_session()
    try:
        from datetime import datetime
        import json
        dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").date()
        
        # 既存チェック・削除
        session.query(AIAnalysisResult).filter_by(
            code=target_disclosure.code, 
            disclosed_date=dt
        ).delete()
        session.commit()
        
        values = dict(
            code=target_disclosure.code,
            disclosed_date=dt,
            disclosure_number="TEST_DUMMY_NO_" + datetime.now().strftime("%H%M%S"),
            summary=result.get("summary", ""),
            key_points=json.dumps(result.get("key_points", []), ensure_ascii=False),
            keywords=json.dumps(result.get("keywords", []), ensure_ascii=False),
            sentiment=result.get("sentiment", "neutral"),
            signal_words=json.dumps(result.get("signal_words", []), ensure_ascii=False),
            model_used="gemini-2.0-flash-test",
            analyzed_at=datetime.utcnow(),
        )
        
        ai_result = AIAnalysisResult(**values)
        session.add(ai_result)
        
        session.commit()
    finally:
        session.close()
    
    # DB確認
    session = get_session()
    try:
        # DB保存の日付型に合わせて検索
        target_dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").date()
        ai_res = (
            session.query(AIAnalysisResult)
            .filter_by(code=target_disclosure.code, disclosed_date=target_dt)
            .first()
        )
        if ai_res:
            logger.info(f"DB保存確認OK: {ai_res.summary[:20]}...")
            logger.info(f"キーワード: {ai_res.keywords}")
        else:
            logger.error("DB保存確認失敗")
    finally:
        session.close()

    logger.info("=== 統合テスト完了 ===")

if __name__ == "__main__":
    test_integration()
