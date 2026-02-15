
import sys, os, pathlib, json, time
import logging

# パス設定
sys.path.insert(0, '/home/tkimura/kessan_view')
os.chdir('/home/tkimura/kessan_view')

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# DB初期化
import config

# DB初期化
db_path = config.DB_PATH
if db_path.exists():
    db_path.unlink()
    logger.info(f"既存DB削除完了: {db_path}")

from db.database import init_db, get_session
init_db()
logger.info("DB初期化完了")

from services.jquants import JQuantsClient
from services.sync import _parse_date, _safe_float
from models.schemas import Stock, FinancialStatement

client = JQuantsClient()

# 対象銘柄
targets = [
    # code, name, cache_file (optional)
    ('67580', 'ソニーグループ', '/tmp/sony.json'),
    ('79740', '任天堂', '/tmp/nintendo.json'),
    ('54010', '日本製鉄', None),
    ('36970', 'SHIFT', None),
]

session = get_session()

try:
    for code, name, cache_path in targets:
        logger.info(f"=== 処理開始: {name} ({code}) ===")
        
        # 1. 銘柄マスタ取得・登録
        logger.info("  銘柄マスタ取得中...")
        try:
            # 存在チェック
            existing_stock = session.query(Stock).filter_by(code=code).first()
            if existing_stock:
                logger.info(f"  銘柄マスタ既存: {name}")
            else:
                time.sleep(12) 
                res = client._request("/equities/master", params={"code": code})
                stock_data_list = res.get("data", [])
                
                if stock_data_list:
                    item = stock_data_list[0]
                    stock = Stock(
                        code=item.get("Code"),
                        name=item.get("CoName"),
                        sector_17_code=item.get("S17C", ""),
                        sector_17_name=item.get("S17Nm", ""),
                        sector_33_code=item.get("S33C", ""),
                        sector_33_name=item.get("S33Nm", ""),
                        market_code=item.get("MktC", ""),
                        market_name=item.get("MktNm", ""),
                    )
                    session.add(stock)
                    session.commit()
                    logger.info(f"  銘柄マスタ保存完了: {item.get('CoName')}")
                else:
                    logger.warning(f"  銘柄マスタが見つかりません: {code}")
                    stock = Stock(code=code, name=name, sector_33_name="テストセクター", market_name="テスト市場")
                    session.add(stock)
                    session.commit()
                    logger.warning(f"  ダミーマスタ登録: {name}")

        except Exception as e:
            session.rollback()
            logger.error(f"  銘柄マスタ取得エラー: {e}")
            try:
                existing = session.query(Stock).filter_by(code=code).first()
                if not existing:
                    stock = Stock(code=code, name=name, sector_33_name="エラー", market_name="エラー")
                    session.add(stock)
                    session.commit()
            except Exception as e2:
                session.rollback()
                logger.error(f"  ダミー登録も失敗: {e2}")

        # 2. 決算情報取得
        items = []
        if cache_path and os.path.exists(cache_path):
            logger.info(f"  キャッシュから読み込み: {cache_path}")
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    items = data.get('data', [])
                logger.info(f"  {len(items)}件読み込み")
            except Exception as e:
                logger.error(f"  キャッシュ読み込みエラー: {e}")
        
        if not items:
            logger.info("  APIから決算情報取得...")
            try:
                time.sleep(12)
                items = client.get_statements_by_code(code)
                logger.info(f"  API取得完了: {len(items)}件")
            except Exception as e:
                logger.error(f"  決算情報APIエラー: {e}")
                continue

        # 3. 決算情報保存
        count = 0
        for item in items:
            c = item.get("Code", "")
            disc_no = item.get("DiscNo", "")
            if not c or not disc_no:
                continue
            
            # 重複チェック
            existing_fs = session.query(FinancialStatement).filter_by(
                code=c, disclosure_number=disc_no
            ).first()
            if existing_fs:
                continue

            fs = FinancialStatement(
                code=c,
                disclosed_date=_parse_date(item.get("DiscDate")),
                disclosed_time=item.get("DiscTime", ""),
                disclosure_number=disc_no,
                type_of_document=item.get("DocType", ""),
                type_of_current_period=item.get("CurPerType", ""),
                current_period_start_date=_parse_date(item.get("CurPerSt")),
                current_period_end_date=_parse_date(item.get("CurPerEn")),
                current_fiscal_year_start_date=_parse_date(item.get("CurFYSt")),
                current_fiscal_year_end_date=_parse_date(item.get("CurFYEn")),
                net_sales=_safe_float(item.get("Sales")),
                operating_profit=_safe_float(item.get("OP")),
                ordinary_profit=_safe_float(item.get("OdP")),
                profit=_safe_float(item.get("NP")),
                earnings_per_share=_safe_float(item.get("EPS")),
                total_assets=_safe_float(item.get("Tas")), 
                equity=_safe_float(item.get("Eq")),
                forecast_net_sales=_safe_float(item.get("FSales")),
                forecast_operating_profit=_safe_float(item.get("FOP")),
                forecast_profit=_safe_float(item.get("FNP")),
                raw_json=json.dumps(item, ensure_ascii=False, default=str),
            )
            session.add(fs)
            count += 1
        
        session.commit()
        logger.info(f"  DB保存完了: {count}件")
        
finally:
    session.close()

logger.info("=== 全処理完了 ===")
