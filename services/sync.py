"""KessanView データ同期モジュール

J-Quants APIから取得したデータをDBに保存する。
日付指定での全量取得対応。
"""
import json
import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy.dialects.sqlite import insert as sqlite_upsert

from db.database import get_session
from models.schemas import DailyPrice, FinancialStatement, Stock
from services.jquants import JQuantsClient

logger = logging.getLogger(__name__)


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """日付文字列をdateオブジェクトに変換"""
    if not date_str:
        return None
    try:
        # YYYY-MM-DD or YYYYMMDD
        clean = date_str.replace("-", "")
        return datetime.strptime(clean, "%Y%m%d").date()
    except (ValueError, TypeError):
        return None


def _safe_float(value) -> Optional[float]:
    """安全にfloat変換（空文字・None対応）"""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


class SyncService:
    """データ同期サービス"""

    def __init__(self, client: Optional[JQuantsClient] = None):
        self.client = client or JQuantsClient()

    # ──────────────────────────────────────────
    # 銘柄マスタ同期
    # ──────────────────────────────────────────

    def sync_listed_info(self, progress_callback=None) -> int:
        """全上場銘柄マスタを同期

        Returns:
            同期件数
        """
        logger.info("=== 銘柄マスタ同期開始 ===")
        data = self.client.get_listed_info()

        session = get_session()
        count = 0
        try:
            for item in data:
                code = item.get("Code", "")
                if not code:
                    continue

                existing = session.query(Stock).filter_by(code=code).first()
                if existing:
                    existing.name = item.get("CoName", "")
                    existing.sector_17_code = item.get("S17", "")
                    existing.sector_17_name = item.get("S17Nm", "")
                    existing.sector_33_code = item.get("S33", "")
                    existing.sector_33_name = item.get("S33Nm", "")
                    existing.market_code = item.get("Mkt", "")
                    existing.market_name = item.get("MktNm", "")
                else:
                    stock = Stock(
                        code=code,
                        name=item.get("CoName", ""),
                        sector_17_code=item.get("S17", ""),
                        sector_17_name=item.get("S17Nm", ""),
                        sector_33_code=item.get("S33", ""),
                        sector_33_name=item.get("S33Nm", ""),
                        market_code=item.get("Mkt", ""),
                        market_name=item.get("MktNm", ""),
                    )
                    session.add(stock)

                count += 1
                if progress_callback and count % 500 == 0:
                    progress_callback(count)

            session.commit()
            logger.info(f"銘柄マスタ同期完了: {count}件")
        except Exception as e:
            session.rollback()
            logger.error(f"銘柄マスタ同期エラー: {e}")
            raise
        finally:
            session.close()

        return count

    # ──────────────────────────────────────────
    # 決算情報同期
    # ──────────────────────────────────────────

    def sync_statements_by_date(
        self,
        target_date: str,
        progress_callback=None,
    ) -> int:
        """指定日に開示された全決算情報を取得してDBに保存

        Args:
            target_date: 開示日 (YYYY-MM-DD)
            progress_callback: 進捗コールバック
        Returns:
            同期件数
        """
        logger.info(f"=== 決算情報同期開始: {target_date} ===")
        data = self.client.get_statements_by_date(target_date)

        session = get_session()
        count = 0
        try:
            for item in data:
                code = item.get("Code", "")
                disclosure_number = item.get("DiscNo", "")
                if not code:
                    continue

                existing = (
                    session.query(FinancialStatement)
                    .filter_by(code=code, disclosure_number=disclosure_number)
                    .first()
                )

                values = dict(
                    code=code,
                    disclosed_date=_parse_date(item.get("DiscDate")),
                    disclosed_time=item.get("DiscTime", ""),
                    disclosure_number=disclosure_number,
                    type_of_document=item.get("DocType", ""),
                    type_of_current_period=item.get("CurPerType", ""),
                    current_period_start_date=_parse_date(
                        item.get("CurPerSt")
                    ),
                    current_period_end_date=_parse_date(
                        item.get("CurPerEn")
                    ),
                    current_fiscal_year_start_date=_parse_date(
                        item.get("CurFYSt")
                    ),
                    current_fiscal_year_end_date=_parse_date(
                        item.get("CurFYEn")
                    ),
                    # PL
                    net_sales=_safe_float(item.get("Sales")),
                    operating_profit=_safe_float(item.get("OP")),
                    ordinary_profit=_safe_float(item.get("OdP")),
                    profit=_safe_float(item.get("NP")),
                    earnings_per_share=_safe_float(
                        item.get("EPS")
                    ),
                    # BS
                    total_assets=_safe_float(item.get("TA")),
                    equity=_safe_float(item.get("Eq")),
                    equity_to_asset_ratio=_safe_float(
                        item.get("EqAR")
                    ),
                    book_value_per_share=_safe_float(
                        item.get("BPS")
                    ),
                    # 予想
                    forecast_net_sales=_safe_float(
                        item.get("FSales")
                    ),
                    forecast_operating_profit=_safe_float(
                        item.get("FOP")
                    ),
                    forecast_ordinary_profit=_safe_float(
                        item.get("FOdP")
                    ),
                    forecast_profit=_safe_float(item.get("FNP")),
                    forecast_earnings_per_share=_safe_float(
                        item.get("FEPS")
                    ),
                    # 配当
                    result_dividend_per_share_annual=_safe_float(
                        item.get("DivAnn")
                    ),
                    # 元JSON
                    raw_json=json.dumps(item, ensure_ascii=False, default=str),
                )

                if existing:
                    for key, val in values.items():
                        setattr(existing, key, val)
                else:
                    fs = FinancialStatement(**values)
                    session.add(fs)

                count += 1
                if progress_callback and count % 100 == 0:
                    progress_callback(count)

            session.commit()
            logger.info(f"決算情報同期完了: {count}件")
        except Exception as e:
            session.rollback()
            logger.error(f"決算情報同期エラー: {e}")
            raise
        finally:
            session.close()

        return count

    # ──────────────────────────────────────────
    # 株価同期
    # ──────────────────────────────────────────

    def sync_daily_prices_by_date(
        self,
        target_date: str,
        progress_callback=None,
    ) -> int:
        """指定日の全銘柄株価を取得してDBに保存

        Args:
            target_date: 取引日 (YYYY-MM-DD)
            progress_callback: 進捗コールバック
        Returns:
            同期件数
        """
        logger.info(f"=== 株価同期開始: {target_date} ===")
        data = self.client.get_daily_quotes_by_date(target_date)

        session = get_session()
        count = 0
        try:
            for item in data:
                code = item.get("Code", "")
                trade_date = _parse_date(item.get("Date"))
                if not code or not trade_date:
                    continue

                existing = (
                    session.query(DailyPrice)
                    .filter_by(code=code, trade_date=trade_date)
                    .first()
                )

                values = dict(
                    code=code,
                    trade_date=trade_date,
                    open=_safe_float(item.get("O")),
                    high=_safe_float(item.get("H")),
                    low=_safe_float(item.get("L")),
                    close=_safe_float(item.get("C")),
                    volume=_safe_float(item.get("Vo")),
                    turnover_value=_safe_float(item.get("Va")),
                    adjustment_factor=_safe_float(
                        item.get("AdjFactor")
                    )
                    or 1.0,
                    adjustment_close=_safe_float(
                        item.get("AdjC")
                    ),
                )

                if existing:
                    for key, val in values.items():
                        setattr(existing, key, val)
                else:
                    dp = DailyPrice(**values)
                    session.add(dp)

                count += 1
                if progress_callback and count % 500 == 0:
                    progress_callback(count)

            session.commit()
            logger.info(f"株価同期完了: {count}件")
        except Exception as e:
            session.rollback()
            logger.error(f"株価同期エラー: {e}")
            raise
        finally:
            session.close()

        return count

    def sync_daily_prices_by_code(
        self,
        code: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        progress_callback=None,
    ) -> int:
        """指定銘柄の株価を取得してDBに保存

        Args:
            code: 銘柄コード
            date_from: 開始日
            date_to: 終了日
            progress_callback: 進捗コールバック
        Returns:
            同期件数
        """
        logger.info(f"=== 株価同期開始: {code} ({date_from}~{date_to}) ===")
        data = self.client.get_daily_quotes_by_code(
            code, date_from, date_to
        )

        session = get_session()
        count = 0
        try:
            for item in data:
                trade_date = _parse_date(item.get("Date"))
                if not trade_date:
                    continue

                existing = (
                    session.query(DailyPrice)
                    .filter_by(code=code, trade_date=trade_date)
                    .first()
                )

                values = dict(
                    code=code,
                    trade_date=trade_date,
                    open=_safe_float(item.get("O")),
                    high=_safe_float(item.get("H")),
                    low=_safe_float(item.get("L")),
                    close=_safe_float(item.get("C")),
                    volume=_safe_float(item.get("Vo")),
                    turnover_value=_safe_float(item.get("Va")),
                    adjustment_factor=_safe_float(
                        item.get("AdjFactor")
                    )
                    or 1.0,
                    adjustment_close=_safe_float(
                        item.get("AdjC")
                    ),
                )

                if existing:
                    for key, val in values.items():
                        setattr(existing, key, val)
                else:
                    dp = DailyPrice(**values)
                    session.add(dp)

                count += 1
                if progress_callback and count % 500 == 0:
                    progress_callback(count)

            session.commit()
            logger.info(f"株価同期完了: {count}件")
        except Exception as e:
            session.rollback()
            logger.error(f"株価同期エラー: {e}")
            raise
        finally:
            session.close()

        return count
