"""KessanView 財務分析モジュール

前Q比較・前Y比較を行い、業績の変化を検出する。
"""
import logging
from datetime import date
from typing import Optional

from sqlalchemy import asc

from db.database import get_session
from models.schemas import FinancialStatement, Stock

logger = logging.getLogger(__name__)

# 四半期の順序マッピング
PERIOD_ORDER = {"1Q": 1, "2Q": 2, "3Q": 3, "FY": 4}


def _change_rate(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    """変化率を計算 (%)

    Returns:
        変化率 (%) or None (計算不可)
    """
    if current is None or previous is None:
        return None
    if previous == 0:
        if current == 0:
            return 0.0
        return 100.0 if current > 0 else -100.0
    return ((current - previous) / abs(previous)) * 100


class FinancialAnalyzer:
    """財務分析エンジン

    前Q比較 (QoQ) と前Y比較 (YoY) を行い、
    業績の変化を数値化する。
    """

    # 決算短信（実績）として使用するDocTypeパターン
    # FinancialStatementsが含まれるもの = 正規の決算短信
    # EarnForecastRevision = 業績予想修正 → 除外
    # DivForecastRevision = 配当予想修正 → 除外
    # *Correction* = 訂正（同一期間の最新を使用）
    EARNINGS_DOC_TYPES = "FinancialStatements"

    def _is_earnings_statement(self, doc_type: str) -> bool:
        """決算短信の実績レコードかどうか判定"""
        if not doc_type:
            return False
        # FinancialStatementsが含まれるもの（1Q/2Q/3Q/FY全て）
        return self.EARNINGS_DOC_TYPES in doc_type

    def get_statements_for_code(self, code: str) -> list[FinancialStatement]:
        """指定銘柄の決算短信（実績）を期間順で取得

        業績予想修正(EarnForecastRevision)や配当予想修正(DivForecastRevision)を除外し、
        同一四半期で複数レコード（訂正等）がある場合は最新の開示番号を優先する。
        """
        session = get_session()
        try:
            all_records = (
                session.query(FinancialStatement)
                .filter_by(code=code)
                .order_by(
                    asc(FinancialStatement.current_fiscal_year_end_date),
                    asc(FinancialStatement.current_period_end_date),
                    asc(FinancialStatement.disclosure_number),
                )
                .all()
            )
            session.expunge_all()
        finally:
            session.close()

        # 1. 決算短信のみフィルタ（業績予想修正等を除外）
        earnings_only = [
            s for s in all_records
            if self._is_earnings_statement(s.type_of_document)
        ]

        # 2. 同一期間（決算年度末 + 四半期種別）で重複がある場合、
        #    最新の開示番号（DiscNo）を持つレコードを採用
        deduped = {}
        for s in earnings_only:
            key = (
                str(s.current_fiscal_year_end_date),
                s.type_of_current_period or "",
            )
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = s
            else:
                # 開示番号が大きい（= より新しい）方を採用
                if (s.disclosure_number or "") > (existing.disclosure_number or ""):
                    deduped[key] = s

        result = sorted(
            deduped.values(),
            key=lambda s: (
                s.current_fiscal_year_end_date or date.min,
                PERIOD_ORDER.get(s.type_of_current_period or "", 0),
            ),
        )

        logger.debug(
            f"{code}: 全{len(all_records)}件 → 決算短信{len(earnings_only)}件 → 重複排除後{len(result)}件"
        )
        return result

    def find_previous_year_statement(
        self, statements: list[FinancialStatement], current: FinancialStatement
    ) -> Optional[FinancialStatement]:
        """前年同期の決算を探す"""
        if not current.current_fiscal_year_end_date or not current.type_of_current_period:
            return None

        target_fy_year = current.current_fiscal_year_end_date.year - 1
        target_period = current.type_of_current_period

        for s in statements:
            if (
                s.current_fiscal_year_end_date
                and s.current_fiscal_year_end_date.year == target_fy_year
                and s.type_of_current_period == target_period
                and s.id != current.id
            ):
                return s
        return None

    def find_previous_quarter_statement(
        self, statements: list[FinancialStatement], current: FinancialStatement
    ) -> Optional[FinancialStatement]:
        """前四半期の決算を探す"""
        if not current.type_of_current_period or not current.current_fiscal_year_end_date:
            return None

        current_order = PERIOD_ORDER.get(current.type_of_current_period, 0)
        if current_order == 0:
            return None

        if current_order == 1:
            # 1Qの前四半期は前年度のFY
            target_fy_year = current.current_fiscal_year_end_date.year - 1
            target_period = "FY"
        else:
            target_fy_year = current.current_fiscal_year_end_date.year
            # 前の四半期を逆引き
            rev_map = {v: k for k, v in PERIOD_ORDER.items()}
            target_period = rev_map.get(current_order - 1, "")

        for s in statements:
            if (
                s.current_fiscal_year_end_date
                and s.current_fiscal_year_end_date.year == target_fy_year
                and s.type_of_current_period == target_period
                and s.id != current.id
            ):
                return s
        return None

    def compare_year_over_year(self, code: str, target_statement: Optional[FinancialStatement] = None) -> dict:
        """前年同期比 (YoY) を計算

        Args:
            code: 銘柄コード
            target_statement: 対象の決算（Noneなら最新）
        Returns:
            {
                'code': str,
                'current_period': str,
                'yoy_net_sales': float or None,
                'yoy_operating_profit': float or None,
                'yoy_ordinary_profit': float or None,
                'yoy_profit': float or None,
                'current': FinancialStatement,
                'previous': FinancialStatement or None,
            }
        """
        statements = self.get_statements_for_code(code)
        if not statements:
            return {"code": code, "error": "データなし"}

        current = target_statement or statements[-1]
        previous = self.find_previous_year_statement(statements, current)

        result = {
            "code": code,
            "current_period": current.type_of_current_period or "",
            "current_fy_end": str(current.current_fiscal_year_end_date) if current.current_fiscal_year_end_date else "",
            "yoy_net_sales": None,
            "yoy_operating_profit": None,
            "yoy_ordinary_profit": None,
            "yoy_profit": None,
            "current": current,
            "previous": previous,
        }

        if previous:
            result["yoy_net_sales"] = _change_rate(current.net_sales, previous.net_sales)
            result["yoy_operating_profit"] = _change_rate(current.operating_profit, previous.operating_profit)
            result["yoy_ordinary_profit"] = _change_rate(current.ordinary_profit, previous.ordinary_profit)
            result["yoy_profit"] = _change_rate(current.profit, previous.profit)

        return result

    def compare_quarter_over_quarter(self, code: str, target_statement: Optional[FinancialStatement] = None) -> dict:
        """前四半期比 (QoQ) を計算

        Returns:
            {
                'code': str,
                'qoq_net_sales': float or None,
                'qoq_operating_profit': float or None,
                'qoq_profit': float or None,
                ...
            }
        """
        statements = self.get_statements_for_code(code)
        if not statements:
            return {"code": code, "error": "データなし"}

        current = target_statement or statements[-1]
        previous = self.find_previous_quarter_statement(statements, current)

        result = {
            "code": code,
            "current_period": current.type_of_current_period or "",
            "qoq_net_sales": None,
            "qoq_operating_profit": None,
            "qoq_ordinary_profit": None,
            "qoq_profit": None,
            "current": current,
            "previous": previous,
        }

        if previous:
            result["qoq_net_sales"] = _change_rate(current.net_sales, previous.net_sales)
            result["qoq_operating_profit"] = _change_rate(current.operating_profit, previous.operating_profit)
            result["qoq_ordinary_profit"] = _change_rate(current.ordinary_profit, previous.ordinary_profit)
            result["qoq_profit"] = _change_rate(current.profit, previous.profit)

        return result

    def detect_signals(self, code: str, target_statement: Optional[FinancialStatement] = None) -> list[str]:
        """業績変化シグナルを検出

        Returns:
            シグナル文字列のリスト
            例: ['営業利益 大幅増益 (+45.3%)', '黒字転換']
        """
        signals = []
        yoy = self.compare_year_over_year(code, target_statement)

        if yoy.get("error"):
            return signals

        current = yoy.get("current")
        previous = yoy.get("previous")

        # 大幅増益検出 (YoY +30%以上)
        for label, key in [
            ("売上高", "yoy_net_sales"),
            ("営業利益", "yoy_operating_profit"),
            ("経常利益", "yoy_ordinary_profit"),
            ("純利益", "yoy_profit"),
        ]:
            val = yoy.get(key)
            if val is not None:
                if val >= 30:
                    signals.append(f"📈 {label} 大幅増益 (+{val:.1f}%)")
                elif val <= -30:
                    signals.append(f"📉 {label} 大幅減益 ({val:.1f}%)")

        # 黒字転換 / 赤字転落
        if current and previous:
            if current.profit is not None and previous.profit is not None:
                if previous.profit < 0 and current.profit >= 0:
                    signals.append("✅ 黒字転換")
                elif previous.profit >= 0 and current.profit < 0:
                    signals.append("⚠️ 赤字転落")

            # 過去最高益の可能性（簡易チェック）
            if current.operating_profit is not None:
                statements = self.get_statements_for_code(code)
                same_period = [
                    s for s in statements
                    if s.type_of_current_period == current.type_of_current_period
                    and s.operating_profit is not None
                    and s.id != current.id
                ]
                if same_period:
                    max_op = max(s.operating_profit for s in same_period)
                    if current.operating_profit > max_op:
                        signals.append("🏆 同期間 営業利益 過去最高更新")

        return signals

    def analyze_earnings_for_date(self, target_date: str) -> list[dict]:
        """指定日に開示された全決算を分析

        Args:
            target_date: 開示日 (YYYY-MM-DD)
        Returns:
            各銘柄の分析結果リスト
        """
        from datetime import datetime

        session = get_session()
        try:
            dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").date()
            statements = (
                session.query(FinancialStatement)
                .filter(FinancialStatement.disclosed_date == dt)
                .all()
            )
            session.expunge_all()
        finally:
            session.close()

        results = []
        for stmt in statements:
            yoy = self.compare_year_over_year(stmt.code, stmt)
            qoq = self.compare_quarter_over_quarter(stmt.code, stmt)
            signals = self.detect_signals(stmt.code, stmt)

            # 銘柄名を取得
            session = get_session()
            try:
                stock = session.query(Stock).filter_by(code=stmt.code).first()
                name = stock.name if stock else ""
            finally:
                session.close()

            results.append({
                "code": stmt.code,
                "name": name,
                "disclosed_date": str(stmt.disclosed_date),
                "period": stmt.type_of_current_period or "",
                "net_sales": stmt.net_sales,
                "operating_profit": stmt.operating_profit,
                "profit": stmt.profit,
                "yoy": yoy,
                "qoq": qoq,
                "signals": signals,
            })

        return results
