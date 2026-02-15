"""KessanView 重要度スコアリングモジュール

財務分析結果に基づき、各銘柄に0-100の重要度スコアを付与する。
スコアリング結果はDBに保存される。
"""
import logging
import math
from datetime import datetime
from typing import Optional

from db.database import get_session
from models.schemas import EarningsScore, FinancialStatement, Stock
from services.financial_analysis import FinancialAnalyzer
import config

logger = logging.getLogger(__name__)


def _sigmoid_score(value: Optional[float], scale: float = 0.05) -> float:
    """シグモイド関数で変化率を0-1スコアに変換

    大きな正の変化率 → 1に近づく
    0付近 → 0.5
    大きな負の変化率 → 0に近づく

    Args:
        value: 変化率 (%)
        scale: 感度調整パラメータ
    Returns:
        0.0 - 1.0 のスコア
    """
    if value is None:
        return 0.5  # データなしは中立
    return 1.0 / (1.0 + math.exp(-scale * value))


class ScoringService:
    """重要度スコアリングサービス"""

    def __init__(self, weights: Optional[dict] = None):
        self.weights = weights or config.DEFAULT_SCORING_WEIGHTS
        self.analyzer = FinancialAnalyzer()

    def score_single(
        self,
        code: str,
        target_statement: Optional[FinancialStatement] = None,
    ) -> dict:
        """単一銘柄のスコアリング

        Returns:
            {
                'code': str,
                'total_score': float (0-100),
                'category': str ('注目'/'要確認'/'通常'),
                'yoy_sales_change': float or None,
                'yoy_op_change': float or None,
                'yoy_profit_change': float or None,
                'qoq_acceleration': float or None,
                'revision_flag': int,
                'turnaround_flag': int,
                'signals': list[str],
            }
        """
        yoy = self.analyzer.compare_year_over_year(code, target_statement)
        qoq = self.analyzer.compare_quarter_over_quarter(code, target_statement)
        signals = self.analyzer.detect_signals(code, target_statement)

        # 各スコア項目 (0-1)
        s_yoy_sales = _sigmoid_score(yoy.get("yoy_net_sales"))
        s_yoy_op = _sigmoid_score(yoy.get("yoy_operating_profit"))
        s_yoy_profit = _sigmoid_score(yoy.get("yoy_profit"))

        # QoQ加速度: QoQとYoYの差分 = QoQが上回っていれば加速
        qoq_op = qoq.get("qoq_operating_profit")
        yoy_op = yoy.get("yoy_operating_profit")
        if qoq_op is not None and yoy_op is not None:
            accel = qoq_op - yoy_op  # 正なら加速
            s_accel = _sigmoid_score(accel, scale=0.03)
        else:
            s_accel = 0.5
            accel = None

        # 業績修正フラグ
        revision_flag = 0
        for sig in signals:
            if "大幅増益" in sig:
                revision_flag = 1
                break
            elif "大幅減益" in sig:
                revision_flag = -1
                break
        s_revision = {1: 1.0, -1: 0.0, 0: 0.5}[revision_flag]

        # 赤黒転換フラグ
        turnaround_flag = 0
        for sig in signals:
            if "黒字転換" in sig:
                turnaround_flag = 1
                break
            elif "赤字転落" in sig:
                turnaround_flag = -1
                break
        s_turnaround = {1: 1.0, -1: 0.0, 0: 0.5}[turnaround_flag]

        # 加重平均スコア (0-1)
        w = self.weights
        weighted_score = (
            w["yoy_sales"] * s_yoy_sales
            + w["yoy_operating_income"] * s_yoy_op
            + w["yoy_profit"] * s_yoy_profit
            + w["qoq_acceleration"] * s_accel
            + w["revision_flag"] * s_revision
            + w["turnaround_flag"] * s_turnaround
        )

        # 0-100 に変換
        total_score = round(weighted_score * 100, 1)
        total_score = max(0, min(100, total_score))

        # カテゴリ分類
        if total_score >= 80:
            category = "注目"
        elif total_score >= 50:
            category = "要確認"
        else:
            category = "通常"

        return {
            "code": code,
            "total_score": total_score,
            "category": category,
            "yoy_sales_change": yoy.get("yoy_net_sales"),
            "yoy_op_change": yoy.get("yoy_operating_profit"),
            "yoy_profit_change": yoy.get("yoy_profit"),
            "qoq_acceleration": accel,
            "revision_flag": revision_flag,
            "turnaround_flag": turnaround_flag,
            "signals": signals,
        }

    def score_and_save(
        self,
        code: str,
        disclosed_date: str,
        disclosure_number: str = "",
        target_statement: Optional[FinancialStatement] = None,
    ) -> dict:
        """スコアリングしてDBに保存

        Returns:
            スコアリング結果dict
        """
        result = self.score_single(code, target_statement)

        session = get_session()
        try:
            dt = datetime.strptime(disclosed_date.replace("-", ""), "%Y%m%d").date()

            existing = (
                session.query(EarningsScore)
                .filter_by(code=code, disclosure_number=disclosure_number)
                .first()
            )

            values = dict(
                code=code,
                disclosed_date=dt,
                disclosure_number=disclosure_number,
                yoy_sales_change=result["yoy_sales_change"],
                yoy_op_change=result["yoy_op_change"],
                yoy_profit_change=result["yoy_profit_change"],
                qoq_acceleration=result["qoq_acceleration"],
                revision_flag=result["revision_flag"],
                turnaround_flag=result["turnaround_flag"],
                total_score=result["total_score"],
                category=result["category"],
            )

            if existing:
                for key, val in values.items():
                    setattr(existing, key, val)
            else:
                score = EarningsScore(**values)
                session.add(score)

            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"スコア保存エラー ({code}): {e}")
            raise
        finally:
            session.close()

        return result

    def score_all_for_date(
        self,
        target_date: str,
        progress_callback=None,
    ) -> list[dict]:
        """指定日の全決算をスコアリングしてDB保存

        Args:
            target_date: 開示日 (YYYY-MM-DD)
            progress_callback: fn(current, total)
        Returns:
            スコアリング結果のリスト（スコア降順）
        """
        logger.info(f"=== スコアリング開始: {target_date} ===")

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
        total = len(statements)
        for i, stmt in enumerate(statements, 1):
            try:
                result = self.score_and_save(
                    code=stmt.code,
                    disclosed_date=target_date,
                    disclosure_number=stmt.disclosure_number or "",
                    target_statement=stmt,
                )

                # 銘柄名を追加
                session = get_session()
                try:
                    stock = session.query(Stock).filter_by(code=stmt.code).first()
                    result["name"] = stock.name if stock else ""
                finally:
                    session.close()

                result["disclosed_date"] = target_date
                result["period"] = stmt.type_of_current_period or ""
                result["net_sales"] = stmt.net_sales
                result["operating_profit"] = stmt.operating_profit
                result["profit"] = stmt.profit
                results.append(result)
            except Exception as e:
                logger.warning(f"スコアリングスキップ ({stmt.code}): {e}")

            if progress_callback:
                progress_callback(i, total)

        # スコア降順ソート
        results.sort(key=lambda x: x.get("total_score", 0), reverse=True)

        logger.info(f"スコアリング完了: {len(results)}/{total}件")
        return results
