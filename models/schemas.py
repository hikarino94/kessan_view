"""KessanView データモデル定義

J-Quants API取得データ、TDnet開示情報、AI分析結果を
SQLiteで一元管理するためのSQLAlchemyモデル。
"""
from datetime import date, datetime

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """SQLAlchemy 宣言的ベースクラス"""
    pass


class Stock(Base):
    """銘柄マスタ (J-Quants listed/info)"""
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    sector_17_code = Column(String(10), default="")
    sector_17_name = Column(String(100), default="")
    sector_33_code = Column(String(10), default="")
    sector_33_name = Column(String(100), default="")
    market_code = Column(String(10), default="")
    market_name = Column(String(100), default="")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Stock {self.code} {self.name}>"


class FinancialStatement(Base):
    """決算情報 (J-Quants fins/statements)

    四半期ごとの決算短信サマリーデータを保存。
    前Q・前Y比較の元データとなる。
    """
    __tablename__ = "financial_statements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 識別情報
    code = Column(String(10), nullable=False, index=True)
    disclosed_date = Column(Date, nullable=False, index=True)
    disclosed_time = Column(String(10), default="")
    disclosure_number = Column(String(30), default="", index=True)
    # 期間情報
    type_of_document = Column(String(100), default="")
    type_of_current_period = Column(String(10), default="")  # FY, 1Q, 2Q, 3Q
    current_period_start_date = Column(Date, nullable=True)
    current_period_end_date = Column(Date, nullable=True)
    current_fiscal_year_start_date = Column(Date, nullable=True)
    current_fiscal_year_end_date = Column(Date, nullable=True)
    # 損益計算書 (PL)
    net_sales = Column(Float, nullable=True)                   # 売上高
    operating_profit = Column(Float, nullable=True)            # 営業利益
    ordinary_profit = Column(Float, nullable=True)             # 経常利益
    profit = Column(Float, nullable=True)                      # 当期純利益
    earnings_per_share = Column(Float, nullable=True)          # EPS
    # 貸借対照表 (BS)
    total_assets = Column(Float, nullable=True)                # 総資産
    equity = Column(Float, nullable=True)                      # 純資産
    equity_to_asset_ratio = Column(Float, nullable=True)       # 自己資本比率
    book_value_per_share = Column(Float, nullable=True)        # BPS
    # 業績予想
    forecast_net_sales = Column(Float, nullable=True)          # 通期予想売上高
    forecast_operating_profit = Column(Float, nullable=True)   # 通期予想営業利益
    forecast_ordinary_profit = Column(Float, nullable=True)    # 通期予想経常利益
    forecast_profit = Column(Float, nullable=True)             # 通期予想純利益
    forecast_earnings_per_share = Column(Float, nullable=True) # 通期予想EPS
    # 配当
    result_dividend_per_share_annual = Column(Float, nullable=True)  # 年間配当（実績）
    # メタ
    raw_json = Column(Text, default="")  # 元のJSONレスポンスを保存
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("code", "disclosure_number", name="uq_fs_code_disclosure"),
    )

    def __repr__(self):
        return f"<FinancialStatement {self.code} {self.disclosed_date} {self.type_of_current_period}>"


class DailyPrice(Base):
    """日足株価 (J-Quants prices/daily_quotes)"""
    __tablename__ = "daily_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    turnover_value = Column(Float, nullable=True)  # 売買代金
    adjustment_factor = Column(Float, default=1.0) # 調整係数
    adjustment_close = Column(Float, nullable=True) # 調整後終値
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("code", "trade_date", name="uq_dp_code_date"),
    )

    def __repr__(self):
        return f"<DailyPrice {self.code} {self.trade_date}>"


class TDnetDisclosure(Base):
    """TDnet適時開示情報 (やのしんWEB-API)"""
    __tablename__ = "tdnet_disclosures"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=True, index=True)
    company_name = Column(String(200), default="")
    disclosed_date = Column(Date, nullable=False, index=True)
    disclosed_time = Column(String(10), default="")
    title = Column(String(500), default="")
    document_url = Column(String(1000), default="")
    pdf_url = Column(String(1000), default="")
    pdf_local_path = Column(String(500), default="")  # ダウンロード済みローカルパス
    xbrl_url = Column(String(1000), default="")
    is_earnings_report = Column(Integer, default=0)    # 決算短信フラグ
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("document_url", name="uq_tdnet_doc_url"),
    )

    def __repr__(self):
        return f"<TDnetDisclosure {self.code} {self.title[:30]}>"


class AIAnalysisResult(Base):
    """AI決算分析結果 (Gemini)"""
    __tablename__ = "ai_analysis_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    disclosed_date = Column(Date, nullable=False, index=True)
    disclosure_number = Column(String(30), default="")
    # 分析結果
    summary = Column(Text, default="")           # 要約テキスト
    key_points = Column(Text, default="")        # 注目ポイント (JSON配列)
    keywords = Column(Text, default="")          # 抽出キーワード (JSON配列)
    sentiment = Column(String(20), default="")   # positive / neutral / negative
    signal_words = Column(Text, default="")      # シグナルワード (JSON配列)
    # メタ
    model_used = Column(String(50), default="")
    analyzed_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("code", "disclosure_number", name="uq_ai_code_disclosure"),
    )

    def __repr__(self):
        return f"<AIAnalysisResult {self.code} {self.sentiment}>"


class EarningsScore(Base):
    """重要度スコア（スコアリングエンジン結果）"""
    __tablename__ = "earnings_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    disclosed_date = Column(Date, nullable=False, index=True)
    disclosure_number = Column(String(30), default="")
    # 個別スコア
    yoy_sales_change = Column(Float, nullable=True)      # 売上高YoY変化率
    yoy_op_change = Column(Float, nullable=True)          # 営業利益YoY変化率
    yoy_profit_change = Column(Float, nullable=True)      # 純利益YoY変化率
    qoq_acceleration = Column(Float, nullable=True)       # QoQ加速度
    revision_flag = Column(Integer, default=0)             # 業績修正フラグ (+1/-1/0)
    turnaround_flag = Column(Integer, default=0)           # 赤黒転換フラグ (+1/-1/0)
    # 総合スコア
    total_score = Column(Float, default=0.0)               # 0–100
    category = Column(String(20), default="通常")          # 注目/要確認/通常
    # メタ
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("code", "disclosure_number", name="uq_score_code_disclosure"),
    )

    def __repr__(self):
        return f"<EarningsScore {self.code} score={self.total_score}>"
