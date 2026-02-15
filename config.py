"""KessanView 設定管理モジュール"""
import os
from pathlib import Path
from dotenv import load_dotenv

# .env ファイル読み込み
load_dotenv(override=True)


def _env_int(key: str, default: int) -> int:
    """環境変数からint取得（不正値はdefault）"""
    raw = os.getenv(key)
    if raw is None:
        return default
    clean = raw.split("#", 1)[0].strip()
    try:
        return int(clean)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    """環境変数からfloat取得（不正値はdefault）"""
    raw = os.getenv(key)
    if raw is None:
        return default
    clean = raw.split("#", 1)[0].strip()
    try:
        return float(clean)
    except ValueError:
        return default

# ── プロジェクトパス ──────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PDF_DIR = DATA_DIR / "pdfs"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = BASE_DIR / "kessan_view.db"

# ディレクトリ自動作成
for d in [DATA_DIR, PDF_DIR, CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── APIキー ───────────────────────────────────
JQUANTS_API_KEY = os.getenv("JQUANTS_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ── J-Quants API 設定 ─────────────────────────
JQUANTS_BASE_URL = "https://api.jquants.com/v2"

# プラン別レート制限 (リクエスト/分)
JQUANTS_RATE_LIMITS = {
    "free": 5,
    "light": 60,
    "standard": 120,
    "premium": 500,
}

# 使用中のプラン (.env で設定可能)
JQUANTS_PLAN = os.getenv("JQUANTS_PLAN", "free").lower()

# レート制限に基づくリクエスト間隔 (秒)
_rate = JQUANTS_RATE_LIMITS.get(JQUANTS_PLAN, 5)
JQUANTS_REQUEST_INTERVAL = 60.0 / _rate

# HTTP 429 時のリトライ設定
JQUANTS_RETRY_WAIT = 60   # 秒
JQUANTS_MAX_RETRIES = 3

# ── TDnet WEB-API (やのしん) 設定 ─────────────
TDNET_API_BASE_URL = "https://webapi.yanoshin.jp/webapi/tdnet/list"

# ── Gemini API 設定 ───────────────────────────
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_MAX_RETRIES = _env_int("GEMINI_MAX_RETRIES", 2)
GEMINI_RETRY_BASE_WAIT = _env_float("GEMINI_RETRY_BASE_WAIT", 5.0)
GEMINI_REQUEST_INTERVAL = _env_float("GEMINI_REQUEST_INTERVAL", 5.0)

# ── 開発用テスト日付 ──────────────────────────
# 決算集中日をセットして開発・動作確認に使用
DEV_TEST_DATE = os.getenv("DEV_TEST_DATE", "2025-11-14")

# ── スコアリング デフォルト重み ─────────────────
DEFAULT_SCORING_WEIGHTS = {
    "yoy_sales": 0.15,        # 売上高YoY変化率
    "yoy_operating_income": 0.25,  # 営業利益YoY変化率
    "yoy_profit": 0.20,       # 純利益YoY変化率
    "qoq_acceleration": 0.15, # QoQ加速度
    "revision_flag": 0.15,    # 業績修正フラグ
    "turnaround_flag": 0.10,  # 赤黒転換フラグ
}

# ── DB 設定 ────────────────────────────────────
DATABASE_URL = f"sqlite:///{DB_PATH}"
# ── Reload trigger ─────────────────────────────
# Force reload for Streamlit
