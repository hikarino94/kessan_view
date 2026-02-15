"""KessanView データベース接続・初期化モジュール"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import DATABASE_URL
from models.schemas import Base


# SQLite エンジン
engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False},  # Streamlit用
)

# セッションファクトリ
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def init_db():
    """全テーブルを作成（存在しない場合のみ）"""
    Base.metadata.create_all(bind=engine)


def get_session() -> Session:
    """新しいDBセッションを取得"""
    return SessionLocal()


# モジュール読み込み時にテーブル自動作成
init_db()
