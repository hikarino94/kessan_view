"""J-Quants API V2 クライアント

プラン別レート制限対応、ページネーション対応、
HTTP 429 リトライ機能を備えたAPIクライアント。
"""
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import requests

import config

logger = logging.getLogger(__name__)


class JQuantsAPIError(Exception):
    """J-Quants API エラー"""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


class JQuantsClient:
    """J-Quants API V2 クライアント

    - プラン別レート制限の自動ウェイト処理
    - HTTP 429 時の自動リトライ
    - ページネーション対応
    - レスポンスキャッシュ（オプション）
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        plan: Optional[str] = None,
        use_cache: bool = True,
    ):
        self.api_key = api_key or config.JQUANTS_API_KEY
        self.plan = plan or config.JQUANTS_PLAN
        self.base_url = config.JQUANTS_BASE_URL
        self.use_cache = use_cache

        # レート制限
        rate = config.JQUANTS_RATE_LIMITS.get(self.plan, 5)
        self.request_interval = 60.0 / rate
        self.retry_wait = config.JQUANTS_RETRY_WAIT
        self.max_retries = config.JQUANTS_MAX_RETRIES

        # 最後のリクエスト時刻
        self._last_request_time = 0.0

        if not self.api_key:
            logger.warning("J-Quants APIキーが設定されていません")

    def _wait_for_rate_limit(self):
        """レート制限に基づくウェイト処理"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.request_interval:
            wait_time = self.request_interval - elapsed
            logger.debug(f"レート制限ウェイト: {wait_time:.2f}秒")
            time.sleep(wait_time)

    def _request(
        self,
        endpoint: str,
        params: Optional[dict] = None,
    ) -> dict:
        """API リクエスト実行（レート制限 + リトライ対応）"""
        url = f"{self.base_url}{endpoint}"
        headers = {"x-api-key": self.api_key}

        for attempt in range(self.max_retries + 1):
            self._wait_for_rate_limit()
            self._last_request_time = time.time()

            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
            except requests.RequestException as e:
                logger.error(f"リクエストエラー: {e}")
                if attempt < self.max_retries:
                    logger.info(f"リトライ {attempt + 1}/{self.max_retries}...")
                    time.sleep(self.retry_wait)
                    continue
                raise

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                logger.warning(
                    f"レート制限超過 (429)。{self.retry_wait}秒待機後リトライ "
                    f"({attempt + 1}/{self.max_retries})"
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                    continue
                raise JQuantsAPIError(429, "レート制限超過。リトライ上限に到達。")

            raise JQuantsAPIError(
                response.status_code,
                response.text[:500],
            )

        raise JQuantsAPIError(0, "リクエスト失敗（リトライ上限）")

    def _request_all_pages(
        self,
        endpoint: str,
        data_key: str,
        params: Optional[dict] = None,
        progress_callback=None,
    ) -> list:
        """ページネーション対応で全データ取得

        Args:
            endpoint: APIエンドポイント
            data_key: レスポンスJSONのデータ配列キー
            params: クエリパラメータ
            progress_callback: 進捗コールバック fn(fetched_count)
        """
        all_data = []
        params = dict(params) if params else {}
        page = 0

        while True:
            page += 1
            result = self._request(endpoint, params)
            data = result.get(data_key, [])
            all_data.extend(data)

            if progress_callback:
                progress_callback(len(all_data))

            logger.info(
                f"  ページ {page}: {len(data)}件取得 (累計 {len(all_data)}件)"
            )

            pagination_key = result.get("pagination_key")
            if not pagination_key:
                break
            params["pagination_key"] = pagination_key

        return all_data

    # ──────────────────────────────────────────
    # 銘柄マスタ
    # ──────────────────────────────────────────

    def get_listed_info(self) -> list[dict]:
        """全上場銘柄一覧を取得

        Returns:
            銘柄情報のリスト
        """
        logger.info("銘柄マスタ取得中...")
        return self._request_all_pages("/equities/master", "data")

    # ──────────────────────────────────────────
    # 財務情報
    # ──────────────────────────────────────────

    def get_statements_by_date(
        self,
        target_date: str,
        progress_callback=None,
    ) -> list[dict]:
        """指定日に開示された全決算情報を取得

        Args:
            target_date: 開示日 (YYYY-MM-DD or YYYYMMDD)
            progress_callback: 進捗コールバック
        Returns:
            決算情報のリスト
        """
        logger.info(f"決算情報取得中: {target_date}")
        return self._request_all_pages(
            "/fins/summary",
            "data",
            params={"date": target_date},
            progress_callback=progress_callback,
        )

    def get_statements_by_code(
        self,
        code: str,
        progress_callback=None,
    ) -> list[dict]:
        """指定銘柄の全決算情報を取得

        Args:
            code: 銘柄コード (4桁 or 5桁)
            progress_callback: 進捗コールバック
        Returns:
            決算情報のリスト
        """
        logger.info(f"決算情報取得中: {code}")
        return self._request_all_pages(
            "/fins/summary",
            "data",
            params={"code": code},
            progress_callback=progress_callback,
        )

    # ──────────────────────────────────────────
    # 株価
    # ──────────────────────────────────────────

    def get_daily_quotes_by_date(
        self,
        target_date: str,
        progress_callback=None,
    ) -> list[dict]:
        """指定日の全銘柄株価を取得

        Args:
            target_date: 取引日 (YYYY-MM-DD or YYYYMMDD)
            progress_callback: 進捗コールバック
        Returns:
            株価データのリスト
        """
        logger.info(f"株価取得中: {target_date}")
        return self._request_all_pages(
            "/equities/bars/daily",
            "data",
            params={"date": target_date},
            progress_callback=progress_callback,
        )

    def get_daily_quotes_by_code(
        self,
        code: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        progress_callback=None,
    ) -> list[dict]:
        """指定銘柄の株価を取得

        Args:
            code: 銘柄コード
            date_from: 開始日 (YYYY-MM-DD)
            date_to: 終了日 (YYYY-MM-DD)
            progress_callback: 進捗コールバック
        Returns:
            株価データのリスト
        """
        logger.info(f"株価取得中: {code} ({date_from} ~ {date_to})")
        params = {"code": code}
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to

        return self._request_all_pages(
            "/equities/bars/daily",
            "data",
            params=params,
            progress_callback=progress_callback,
        )

    # ──────────────────────────────────────────
    # キャッシュ
    # ──────────────────────────────────────────

    def save_cache(self, key: str, data: Any):
        """APIレスポンスをキャッシュに保存"""
        if not self.use_cache:
            return
        cache_file = config.CACHE_DIR / f"{key}.json"
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=str)
        logger.debug(f"キャッシュ保存: {cache_file}")

    def load_cache(self, key: str) -> Optional[Any]:
        """キャッシュからデータ読み込み"""
        if not self.use_cache:
            return None
        cache_file = config.CACHE_DIR / f"{key}.json"
        if cache_file.exists():
            with open(cache_file, "r", encoding="utf-8") as f:
                logger.debug(f"キャッシュ読込: {cache_file}")
                return json.load(f)
        return None
