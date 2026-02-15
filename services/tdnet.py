"""KessanView TDnet WEB-API クライアント

やのしん氏提供のTDnet WEB-APIを使用して
適時開示情報の取得・決算短信PDFのダウンロードを行う。
"""
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

import config
from db.database import get_session
from models.schemas import TDnetDisclosure

logger = logging.getLogger(__name__)

# 決算短信を判別するキーワード
EARNINGS_KEYWORDS = [
    "決算短信",
    "業績予想の修正",
    "配当予想の修正",
    "四半期報告書",
]


class TDnetClient:
    """やのしんTDnet WEB-API クライアント

    API: https://webapi.yanoshin.jp/webapi/tdnet/list/{条件}.{形式}
    """

    def __init__(self):
        self.base_url = config.TDNET_API_BASE_URL

    def get_disclosures_by_date(
        self,
        target_date: str,
        limit: int = 1000,
    ) -> list[dict]:
        """指定日の開示情報一覧を取得

        Args:
            target_date: 対象日 (YYYY-MM-DD or YYYYMMDD)
            limit: 最大取得件数
        Returns:
            開示情報のリスト
        """
        # 日付をYYYYMMDD形式に
        date_str = target_date.replace("-", "")
        url = f"{self.base_url}/{date_str}.json"
        params = {"limit": limit}

        logger.info(f"TDnet開示情報取得: {target_date}")

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error(f"TDnet APIエラー: {e}")
            return []

        # レスポンス構造に応じてパース
        raw_items = data if isinstance(data, list) else data.get("items", [])
        # 各アイテムは {"Tdnet": {...}} でラップされているのでフラット化
        items = [item.get("Tdnet", item) if isinstance(item, dict) else item for item in raw_items]
        logger.info(f"TDnet開示情報: {len(items)}件取得")
        return items

    def filter_earnings_reports(
        self,
        disclosures: list[dict],
    ) -> list[dict]:
        """開示一覧から決算短信・業績修正のみをフィルタリング"""
        earnings = []
        for item in disclosures:
            title = item.get("title", "")
            if any(kw in title for kw in EARNINGS_KEYWORDS):
                earnings.append(item)
        logger.info(f"決算短信フィルタ結果: {len(earnings)}/{len(disclosures)}件")
        return earnings

    def _extract_code_from_item(self, item: dict) -> str:
        """開示情報から銘柄コードを抽出"""
        # 主要なフィールドを確認
        code = item.get("company_code", "") or item.get("code", "") or item.get("Code", "")
        if code:
            return str(code).strip()

        # タイトルやURLから抽出を試行
        title = item.get("title", "")
        url = item.get("document_url", "") or item.get("url", "") or item.get("link", "")

        # URLパターン: /xxxx/ のような4桁数字
        match = re.search(r"/(\d{4})/", url)
        if match:
            return match.group(1)

        return ""

    def _extract_pdf_url(self, item: dict) -> str:
        """開示情報からPDFのURLを抽出"""
        # 直接のPDFフィールド
        pdf_url = item.get("pdf_url", "") or item.get("pdfUrl", "")
        if pdf_url:
            return pdf_url

        # document_url がリダイレクタ経由のPDFリンクの場合
        doc_url = item.get("document_url", "")
        if doc_url:
            return doc_url

        # 書類URLからPDFリンクを推測
        fallback_url = item.get("url", "") or item.get("link", "")
        if fallback_url:
            return fallback_url

        return ""

    def save_disclosures_to_db(
        self,
        disclosures: list[dict],
        target_date: str,
    ) -> int:
        """開示情報をDBに保存

        Returns:
            保存件数
        """
        session = get_session()
        count = 0
        try:
            dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").date()

            for item in disclosures:
                title = item.get("title", "")
                doc_url = item.get("document_url", "") or item.get("url", "") or item.get("link", "")
                if not doc_url:
                    continue

                # 重複チェック
                existing = (
                    session.query(TDnetDisclosure)
                    .filter_by(document_url=doc_url)
                    .first()
                )
                if existing:
                    continue

                code = self._extract_code_from_item(item)
                pdf_url = self._extract_pdf_url(item)
                is_earnings = 1 if any(kw in title for kw in EARNINGS_KEYWORDS) else 0

                disclosure = TDnetDisclosure(
                    code=code,
                    company_name=item.get("company_name", ""),
                    disclosed_date=dt,
                    disclosed_time=item.get("pubdate", "") or item.get("time", ""),
                    title=title,
                    document_url=doc_url,
                    pdf_url=pdf_url,
                    xbrl_url=item.get("url_xbrl", "") or item.get("xbrl_url", "") or item.get("xbrlUrl", ""),
                    is_earnings_report=is_earnings,
                )
                session.add(disclosure)
                count += 1

            session.commit()
            logger.info(f"開示情報DB保存: {count}件")
        except Exception as e:
            session.rollback()
            logger.error(f"開示情報保存エラー: {e}")
            raise
        finally:
            session.close()

        return count

    def download_pdf(
        self,
        pdf_url: str,
        save_dir: Optional[Path] = None,
    ) -> Optional[str]:
        """PDFをダウンロード

        Args:
            pdf_url: PDF URL
            save_dir: 保存先ディレクトリ
        Returns:
            保存先パス (失敗時はNone)
        """
        if not pdf_url:
            return None

        save_dir = save_dir or config.PDF_DIR
        save_dir.mkdir(parents=True, exist_ok=True)

        # ファイル名: URLの末尾 or ハッシュ
        filename = pdf_url.split("/")[-1]
        if not filename.endswith(".pdf"):
            filename = filename + ".pdf"
        save_path = save_dir / filename

        # 既にダウンロード済み
        if save_path.exists():
            logger.debug(f"PDF既存: {save_path}")
            return str(save_path)

        try:
            logger.info(f"PDFダウンロード: {pdf_url}")
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()

            with open(save_path, "wb") as f:
                f.write(response.content)

            logger.info(f"PDF保存: {save_path}")
            return str(save_path)
        except requests.RequestException as e:
            logger.error(f"PDFダウンロードエラー: {e}")
            return None

    def download_all_earnings_pdfs(
        self,
        target_date: str,
        progress_callback=None,
    ) -> list[dict]:
        """指定日の決算短信PDFを一括ダウンロード

        Args:
            target_date: 対象日 (YYYY-MM-DD)
            progress_callback: fn(current, total)
        Returns:
            ダウンロード結果のリスト
        """
        # 開示一覧を取得
        disclosures = self.get_disclosures_by_date(target_date)

        # DB保存
        self.save_disclosures_to_db(disclosures, target_date)

        # 決算短信フィルタ
        earnings = self.filter_earnings_reports(disclosures)

        results = []
        total = len(earnings)
        for i, item in enumerate(earnings, 1):
            pdf_url = self._extract_pdf_url(item)
            local_path = self.download_pdf(pdf_url)

            # DBのpdf_local_pathを更新
            if local_path:
                doc_url = item.get("document_url", "") or item.get("url", "") or item.get("link", "")
                session = get_session()
                try:
                    disc = (
                        session.query(TDnetDisclosure)
                        .filter_by(document_url=doc_url)
                        .first()
                    )
                    if disc:
                        disc.pdf_local_path = local_path
                        session.commit()
                finally:
                    session.close()

            results.append({
                "title": item.get("title", ""),
                "code": self._extract_code_from_item(item),
                "pdf_url": pdf_url,
                "local_path": local_path,
                "success": local_path is not None,
            })

            if progress_callback:
                progress_callback(i, total)

            # TDnetサーバー負荷軽減のため少しウェイト
            time.sleep(0.5)

        success_count = sum(1 for r in results if r["success"])
        logger.info(f"PDFダウンロード完了: {success_count}/{total}件")
        return results
