"""KessanView AI決算分析モジュール (Gemini)

google-genai SDK を使用して決算短信PDFを直接アップロードし、
要約・キーワード抽出・センチメント判定を行う。結果はDBに保存。
"""
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import config
from db.database import get_session
from models.schemas import AIAnalysisResult

logger = logging.getLogger(__name__)
ERROR_SUMMARY_PREFIX = "分析エラー:"

# 決算分析用プロンプトテンプレート
ANALYSIS_PROMPT = """あなたは日本株の決算分析の専門家です。
以下の決算短信PDFを分析し、投資家向けに要約してください。

## 分析対象
銘柄コード: {code}
企業名: {company_name}

## 出力形式 (必ずJSON形式で出力してください)
```json
{{
    "summary": "決算内容の要約（200文字以内）",
    "key_points": ["注目ポイント1", "注目ポイント2", "注目ポイント3"],
    "keywords": ["キーワード1", "キーワード2", ...],
    "sentiment": "positive/neutral/negative のいずれか",
    "signal_words": ["業績に関連するシグナルワード（例: 上方修正, V字回復, 過去最高, 黒字転換, 構造改革, 新規事業等）"]
}}
```

注意事項:
- 売上高・営業利益・純利益の前年同期比変化率に着目してください
- 業績予想の修正があれば必ず言及してください
- 投資家にとって重要な情報を優先してください
- 必ず有効なJSON形式で出力してください
"""


class AIAnalyzer:
    """AI決算分析サービス (google-genai SDK)"""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or config.GEMINI_API_KEY
        self.model_name = model or config.GEMINI_MODEL
        self.max_retries = max(0, int(getattr(config, "GEMINI_MAX_RETRIES", 2)))
        self.retry_base_wait = max(1.0, float(getattr(config, "GEMINI_RETRY_BASE_WAIT", 5.0)))
        self.request_interval = max(0.0, float(getattr(config, "GEMINI_REQUEST_INTERVAL", 2.0)))
        self._client = None

    def _get_client(self):
        """google-genai クライアントを遅延初期化"""
        if self._client is None:
            try:
                from google import genai

                self._client = genai.Client(api_key=self.api_key)
                logger.info(f"Gemini初期化完了: {self.model_name}")
            except Exception as e:
                logger.error(f"Gemini初期化エラー: {e}")
                raise
        return self._client

    # ------------------------------------------------------------------
    # エラー判定ヘルパー
    # ------------------------------------------------------------------
    def _is_quota_exceeded_error(self, error: Exception) -> bool:
        """クォータ超過エラーか判定"""
        text = str(error).lower()
        if "exceeded your current quota" in text:
            return True
        if "quota" in text and "billing" in text:
            return True
        if "insufficient quota" in text:
            return True
        return False

    def _is_rate_limit_error(self, error: Exception) -> bool:
        """レート制限エラーか判定"""
        text = str(error).lower()
        if "429" in text:
            return True
        if "resource_exhausted" in text:
            return True
        if "rate limit" in text:
            return True
        if "too many requests" in text:
            return True
        return False

    def _build_error_result(self, message: str, error_type: str) -> dict:
        """UI/DB連携用の標準エラー結果"""
        return {
            "summary": f"{ERROR_SUMMARY_PREFIX} {message}",
            "key_points": [],
            "keywords": [],
            "sentiment": "neutral",
            "signal_words": [],
            "is_error": True,
            "error": message,
            "error_type": error_type,
        }

    # ------------------------------------------------------------------
    # コア分析: PDFを直接Geminiへ送信
    # ------------------------------------------------------------------
    def analyze_pdf(
        self,
        pdf_path: str,
        code: str = "",
        company_name: str = "",
    ) -> dict:
        """PDFファイルを直接Geminiへアップロードして分析

        Args:
            pdf_path: PDFファイルパス
            code: 銘柄コード
            company_name: 企業名
        Returns:
            分析結果dict
        """
        from google.genai import types

        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            return self._build_error_result(
                f"PDFファイルが見つかりません: {pdf_path}", "file_not_found"
            )

        try:
            pdf_data = pdf_file.read_bytes()
        except Exception as e:
            return self._build_error_result(f"PDF読み込みエラー: {e}", "file_read_error")

        if len(pdf_data) == 0:
            return self._build_error_result("PDFファイルが空です", "empty_file")

        logger.info(f"PDF直接アップロード: {pdf_file.name} ({len(pdf_data):,} bytes)")

        prompt = ANALYSIS_PROMPT.format(code=code, company_name=company_name)

        try:
            client = self._get_client()
        except Exception as e:
            logger.error(f"Gemini初期化エラー ({code}): {e}")
            return self._build_error_result(str(e), "client_init_error")

        for attempt in range(self.max_retries + 1):
            try:
                response = client.models.generate_content(
                    model=self.model_name,
                    contents=[
                        types.Part.from_bytes(
                            data=pdf_data,
                            mime_type="application/pdf",
                        ),
                        prompt,
                    ],
                )
                response_text = response.text or ""
                if not response_text:
                    logger.warning(f"Geminiレスポンス空 ({code})")
                    return self._build_error_result(
                        "Geminiレスポンスが空です", "empty_response"
                    )

                result = self._parse_response(response_text)
                result["is_error"] = False
                return result
            except Exception as e:
                if self._is_quota_exceeded_error(e):
                    logger.error(f"Geminiクォータ超過 ({code}): {e}")
                    return self._build_error_result(
                        "Gemini APIクォータ超過。課金プランまたは使用量を確認してください。",
                        "quota_exceeded",
                    )

                if self._is_rate_limit_error(e) and attempt < self.max_retries:
                    wait_sec = self.retry_base_wait * (2**attempt)
                    logger.warning(
                        f"Geminiレート制限 ({code}): {e} / {wait_sec:.1f}秒後に再試行 "
                        f"({attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(wait_sec)
                    continue

                logger.error(f"Gemini分析エラー ({code}): {e}")
                if self._is_rate_limit_error(e):
                    return self._build_error_result(
                        "Gemini APIレート制限。時間を空けて再実行してください。",
                        "rate_limited",
                    )
                return self._build_error_result(str(e), "api_error")

        return self._build_error_result("Gemini分析失敗", "api_error")

    # ------------------------------------------------------------------
    # 後方互換: テキストベース分析 (必要に応じて残す)
    # ------------------------------------------------------------------
    def analyze_earnings(
        self,
        text: str,
        code: str = "",
        company_name: str = "",
    ) -> dict:
        """決算テキストをGeminiで分析 (後方互換用)"""
        from google.genai import types

        if not text.strip():
            return {
                "summary": "テキスト抽出できませんでした",
                "key_points": [],
                "keywords": [],
                "sentiment": "neutral",
                "signal_words": [],
                "is_error": False,
            }

        prompt = ANALYSIS_PROMPT.format(code=code, company_name=company_name)
        prompt += f"\n\n## 決算短信テキスト\n{text[:15000]}"

        try:
            client = self._get_client()
        except Exception as e:
            return self._build_error_result(str(e), "client_init_error")

        try:
            response = client.models.generate_content(
                model=self.model_name,
                contents=[prompt],
            )
            result = self._parse_response(response.text or "")
            result["is_error"] = False
            return result
        except Exception as e:
            logger.error(f"Gemini分析エラー ({code}): {e}")
            return self._build_error_result(str(e), "api_error")

    # ------------------------------------------------------------------
    # レスポンスパーサ
    # ------------------------------------------------------------------
    def _parse_response(self, response_text: str) -> dict:
        """GeminiレスポンスからJSON抽出・パース"""
        import re

        json_match = re.search(
            r"```(?:json)?\s*\n?(.*?)\n?\s*```", response_text, re.DOTALL
        )
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = response_text.strip()

        try:
            result = json.loads(json_str)
            return {
                "summary": result.get("summary", ""),
                "key_points": result.get("key_points", []),
                "keywords": result.get("keywords", []),
                "sentiment": result.get("sentiment", "neutral"),
                "signal_words": result.get("signal_words", []),
            }
        except json.JSONDecodeError:
            logger.warning("JSONパース失敗。テキストレスポンスを使用。")
            return {
                "summary": response_text[:500],
                "key_points": [],
                "keywords": [],
                "sentiment": "neutral",
                "signal_words": [],
            }

    # ------------------------------------------------------------------
    # DB連携: 分析 → 保存
    # ------------------------------------------------------------------
    def analyze_and_save(
        self,
        pdf_path: str,
        code: str,
        disclosed_date: str,
        disclosure_number: str = "",
        company_name: str = "",
    ) -> dict:
        """PDFを分析してDBに保存

        Args:
            pdf_path: PDFパス
            code: 銘柄コード
            disclosed_date: 開示日
            disclosure_number: 開示番号
            company_name: 企業名
        Returns:
            分析結果dict
        """
        # 既存の分析結果をチェック
        session = get_session()
        try:
            existing = (
                session.query(AIAnalysisResult)
                .filter_by(code=code, disclosure_number=disclosure_number)
                .first()
            )
            if (
                existing
                and existing.summary
                and not existing.summary.startswith(ERROR_SUMMARY_PREFIX)
            ):
                logger.info(f"分析済み: {code} ({disclosure_number})")
                return {
                    "summary": existing.summary,
                    "key_points": json.loads(existing.key_points) if existing.key_points else [],
                    "keywords": json.loads(existing.keywords) if existing.keywords else [],
                    "sentiment": existing.sentiment,
                    "signal_words": json.loads(existing.signal_words) if existing.signal_words else [],
                    "is_error": False,
                }
        finally:
            session.close()

        # PDF直接分析 (テキスト抽出不要)
        result = self.analyze_pdf(pdf_path, code, company_name)

        # API失敗時はDB保存せず、次回再実行可能にする
        if result.get("is_error"):
            logger.warning(f"AI分析失敗 (未保存): {code} {result.get('error', '')}")
            return result

        # DBに保存
        session = get_session()
        try:
            dt = datetime.strptime(disclosed_date.replace("-", ""), "%Y%m%d").date()

            existing = (
                session.query(AIAnalysisResult)
                .filter_by(code=code, disclosure_number=disclosure_number)
                .first()
            )

            values = dict(
                code=code,
                disclosed_date=dt,
                disclosure_number=disclosure_number,
                summary=result.get("summary", ""),
                key_points=json.dumps(result.get("key_points", []), ensure_ascii=False),
                keywords=json.dumps(result.get("keywords", []), ensure_ascii=False),
                sentiment=result.get("sentiment", "neutral"),
                signal_words=json.dumps(result.get("signal_words", []), ensure_ascii=False),
                model_used=self.model_name,
                analyzed_at=datetime.utcnow(),
            )

            if existing:
                for key, val in values.items():
                    setattr(existing, key, val)
            else:
                ai_result = AIAnalysisResult(**values)
                session.add(ai_result)

            session.commit()
            logger.info(f"AI分析結果保存: {code}")
        except Exception as e:
            session.rollback()
            logger.error(f"AI分析結果保存エラー ({code}): {e}")
            raise
        finally:
            session.close()

        return result

    # ------------------------------------------------------------------
    # バッチ処理
    # ------------------------------------------------------------------
    def batch_analyze(
        self,
        items: list[dict],
        progress_callback=None,
    ) -> list[dict]:
        """複数PDFを一括分析

        Args:
            items: [{'pdf_path': str, 'code': str, 'disclosed_date': str, ...}, ...]
            progress_callback: fn(current, total)
        Returns:
            分析結果のリスト
        """
        results = []
        total = len(items)
        quota_exhausted = False

        for i, item in enumerate(items, 1):
            code = item.get("code", "")

            if quota_exhausted:
                results.append({
                    "code": code,
                    "success": False,
                    "error": "Gemini APIクォータ超過のため残件をスキップしました",
                    "error_type": "quota_exceeded",
                })
                if progress_callback:
                    progress_callback(i, total)
                continue

            try:
                result = self.analyze_and_save(
                    pdf_path=item["pdf_path"],
                    code=code,
                    disclosed_date=item.get("disclosed_date", ""),
                    disclosure_number=item.get("disclosure_number", ""),
                    company_name=item.get("company_name", ""),
                )
                if result.get("is_error"):
                    logger.warning(f"分析失敗 ({code}): {result.get('error', '')}")
                    results.append({
                        "code": code,
                        "success": False,
                        "error": result.get("error", "分析失敗"),
                        "error_type": result.get("error_type", "api_error"),
                    })
                    if result.get("error_type") == "quota_exceeded":
                        quota_exhausted = True
                else:
                    result["code"] = code
                    result["success"] = True
                    results.append(result)
            except Exception as e:
                logger.warning(f"分析スキップ ({code}): {e}")
                results.append({
                    "code": code,
                    "success": False,
                    "error": str(e),
                })

            if progress_callback:
                progress_callback(i, total)

            # Gemini APIのレート制限回避
            if self.request_interval > 0 and i < total:
                time.sleep(self.request_interval)

        success_count = sum(1 for r in results if r.get("success"))
        logger.info(f"AI分析完了: {success_count}/{total}件")
        return results
