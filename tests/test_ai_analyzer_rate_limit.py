import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.ai_analyzer import AIAnalyzer


class _DummyResponse:
    def __init__(self, text: str):
        self.text = text


def test_analyze_earnings_retries_on_rate_limit(monkeypatch):
    analyzer = AIAnalyzer(api_key="dummy")
    analyzer.max_retries = 2
    analyzer.retry_base_wait = 1.5

    calls = {"count": 0}

    class _DummyClient:
        def generate_content(self, prompt: str):
            calls["count"] += 1
            if calls["count"] < 3:
                raise Exception("429 rate limit exceeded")
            return _DummyResponse(
                '{"summary":"ok","key_points":[],"keywords":[],"sentiment":"neutral","signal_words":[]}'
            )

    wait_calls = []
    monkeypatch.setattr(analyzer, "_get_client", lambda: _DummyClient())
    monkeypatch.setattr("services.ai_analyzer.time.sleep", lambda sec: wait_calls.append(sec))

    result = analyzer.analyze_earnings("dummy text", code="1234", company_name="Test")

    assert result["is_error"] is False
    assert result["summary"] == "ok"
    assert calls["count"] == 3
    assert wait_calls == [1.5, 3.0]


def test_analyze_earnings_quota_exceeded_stops_immediately(monkeypatch):
    analyzer = AIAnalyzer(api_key="dummy")
    analyzer.max_retries = 3

    calls = {"count": 0}

    class _DummyClient:
        def generate_content(self, prompt: str):
            calls["count"] += 1
            raise Exception(
                "429 You exceeded your current quota, please check your plan and billing details."
            )

    wait_calls = []
    monkeypatch.setattr(analyzer, "_get_client", lambda: _DummyClient())
    monkeypatch.setattr("services.ai_analyzer.time.sleep", lambda sec: wait_calls.append(sec))

    result = analyzer.analyze_earnings("dummy text", code="5678", company_name="Quota")

    assert result["is_error"] is True
    assert result["error_type"] == "quota_exceeded"
    assert calls["count"] == 1
    assert wait_calls == []


def test_batch_analyze_skips_remaining_items_after_quota_exceeded(monkeypatch):
    analyzer = AIAnalyzer(api_key="dummy")
    analyzer.request_interval = 0

    calls = {"count": 0}

    def _fake_analyze_and_save(**kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "is_error": True,
                "error": "quota exceeded",
                "error_type": "quota_exceeded",
            }
        return {
            "is_error": False,
            "summary": "ok",
            "key_points": [],
            "keywords": [],
            "sentiment": "neutral",
            "signal_words": [],
        }

    monkeypatch.setattr(analyzer, "analyze_and_save", _fake_analyze_and_save)

    items = [
        {"pdf_path": "a.pdf", "code": "1111", "disclosed_date": "2026-02-13"},
        {"pdf_path": "b.pdf", "code": "2222", "disclosed_date": "2026-02-13"},
    ]

    results = analyzer.batch_analyze(items)

    assert calls["count"] == 1
    assert len(results) == 2
    assert results[0]["success"] is False
    assert results[0]["error_type"] == "quota_exceeded"
    assert results[1]["success"] is False
    assert results[1]["error_type"] == "quota_exceeded"
