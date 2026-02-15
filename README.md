# KessanView - 決算分析補助ツール

日本株の決算シーズンにおいて、1日400件以上の決算短信を効率的にスクリーニング・分析するための個人向けツール。

## 機能

- **J-Quants API連携**: 財務データ取得・DB保存（プラン別レート制限対応）
- **前Q/前Y比較**: 売上高・営業利益・純利益の変化率を自動計算
- **重要度スコアリング**: 0-100スコアで決算の注目度をランク付け
- **TDnet WEB-API連携**: 決算短信PDF自動ダウンロード
- **AI決算分析**: Geminiによる要約・キーワード抽出・センチメント判定
- **シングルページUI**: Streamlitによるスクロール型ダッシュボード

## セットアップ

```bash
# 1. 仮想環境作成
python3 -m venv venv
source venv/bin/activate

# 2. パッケージインストール
pip install -r requirements.txt

# 3. 環境変数設定
cp .env.example .env
# .env を編集してAPIキーを設定

# 4. 起動
streamlit run app.py
```

## 必要なAPIキー

| API | 取得先 |
|-----|--------|
| J-Quants API | https://jpx-jquants.com/ |
| Gemini API | https://aistudio.google.com/ |

## 使い方

1. サイドバーで対象日付を選択
2. 「同期実行」でJ-Quantsからデータ取得
3. 「スコアリング実行」で重要度スコアを計算
4. スコアランキングで注目銘柄を確認
5. 銘柄を選択して詳細・AI分析結果を閲覧
