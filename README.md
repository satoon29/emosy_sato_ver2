# EmoSy (Emotion Sympathy) - 感情記録可視化ダッシュボード

Firestore に蓄積された感情記録データを可視化する Streamlit アプリケーションと、実験データの分析スクリプト群です。

---

## 機能概要

### Streamlit ダッシュボード (`app.py`)
- **1日間 / 3日間表示**: 指定期間の感情バレンス時系列・感情マップ・入力履歴を表示
- **累積分析**: 全期間の感情データを集計し、累積グラフ・感情クラスタ円グラフ・地図を表示
- **トークン認証**: URLパラメータ (`?t=<token>`) によるユーザー認証
- **アクセスログ**: Firestore にアクセスログ・ページビュー滞在時間を記録

### 分析スクリプト
| スクリプト | 内容 |
|---|---|
| `emotion_response_analysis.py` | ユーザー・群別の感情入力率（回答率）を集計・グラフ出力 |
| `analyze_access_logs.py` | アクセスログの集計（ユーザー別・日別・表示モード別） |
| `analyze_access_by_condition.py` | 群別（スマートフォン通知条件 / ロボット共感条件）の日別アクセス数推移を分析 |
| `calculate_group_response_rates.py` | 各群の実験期間全体回答率を比較・棒グラフ出力 |

---

## ディレクトリ構成

```
emosy_sato_ver2/
├── app.py                              # Streamlit メインアプリ
├── config.py                           # 設定項目（Firebase認証パス等）
├── data_handler.py                     # データ取得・加工ロジック
├── ui_components.py                    # UI描画コンポーネント
├── style.css                           # カスタムCSS
├── requirements.txt                    # Python依存パッケージ
├── .gitignore
├── assets/
│   ├── NotoSansJP-Regular.ttf          # 日本語フォント
│   └── emoji_list/                     # 絵文字画像
├── .streamlit/
│   └── secrets.toml                    # Streamlit Secrets（Git管理外）
├── emotion_response_analysis.py        # 回答率分析スクリプト
├── analyze_access_logs.py              # アクセスログ分析スクリプト
├── analyze_access_by_condition.py      # 群別アクセス分析スクリプト
└── calculate_group_response_rates.py   # 群別回答率比較スクリプト
```

---

## セットアップ

### 1. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 2. Firebase 認証情報の配置

#### ローカル環境
Firebase Admin SDK のサービスアカウントキー JSON を取得し、プロジェクトルートに配置します。  
`config.py` の `FIREBASE_CREDENTIALS_PATH` にファイル名を設定してください。

```python
FIREBASE_CREDENTIALS_PATH = "your-firebase-adminsdk.json"
```

#### Streamlit Cloud 環境
`.streamlit/secrets.toml` に以下の形式で設定します。

```toml
[firebase_credentials]
type = "service_account"
project_id = "your-project-id"
private_key_id = "..."
private_key = "..."
client_email = "..."
client_id = "..."
# ... その他のフィールド

[tokens]
"your-token-string" = "user_id"
```

### 3. 日本語フォントの配置

`assets/NotoSansJP-Regular.ttf` に Noto Sans JP フォントを配置してください。  
グラフの日本語表示に使用されます。

---

## 使い方

### ダッシュボードの起動

```bash
streamlit run app.py
```

ブラウザで以下のようにトークン付きURLでアクセスします。

```
http://localhost:8501/?t=your-token
```

### 分析スクリプトの実行

```bash
# 感情入力率の分析
python emotion_response_analysis.py

# アクセスログの分析
python analyze_access_logs.py

# 群別アクセス数の分析
python analyze_access_by_condition.py

# 群別回答率の比較
python calculate_group_response_rates.py
```

各スクリプトはテキストレポート（`.txt`）とグラフ（`.pdf`）を出力します。

---

## Firestore データ構造

```
users/
  {user_id}/
    emotions/          # 感情記録
      {doc_id}: { day, time, valence, cluster, lat, lng, emoji, ... }
    page_views/        # ページビューログ
      {doc_id}: { session_id, view_mode, start_time, end_time, duration_seconds }

access_logs/           # アクセスログ
  {doc_id}: { user_id, token, timestamp, session_id, view_mode }
```

---

## 主な技術スタック

- **Python 3.10+**
- **Streamlit** — Web ダッシュボード
- **Firebase Admin SDK** — Firestore データベース
- **Matplotlib** — グラフ描画
- **Pandas** — データ処理
- **Folium / streamlit-folium** — 地図表示

---

## 注意事項

- Firebase 認証情報ファイル（`.json`）や `.streamlit/secrets.toml` は `.gitignore` に含まれており、Git にコミットされません。
- 実験期間の定義は各分析スクリプト内の `EXPERIMENT_PERIODS` で管理されています。変更が必要な場合は該当箇所を編集してください。
