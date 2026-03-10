# サッカー試合予測アプリ - 開発規約

## 📋 プロジェクト概要

Snowflake上で動作するサッカー試合予測Streamlitアプリケーション。
ユーザーは試合結果を予想し、AI（Snowflake Cortex）による試合プレビューを閲覧できます。

---

## 📁 プロジェクト構成

```text
.
├── streamlit_app.py   # メインアプリケーション
├── AGENTS.md          # 開発規約（本ファイル）
└── requirements.txt   # 依存パッケージ（後述）
```

### requirements.txt

Streamlit in Snowflakeでは以下は自動管理のため記載不要：
- `snowflake-snowpark-python`
- `streamlit`

手動追加が必要な場合のみ記載：

```
# 例：追加ライブラリが必要な場合のみ
pandas==2.0.0
```

---

## 🎯 技術スタック

- **UI**: Streamlit (Snowflake Native App)
- **データベース**: Snowflake (SOCCER_APP.MATCH_DATA)
- **AI**: Snowflake Cortex AI (claude-3-5-sonnet, クロスリージョン推論)
- **キャッシング**: Streamlit Cache API

---

## 🗄️ データベース構造

### 完全修飾名

**すべてのテーブル**: `SOCCER_APP.MATCH_DATA.*`

### テーブル定義

#### MATCHES

```sql
- match_id     (NUMBER)     - PK
- home_team_id (NUMBER)     - FK to TEAMS
- away_team_id (NUMBER)     - FK to TEAMS
- match_date   (DATE)
- home_score   (NUMBER)
- away_score   (NUMBER)
- result       (VARCHAR(1)) - 'H'/'D'/'A'
```

#### TEAMS

```sql
- team_id   (NUMBER)  - PK
- team_name (VARCHAR)
```

#### PREDICTIONS（Hybrid Table）

```sql
- prediction_id    (NUMBER)     - PK, IDENTITY
- match_id         (NUMBER)     - FK to MATCHES
- user_name        (VARCHAR)
- predicted_result (VARCHAR(1)) - 'H'/'D'/'A'
- is_correct       (BOOLEAN)
- created_at       (TIMESTAMP)
```

> ⚠️ **Hybrid Table 制約事項**
> - ❌ TIME TRAVEL 非サポート（誤削除からの復元不可）
> - ❌ CLUSTERING KEY 非サポート
> - ✅ PRIMARY KEY インデックスによる低レイテンシ読み書き
> - ✅ 行レベルロック対応
> - ⚠️ WAREHOUSE サイズを XS にすると書き込みが詰まる場合あり

#### LEADERBOARD

```sql
- user_name           (VARCHAR) - PK
- total_predictions   (NUMBER)
- correct_predictions (NUMBER)
- accuracy_rate       (NUMBER)
- last_updated        (TIMESTAMP)
```

#### TEAM_FORM（ビュー）

```sql
- team_name       (VARCHAR)
- wins_last_5     (NUMBER)  - 直近5試合の勝利数
- draws_last_5    (NUMBER)
- losses_last_5   (NUMBER)
- goals_for_5     (NUMBER)
- goals_against_5 (NUMBER)
```

> ※ MATCHES テーブルから動的に集計するビュー。`GET_MATCH_PREVIEW` 関数内で参照。

### ストアドプロシージャ/関数

- `GET_MATCH_PREVIEW(home_team, away_team)` — Cortex AI による試合プレビュー生成

---

## 💻 コーディング規約

### 1. Snowflake接続

✅ 正しい実装

```python
@st.cache_resource
def get_session():
    from snowflake.snowpark.context import get_active_session
    return get_active_session()

def get_valid_session():
    """セッション失効チェック付きで取得"""
    session = get_session()
    try:
        session.sql("SELECT 1").collect()  # 疎通確認
        return session
    except Exception:
        get_session.clear()  # ゾンビセッションを破棄して再取得
        return get_session()
```

❌ 誤った実装

```python
# グローバルで即実行（起動が遅くなる・失効対策なし）
session = get_active_session()
```

---

### 2. ユーザー名バリデーション

ログイン時の「非空白チェック」のみでは不十分。XSS・SQLインジェクションの入口になりえる。

```python
import re

def validate_username(name: str) -> tuple[bool, str]:
    if not name or not name.strip():
        return False, "ユーザー名を入力してください"
    if len(name) > 50:
        return False, "50文字以内で入力してください"
    if not re.match(r'^[a-zA-Z0-9_\u3040-\u9FFF]+$', name):
        return False, "使用できない文字が含まれています"
    return True, ""
```

許可文字: 英数字・アンダースコア・ひらがな・カタカナ・漢字

---

### 3. SQLクエリ

✅ 正しい実装（パラメータ化クエリ）

```python
query = "SELECT * FROM SOCCER_APP.MATCH_DATA.PREDICTIONS WHERE user_name = ?"
result = session.sql(query, params=[user_name]).to_pandas()
```

❌ 誤った実装（SQLインジェクションリスク）

```python
# f-string使用禁止
query = f"SELECT * FROM PREDICTIONS WHERE user_name = '{user_name}'"
```

✅ MERGE文の使用

```python
# 重複防止のため、INSERT ではなく MERGE を使用
query = """
MERGE INTO SOCCER_APP.MATCH_DATA.PREDICTIONS p
USING (SELECT ? AS match_id, ? AS user_name, ? AS predicted_result) s
ON p.match_id = s.match_id AND p.user_name = s.user_name
WHEN MATCHED THEN
    UPDATE SET predicted_result = s.predicted_result
WHEN NOT MATCHED THEN
    INSERT (match_id, user_name, predicted_result)
    VALUES (s.match_id, s.user_name, s.predicted_result)
"""
```

---

### 4. キャッシング戦略

| 関数 | TTL | 理由 |
|------|-----|------|
| `get_session()` | 永続 | `@st.cache_resource` |
| `get_upcoming_matches()` | 60秒 | 予想保存で変更される |
| `get_leaderboard()` | 300秒 | 頻繁な更新不要 |
| `get_match_preview()` | 600秒 | Cortex AI 呼び出しが高コスト |
| `get_user_predictions()` | 60秒 | ユーザー操作で変更 |

✅ キャッシュクリアの一元管理

```python
def clear_prediction_cache():
    get_upcoming_matches.clear()
    get_user_predictions.clear()

def save_prediction(...):
    # 保存処理
    clear_prediction_cache()
    st.session_state.leaderboard_updated = False
    return True
```

---

### 5. パフォーマンス計測

#### デコレータ適用順序（重要）

```python
# ✅ 正しい順序（キャッシュ miss 時のみ計測）
@st.cache_data(ttl=60)   # 外側：キャッシュ判定が先
@track_performance        # 内側：キャッシュ miss 時のみ実行
def get_upcoming_matches():
    ...

# ❌ 逆にすると全呼び出しを計測してしまう
@track_performance
@st.cache_data(ttl=60)
def get_upcoming_matches():
    ...
```

#### デコレータ実装

```python
def track_performance(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start

        metric = {
            'function': func.__name__,
            'elapsed': elapsed,
            'timestamp': time.strftime('%H:%M:%S')
        }
        st.session_state.perf_metrics.append(metric)

        if st.session_state.get('dev_mode', False) and elapsed > 1.0:
            st.warning(f"⚠️ {func.__name__}が{elapsed:.2f}秒かかりました")

        return result
    return wrapper
```

---

### 6. SQL検証（validate_sql関数）

`dev_mode` が有効な場合のみ実行されるSQL品質チェック関数。

**チェック内容**

- WHERE句にパラメータ化されていないユーザー入力がないか
- テーブルが完全修飾名で参照されているか

```python
def validate_sql(query, params=None, context=""):
    if not st.session_state.get('dev_mode', False):
        return []

    issues = []

    if "WHERE" in query.upper() and params is None:
        # 固定値パターン（IS NULL / 数値 / 固定文字列）は除外
        fixed_patterns = [
            r"WHERE\s+\w+\.\w+\s+IS\s+(NULL|NOT NULL)",
            r"WHERE\s+\w+\s+=\s+\d+",
            r"WHERE\s+\w+\s+=\s+'[^']*'"
        ]
        is_fixed_value = any(
            re.search(p, query, re.IGNORECASE) for p in fixed_patterns
        )
        if not is_fixed_value and "?" not in query:
            issues.append(
                f"⚠️ [{context}] WHERE句にユーザー入力がある場合はパラメータ化してください"
            )

    return issues
```

---

### 7. セッション状態管理

✅ `dev_mode` のシンプルな管理

```python
# checkbox の key で自動同期（二重管理を避ける）
st.sidebar.checkbox("🔧 開発者モード", key="dev_mode")

# 参照時
if st.session_state.get('dev_mode', False):
    # 開発者モード処理
```

---

### 8. パフォーマンス最適化

✅ ログイン前の処理スキップ

```python
if not st.session_state.user_name:
    st.info("👈 サイドバーからログインしてください")
    st.stop()
```

✅ セッション状態による重複処理防止

```python
def update_leaderboard():
    if st.session_state.leaderboard_updated:
        return  # 既に更新済みならスキップ
    # 更新処理
    st.session_state.leaderboard_updated = True
```

✅ ランキング圏外ユーザーへの対応

```python
# TOP10 に入っていない場合は別途個人成績を取得
if user_data.empty:
    personal_query = """
    SELECT user_name, total_predictions, correct_predictions, accuracy_rate
    FROM SOCCER_APP.MATCH_DATA.LEADERBOARD
    WHERE user_name = ?
    """
    personal = session.sql(
        personal_query, params=[st.session_state.user_name]
    ).to_pandas()
    if not personal.empty:
        st.info("あなたはTOP10圏外です（現在の成績を表示）")
        # 成績表示
    else:
        st.info("まだランキングに登録されていません")
```

---

### 9. エラーハンドリング

✅ 粒度を分けた標準パターン

```python
from snowflake.snowpark.exceptions import SnowparkSQLException

def my_function():
    try:
        result = session.sql(query).collect()
        return result
    except SnowparkSQLException as e:
        st.error(f"SQLエラー: {e.message}")
        return None
    except Exception as e:
        st.error(f"予期しないエラー: {str(e)}")
        return None
```

✅ AI出力のプロンプト露出対策（改善版）

```python
def clean_ai_output(text: str) -> str:
    if not text:
        return "プレビューを生成できませんでした"

    # [INST]タグを除去（文頭・途中どちらにも対応）
    if '[INST]' in text:
        parts = text.split('[INST]')
        candidates = [p.strip() for p in parts if p.strip()]
        text = candidates if candidates else ""

    return text if text else "プレビューを生成できませんでした"
```

---

## 🔒 セキュリティガイドライン

### 必須対策

- ✅ 全SQLをパラメータ化 — `params=[...]` を必ず使用
- ✅ ユーザー入力のバリデーション — `validate_username()` を使用
- ✅ セッション状態の管理 — 不正アクセス防止
- ✅ AI生成コンテンツのサニタイズ — XSS対策

### AI生成コンテンツのXSS対策

```python
import html

# ✅ 必須：AI出力をHTMLに埋め込む前にサニタイズ
safe_text = html.escape(preview)
st.markdown(
    f'<div class="ai-preview"><p>{safe_text}</p></div>',
    unsafe_allow_html=True
)

# ❌ 禁止：AIの生テキストをそのままHTMLに埋め込む
st.markdown(f'<p>{preview}</p>', unsafe_allow_html=True)
```

### 禁止事項

- ❌ f-stringでのSQL構築
- ❌ USE SCHEMA文の使用（Streamlit in Snowflakeで非サポート）
- ❌ 完全修飾名を省略したテーブル参照
- ❌ AI出力のサニタイズなしでのHTML埋め込み
- ❌ ユーザー名の未バリデーションでのDB操作

---

## ⚙️ Cortex AI設定

### クロスリージョン推論

高品質なAIモデル（claude-3-5-sonnet等）を使用するため、クロスリージョン推論を有効化。

```sql
-- ✅ 推奨：セッション単位で制御（影響範囲が限定的）
ALTER SESSION SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US_WEST_2';

-- ❌ 非推奨：アカウント全体に影響（ACCOUNTADMIN 必須・誤操作リスクあり）
-- ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
```

> `AWS_US_WEST_2`（Oregon）は日本からの太平洋横断ルートで最短レイテンシのため推奨。

### GET_MATCH_PREVIEW 関数

```sql
CREATE OR REPLACE FUNCTION SOCCER_APP.MATCH_DATA.GET_MATCH_PREVIEW(
    HOME_TEAM_NAME VARCHAR,
    AWAY_TEAM_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS '
  SELECT SNOWFLAKE.CORTEX.COMPLETE(
    ''claude-3-5-sonnet'',
    CONCAT(
      ''あなたはサッカー試合分析の専門家です。150文字以内で簡潔に予想してください。\n\n'',
      ''ホームチーム: '', home_team_name,
      -- ... 以下省略
    )
  )
';
```

### AI出力の勝率パース（⚠️ 未実装・将来対応予定）

実装予定タイミング: フェーズ4（スコア予測追加時）

```python
# TODO: 以下のパターンで勝率を抽出予定
patterns = [
    r'(\d+)\s*%\s*/\s*(\d+)\s*%\s*/\s*(\d+)\s*%',
    r'ホーム.*?(\d+)\s*%.*?引き分け.*?(\d+)\s*%.*?アウェ.*?(\d+)\s*%',
]

for pattern in patterns:
    match = re.search(pattern, full_text, re.IGNORECASE)
    if match:
        home_pct = int(match.group(1))
        draw_pct = int(match.group(2))
        away_pct = int(match.group(3))
        break
```

---

## 🧪 テスト手順

### 機能テスト

**ログイン機能**
- [ ] ユーザー名入力で正常にログイン
- [ ] 禁止文字・50文字超でバリデーションエラーが表示される
- [ ] ログアウトで状態がクリアされる

**試合予想**
- [ ] ホーム勝ち/引き分け/アウェイ勝ちの保存
- [ ] 既存予想の上書き動作確認
- [ ] 保存後のメッセージ表示

**AI試合プレビュー**
- [ ] プレビューが正常に表示される
- [ ] プロンプト（`[INST]`タグ）が露出していない
- [ ] キャッシュが効いている（2回目は高速）
- [ ] XSSサニタイズが適用されている

**結果確認**
- [ ] 自分の予想一覧が表示される
- [ ] 正解/不正解の判定が正確
- [ ] 統計情報（正答率等）が正しい
- [ ] 日本語表示（H/D/A → ホーム勝ち等）

**ランキング**
- [ ] 上位10名が表示される
- [ ] TOP10圏外ユーザーに個人成績が表示される
- [ ] 自分の順位が正確に表示される
- [ ] メダル表示（🥇🥈🥉）が正しい

### パフォーマンステスト

- [ ] 起動時間が3秒以内
- [ ] ログイン前の処理がスキップされる
- [ ] AI予想のキャッシュが効いている
- [ ] 開発者モードでパフォーマンス計測が表示される
- [ ] セッション失効後に自動再接続される

### セキュリティテスト

- [ ] XSSペイロード（例：`<script>alert(1)</script>`）がエスケープされる
- [ ] SQLインジェクション（例：`' OR '1'='1`）が防止される
- [ ] 禁止文字を含むユーザー名が拒否される

---

## 🐛 よくあるエラーと対処法

| # | エラー | 原因 | 対処法 |
|---|--------|------|--------|
| 1 | `Object 'MATCHES' does not exist` | 完全修飾名未使用 | `SOCCER_APP.MATCH_DATA.MATCHES` に変更 |
| 2 | `Unsupported statement type 'USE'` | USE SCHEMA文の使用 | 完全修飾名でテーブル参照 |
| 3 | SQLインジェクション警告 | f-stringでSQL構築 | `session.sql(query, params=[...])` に変更 |
| 4 | プロンプトが画面に表示される | Cortex AI出力に`[INST]`タグ | `clean_ai_output()` で除去 |
| 5 | 起動が遅い | グローバルでセッション取得 | `@st.cache_resource` + `get_valid_session()` |
| 6 | クロスリージョンモデル使用不可 | `CORTEX_ENABLED_CROSS_REGION` 未設定 | `ALTER SESSION SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US_WEST_2'` |
| 7 | XSS脆弱性 | AI出力をサニタイズせずHTML埋め込み | `html.escape()` を使用 |
| 8 | セッション失効エラー | 長時間放置後の操作 | `get_valid_session()` で疎通確認 |

---

## 🚀 デプロイ前チェックリスト

**セキュリティ**
- [ ] 全SQLがパラメータ化されている
- [ ] f-string使用箇所がない
- [ ] ユーザー名バリデーションが実装されている
- [ ] セッション状態が適切に管理されている
- [ ] AI出力が `html.escape()` でサニタイズされている

**パフォーマンス**
- [ ] 全関数に適切なキャッシュ設定がある
- [ ] デコレータの適用順序が正しい（`@cache_data` → `@track_performance`）
- [ ] ログイン前の不要な処理が実行されない
- [ ] AI予想が適切にキャッシュされている
- [ ] キャッシュクリアが一元管理されている
- [ ] `get_valid_session()` でセッション失効対策が実装されている

**機能**
- [ ] 全ページでログイン確認が動作する
- [ ] 予想の保存が正常に動作する
- [ ] ランキングが正確に計算される
- [ ] TOP10圏外ユーザーに成績が表示される

**UX**
- [ ] エラーメッセージが分かりやすい
- [ ] ローディング状態（spinner）が表示される
- [ ] AI予想でプロンプトが露出していない
- [ ] 日本語表示が適切

---

## 📐 関数シグネチャ標準

### バリデーション関数

```python
def validate_xxx(value: str) -> tuple[bool, str]:
    """
    戻り値: (成功フラグ, エラーメッセージ)
    成功時は (True, "") を返す
    """
```

### データ取得関数

```python
@st.cache_data(ttl=60)
@track_performance
def get_xxx() -> pd.DataFrame:
    """
    必ずDataFrameを返す（空の場合もpd.DataFrame()）
    エラー時はst.error()表示後に空DataFrameを返す
    """
```

### 更新関数

```python
def save_xxx(...) -> bool:
    """
    戻り値: 成功/失敗
    成功時: clear_prediction_cache() を呼ぶ
    失敗時: st.error() で理由を表示
    """
```

---

## 🗂️ セッション状態の初期化

### 初期化パターン（ファイル冒頭で実行）

```python
REQUIRED_SESSION_STATES = {
    'user_name': "",
    'leaderboard_updated': False,
    'perf_metrics': [],
}

for key, default in REQUIRED_SESSION_STATES.items():
    if key not in st.session_state:
        st.session_state[key] = default
```

### ログアウト時のリセット

```python
def logout():
    st.session_state.user_name = ""
    st.session_state.leaderboard_updated = False
    st.rerun()
```

> ⚠️ `st.session_state` の直接クリア（`del st.session_state[key]`）は避け、明示的に初期値を代入する


## 📚 参考リソース

- [Streamlit Documentation](https://docs.streamlit.io)
- [Snowflake Cortex AI](https://docs.snowflake.com/en/user-guide/snowflake-cortex/overview)
- [Snowpark Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)

---

最終更新: 2026-03-10　バージョン: 3.0.0
