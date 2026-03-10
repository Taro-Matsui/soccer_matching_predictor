import streamlit as st
import pandas as pd
import html
import time
import re
import json
from functools import wraps

st.set_page_config(
    page_title="サッカー試合予測",
    page_icon="⚽",
    layout="wide"
)

st.markdown("""
<style>
    .main-header { text-align: center; padding: 1rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 10px; margin-bottom: 2rem; }
    .match-card { background: white; border-radius: 15px; padding: 1.5rem; margin: 1rem 0; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 5px solid #667eea; }
    .team-name { font-size: 1.3rem; font-weight: bold; color: #333; }
    .vs-badge { background: #667eea; color: white; padding: 0.3rem 1rem; border-radius: 20px; font-weight: bold; }
    .match-date { color: #666; margin-top: 0.5rem; font-size: 0.9rem; }
    .prediction-btn { margin: 0.5rem; }
    .stats-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 10px; padding: 1.5rem; text-align: center; }
    .stats-value { font-size: 2rem; font-weight: bold; }
    .stats-label { font-size: 0.9rem; opacity: 0.9; }
    .ranking-row { display: flex; align-items: center; padding: 1rem; margin: 0.5rem 0; background: white; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .rank-1 { border-left: 5px solid gold; background: linear-gradient(90deg, rgba(255,215,0,0.1) 0%, white 100%); }
    .rank-2 { border-left: 5px solid silver; }
    .rank-3 { border-left: 5px solid #cd7f32; }
    .rank-other { border-left: 5px solid #667eea; }
    .page-header { text-align: center; margin-bottom: 2rem; }
    .page-header h1 { color: #333; margin-bottom: 0.5rem; }
    .page-header p { color: #666; }
    .ai-preview { background: #f8f9fa; border-radius: 10px; padding: 1rem; margin-top: 1rem; border-left: 4px solid #667eea; }
    .ai-preview h4 { color: #667eea; margin-bottom: 0.5rem; }
    .odds-bar { display: flex; height: 30px; border-radius: 15px; overflow: hidden; margin: 10px 0; }
    .odds-home { background: #28a745; color: white; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 0.85rem; }
    .odds-draw { background: #6c757d; color: white; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 0.85rem; }
    .odds-away { background: #dc3545; color: white; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 0.85rem; }
    .form-indicator { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.8rem; font-weight: bold; }
    .form-good { background: #d4edda; color: #155724; }
    .form-mid { background: #fff3cd; color: #856404; }
    .form-bad { background: #f8d7da; color: #721c24; }
    .h2h-badge { background: #e9ecef; padding: 5px 10px; border-radius: 5px; font-size: 0.85rem; margin: 5px 0; }
    .stat-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eee; }
    .stat-label { color: #666; }
    .stat-value { font-weight: bold; }
</style>
""", unsafe_allow_html=True)

REQUIRED_SESSION_STATES = {
    'user_name': "",
    'leaderboard_updated': False,
    'perf_metrics': [],
}

for key, default in REQUIRED_SESSION_STATES.items():
    if key not in st.session_state:
        st.session_state[key] = default

def validate_username(name: str) -> tuple[bool, str]:
    if not name or not name.strip():
        return False, "ユーザー名を入力してください"
    if len(name) > 50:
        return False, "50文字以内で入力してください"
    if not re.match(r'^[a-zA-Z0-9_\u3040-\u9FFF]+$', name):
        return False, "使用できない文字が含まれています（英数字・アンダースコア・日本語のみ）"
    return True, ""

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

@st.cache_resource
def get_session():
    from snowflake.snowpark.context import get_active_session
    return get_active_session()

def get_valid_session():
    session = get_session()
    try:
        session.sql("SELECT 1").collect()
        return session
    except Exception:
        get_session.clear()
        return get_session()

def clear_prediction_cache():
    get_upcoming_matches.clear()
    get_user_predictions.clear()

def clean_ai_output(text: str) -> str:
    if not text:
        return "プレビューを生成できませんでした"
    
    if '[INST]' in text:
        parts = text.split('[INST]')
        candidates = [p.strip() for p in parts if p.strip()]
        text = candidates[0] if candidates else ""
    
    if '[/INST]' in text:
        parts = text.split('[/INST]')
        candidates = [p.strip() for p in parts if p.strip()]
        text = candidates[-1] if candidates else ""
    
    return text if text else "プレビューを生成できませんでした"

def get_form_class(form_rating: float) -> str:
    if form_rating >= 60:
        return "form-good"
    elif form_rating >= 40:
        return "form-mid"
    else:
        return "form-bad"

def generate_fallback_analysis(home_team: str, away_team: str, stats: dict) -> str:
    home_form = stats.get('home_form', 50)
    away_form = stats.get('away_form', 50)
    home_goals = stats.get('home_goals_for', 0)
    away_goals = stats.get('away_goals_for', 0)
    home_conceded = stats.get('home_goals_against', 0)
    away_conceded = stats.get('away_goals_against', 0)
    h2h_home = stats.get('h2h_home_wins', 0)
    h2h_away = stats.get('h2h_away_wins', 0)
    
    analysis_parts = []
    
    if home_form > away_form + 20:
        analysis_parts.append(f"{home_team}が優勢。直近フォーム{home_form}%と好調。")
    elif away_form > home_form + 20:
        analysis_parts.append(f"{away_team}がアウェイながら有利。{away_form}%のフォームが光る。")
    else:
        analysis_parts.append(f"拮抗した一戦。両チームのフォームは{home_team}({home_form}%) vs {away_team}({away_form}%)。")
    
    if home_goals > 12 and home_conceded < 10:
        analysis_parts.append(f"{home_team}は攻守バランス良好。")
    elif away_goals > 12 and away_conceded < 10:
        analysis_parts.append(f"{away_team}は攻守に安定。")
    elif home_goals > 15:
        analysis_parts.append(f"{home_team}の攻撃力に注目。")
    elif away_goals > 15:
        analysis_parts.append(f"{away_team}の得点力が脅威。")
    
    if h2h_home > h2h_away + 1:
        analysis_parts.append(f"直接対決では{home_team}が優位。")
    elif h2h_away > h2h_home + 1:
        analysis_parts.append(f"過去の対戦では{away_team}が優勢。")
    
    return " ".join(analysis_parts) if analysis_parts else "データ不足のため詳細分析不可"

@st.cache_data(ttl=60)
@track_performance
def get_upcoming_matches():
    session = get_valid_session()
    query = """
    SELECT
        m.match_id,
        m.match_date,
        h.team_name AS home_team,
        a.team_name AS away_team,
        m.status
    FROM SOCCER_APP.MATCH_DATA.MATCHES m
    JOIN SOCCER_APP.MATCH_DATA.TEAMS h ON m.home_team_id = h.team_id
    JOIN SOCCER_APP.MATCH_DATA.TEAMS a ON m.away_team_id = a.team_id
    WHERE m.result IS NULL
    ORDER BY m.match_date
    """
    return session.sql(query).to_pandas()

@track_performance
def save_prediction(match_id: int, user_name: str, predicted_result: str) -> bool:
    try:
        session = get_valid_session()
        query = """
        MERGE INTO SOCCER_APP.MATCH_DATA.PREDICTIONS p
        USING (SELECT ? AS match_id, ? AS user_name, ? AS predicted_result) s
        ON p.match_id = s.match_id AND p.user_name = s.user_name
        WHEN MATCHED THEN
            UPDATE SET predicted_result = s.predicted_result
        WHEN NOT MATCHED THEN
            INSERT (match_id, user_name, predicted_result, created_at)
            VALUES (s.match_id, s.user_name, s.predicted_result, CURRENT_TIMESTAMP())
        """
        session.sql(query, params=[match_id, user_name, predicted_result]).collect()
        clear_prediction_cache()
        st.session_state.leaderboard_updated = False
        return True
    except Exception as e:
        st.error(f"保存エラー: {str(e)}")
        return False

@st.cache_data(ttl=60)
@track_performance
def get_user_predictions(user_name: str):
    session = get_valid_session()
    query = """
    SELECT
        m.match_date,
        h.team_name AS home_team,
        a.team_name AS away_team,
        CASE p.predicted_result
            WHEN 'H' THEN 'ホーム勝ち'
            WHEN 'D' THEN '引き分け'
            WHEN 'A' THEN 'アウェイ勝ち'
        END AS predicted_result,
        CASE m.result
            WHEN 'H' THEN 'ホーム勝ち'
            WHEN 'D' THEN '引き分け'
            WHEN 'A' THEN 'アウェイ勝ち'
            ELSE NULL
        END AS actual_result,
        CASE
            WHEN m.result IS NULL THEN '⏳ 未確定'
            WHEN p.predicted_result = m.result THEN '✅ 正解'
            ELSE '❌ 不正解'
        END AS status
    FROM SOCCER_APP.MATCH_DATA.PREDICTIONS p
    JOIN SOCCER_APP.MATCH_DATA.MATCHES m ON p.match_id = m.match_id
    JOIN SOCCER_APP.MATCH_DATA.TEAMS h ON m.home_team_id = h.team_id
    JOIN SOCCER_APP.MATCH_DATA.TEAMS a ON m.away_team_id = a.team_id
    WHERE p.user_name = ?
    ORDER BY m.match_date DESC
    """
    return session.sql(query, params=[user_name]).to_pandas()

@st.cache_data(ttl=600)
@track_performance
def get_match_preview(home_team: str, away_team: str) -> dict:
    try:
        session = get_valid_session()
        
        stats_query = "SELECT SOCCER_APP.MATCH_DATA.GET_MATCH_PREVIEW_V2(?, ?) AS preview"
        stats_result = session.sql(stats_query, params=[home_team, away_team]).collect()
        
        if stats_result and stats_result[0]['PREVIEW']:
            stats = json.loads(stats_result[0]['PREVIEW'])
        else:
            stats = {
                'home_pct': 40, 'draw_pct': 25, 'away_pct': 35,
                'home_form': 50, 'away_form': 50,
                'home_goals_for': 0, 'home_goals_against': 0,
                'away_goals_for': 0, 'away_goals_against': 0,
                'home_recent_wins': 0, 'away_recent_wins': 0,
                'h2h_home_wins': 0, 'h2h_draws': 0, 'h2h_away_wins': 0
            }
        
        ai_text = ""
        try:
            ai_query = "SELECT SOCCER_APP.MATCH_DATA.GET_AI_ANALYSIS(?, ?) AS analysis"
            ai_result = session.sql(ai_query, params=[home_team, away_team]).collect()
            if ai_result and ai_result[0]['ANALYSIS']:
                ai_text = clean_ai_output(ai_result[0]['ANALYSIS'])
        except Exception:
            pass
        
        if not ai_text or len(ai_text) < 20:
            ai_text = generate_fallback_analysis(home_team, away_team, stats)
        
        return {
            'home': int(stats.get('home_pct', 40)),
            'draw': int(stats.get('draw_pct', 25)),
            'away': int(stats.get('away_pct', 35)),
            'text': ai_text,
            'stats': stats
        }
        
    except Exception as e:
        if st.session_state.get('dev_mode', False):
            st.warning(f"Preview Error: {str(e)}")
        return {
            'home': 33, 'draw': 34, 'away': 33,
            'text': "プレビューを生成できませんでした",
            'stats': {}
        }

@st.cache_data(ttl=300)
@track_performance
def get_leaderboard():
    session = get_valid_session()
    query = """
    SELECT
        user_name,
        total_predictions,
        correct_predictions,
        accuracy_rate
    FROM SOCCER_APP.MATCH_DATA.LEADERBOARD
    ORDER BY accuracy_rate DESC, correct_predictions DESC
    LIMIT 10
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=300)
@track_performance
def get_user_rank(user_name: str):
    session = get_valid_session()
    query = """
    SELECT
        user_name,
        total_predictions,
        correct_predictions,
        accuracy_rate,
        RANK() OVER (ORDER BY accuracy_rate DESC, correct_predictions DESC) AS rank
    FROM SOCCER_APP.MATCH_DATA.LEADERBOARD
    """
    df = session.sql(query).to_pandas()
    user_row = df[df['USER_NAME'] == user_name]
    return user_row if not user_row.empty else None

@track_performance
def update_leaderboard():
    if st.session_state.leaderboard_updated:
        return
    
    session = get_valid_session()
    query = """
    MERGE INTO SOCCER_APP.MATCH_DATA.LEADERBOARD l
    USING (
        SELECT
            p.user_name,
            COUNT(*) AS total,
            SUM(CASE WHEN p.predicted_result = m.result THEN 1 ELSE 0 END) AS correct,
            ROUND(SUM(CASE WHEN p.predicted_result = m.result THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS accuracy
        FROM SOCCER_APP.MATCH_DATA.PREDICTIONS p
        JOIN SOCCER_APP.MATCH_DATA.MATCHES m ON p.match_id = m.match_id
        WHERE m.result IS NOT NULL
        GROUP BY p.user_name
    ) s
    ON l.user_name = s.user_name
    WHEN MATCHED THEN
        UPDATE SET
            total_predictions = s.total,
            correct_predictions = s.correct,
            accuracy_rate = s.accuracy,
            last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (user_name, total_predictions, correct_predictions, accuracy_rate, last_updated)
        VALUES (s.user_name, s.total, s.correct, s.accuracy, CURRENT_TIMESTAMP())
    """
    session.sql(query).collect()
    st.session_state.leaderboard_updated = True
    get_leaderboard.clear()
    get_user_rank.clear()

page = st.sidebar.selectbox("ページ選択", ["⚽ 試合予想", "📊 結果確認", "🏆 ランキング"])

if not st.session_state.user_name:
    st.sidebar.markdown("### ユーザー名を入力")
    user_input = st.sidebar.text_input("ユーザー名", key="user_input")
    if st.sidebar.button("ログイン"):
        valid, msg = validate_username(user_input)
        if valid:
            st.session_state.user_name = user_input.strip()
            st.rerun()
        else:
            st.sidebar.error(msg)
else:
    st.sidebar.success(f"ログイン中: {st.session_state.user_name}")
    if st.sidebar.button("ログアウト"):
        st.session_state.user_name = ""
        st.session_state.leaderboard_updated = False
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.checkbox("🔧 開発者モード", key="dev_mode")

if st.session_state.get('dev_mode', False):
    with st.sidebar.expander("📊 キャッシュ状態", expanded=False):
        st.write("**TTL設定**:")
        st.write("- get_upcoming_matches: 60s")
        st.write("- get_leaderboard: 300s")
        st.write("- get_match_preview: 600s")
        st.write("- get_user_predictions: 60s")
    
    with st.sidebar.expander("⏱️ パフォーマンス", expanded=False):
        if st.session_state.perf_metrics:
            recent_metrics = st.session_state.perf_metrics[-10:]
            perf_df = pd.DataFrame(recent_metrics)
            st.dataframe(perf_df, use_container_width=True, hide_index=True)
            st.metric("平均実行時間", f"{perf_df['elapsed'].mean():.3f}秒")
        else:
            st.info("パフォーマンスデータなし")

if not st.session_state.user_name:
    st.markdown("""
    <div class="page-header">
        <h1>⚽ サッカー試合予測</h1>
        <p>試合結果を予想してランキング上位を目指そう！</p>
    </div>
    """, unsafe_allow_html=True)
    st.info("👈 サイドバーからログインしてください")
    st.stop()

if page == "⚽ 試合予想":
    st.markdown("""
    <div class="page-header">
        <h1>⚽ 試合予想</h1>
        <p>今週の試合を予想しよう</p>
    </div>
    """, unsafe_allow_html=True)

    try:
        matches = get_upcoming_matches()

        if matches.empty:
            st.info("現在予想可能な試合はありません")
        else:
            for idx, match in matches.iterrows():
                home_team = html.escape(str(match['HOME_TEAM']))
                away_team = html.escape(str(match['AWAY_TEAM']))
                match_date = html.escape(str(match['MATCH_DATE']))
                status = match.get('STATUS', '')
                
                status_badge = ""
                if status == "POSTPONED":
                    status_badge = " <span style='color: red; font-size: 0.8rem;'>⚠️ 延期</span>"
                
                st.markdown(f"""
                <div class="match-card">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div class="team-name">🏠 {home_team}</div>
                        <div class="vs-badge">VS</div>
                        <div class="team-name">✈️ {away_team}</div>
                    </div>
                    <div class="match-date">📅 {match_date}{status_badge}</div>
                </div>
                """, unsafe_allow_html=True)

                col1, col2, col3 = st.columns(3)

                with col1:
                    if st.button("🏠 ホーム勝ち", key=f"H_{match['MATCH_ID']}", use_container_width=True):
                        if save_prediction(match['MATCH_ID'], st.session_state.user_name, 'H'):
                            st.success("予想を保存しました！")

                with col2:
                    if st.button("🤝 引き分け", key=f"D_{match['MATCH_ID']}", use_container_width=True):
                        if save_prediction(match['MATCH_ID'], st.session_state.user_name, 'D'):
                            st.success("予想を保存しました！")

                with col3:
                    if st.button("✈️ アウェイ勝ち", key=f"A_{match['MATCH_ID']}", use_container_width=True):
                        if save_prediction(match['MATCH_ID'], st.session_state.user_name, 'A'):
                            st.success("予想を保存しました！")

                with st.expander("🤖 AI試合プレビュー"):
                    with st.spinner("🔮 AIが試合を分析中..."):
                        preview = get_match_preview(match['HOME_TEAM'], match['AWAY_TEAM'])
                        stats = preview.get('stats', {})
                        
                        st.markdown(f"""
                        <div style="margin-bottom: 15px;">
                            <div style="display: flex; justify-content: space-between; margin-bottom: 5px; font-size: 0.9rem;">
                                <span>🏠 {home_team}</span>
                                <span>🤝 引分</span>
                                <span>✈️ {away_team}</span>
                            </div>
                            <div class="odds-bar">
                                <div class="odds-home" style="width: {preview['home']}%;">{preview['home']}%</div>
                                <div class="odds-draw" style="width: {preview['draw']}%;">{preview['draw']}%</div>
                                <div class="odds-away" style="width: {preview['away']}%;">{preview['away']}%</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if stats:
                            col_h, col_a = st.columns(2)
                            
                            home_form = stats.get('home_form', 50)
                            away_form = stats.get('away_form', 50)
                            
                            with col_h:
                                st.markdown(f"**🏠 {home_team}**")
                                form_class = get_form_class(home_form)
                                st.markdown(f"フォーム: <span class='form-indicator {form_class}'>{home_form}%</span>", unsafe_allow_html=True)
                                st.caption(f"直近: {stats.get('home_recent_wins', 0)}勝 | 得点: {stats.get('home_goals_for', 0)} | 失点: {stats.get('home_goals_against', 0)}")
                            
                            with col_a:
                                st.markdown(f"**✈️ {away_team}**")
                                form_class = get_form_class(away_form)
                                st.markdown(f"フォーム: <span class='form-indicator {form_class}'>{away_form}%</span>", unsafe_allow_html=True)
                                st.caption(f"直近: {stats.get('away_recent_wins', 0)}勝 | 得点: {stats.get('away_goals_for', 0)} | 失点: {stats.get('away_goals_against', 0)}")
                            
                            h2h_total = stats.get('h2h_home_wins', 0) + stats.get('h2h_draws', 0) + stats.get('h2h_away_wins', 0)
                            if h2h_total > 0:
                                st.markdown(f"""
                                <div class="h2h-badge">
                                    📊 直接対決: {stats.get('h2h_home_wins', 0)}勝 / {stats.get('h2h_draws', 0)}分 / {stats.get('h2h_away_wins', 0)}敗
                                </div>
                                """, unsafe_allow_html=True)
                        
                        safe_text = html.escape(preview['text'])
                        st.markdown(f"""
                        <div class="ai-preview">
                            <h4>🎯 AI分析</h4>
                            <p>{safe_text}</p>
                        </div>
                        """, unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)

    except Exception as e:
        st.error(f"データ取得エラー: {str(e)}")

elif page == "📊 結果確認":
    st.markdown("""
    <div class="page-header">
        <h1>📊 結果確認</h1>
        <p>あなたの予想結果をチェック</p>
    </div>
    """, unsafe_allow_html=True)

    try:
        predictions = get_user_predictions(st.session_state.user_name)

        if predictions.empty:
            st.info("まだ予想がありません")
        else:
            completed = predictions[predictions['ACTUAL_RESULT'].notna()]
            if not completed.empty:
                correct = len(completed[completed['STATUS'] == '✅ 正解'])
                total = len(completed)
                accuracy = (correct / total * 100) if total > 0 else 0

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"""
                    <div class="stats-card">
                        <div class="stats-value">{total}</div>
                        <div class="stats-label">結果確定済み</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
                    <div class="stats-card">
                        <div class="stats-value">{correct}</div>
                        <div class="stats-label">正解数</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="stats-card">
                        <div class="stats-value">{accuracy:.1f}%</div>
                        <div class="stats-label">正答率</div>
                    </div>
                    """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            
            st.dataframe(
                predictions[['MATCH_DATE', 'HOME_TEAM', 'AWAY_TEAM', 'PREDICTED_RESULT', 'ACTUAL_RESULT', 'STATUS']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "MATCH_DATE": "試合日",
                    "HOME_TEAM": "ホーム",
                    "AWAY_TEAM": "アウェイ",
                    "PREDICTED_RESULT": "あなたの予想",
                    "ACTUAL_RESULT": "結果",
                    "STATUS": "判定"
                }
            )

    except Exception as e:
        st.error(f"データ取得エラー: {str(e)}")

elif page == "🏆 ランキング":
    st.markdown("""
    <div class="page-header">
        <h1>🏆 ランキング</h1>
        <p>予想王は誰だ？</p>
    </div>
    """, unsafe_allow_html=True)

    try:
        update_leaderboard()
        leaderboard = get_leaderboard()

        if leaderboard.empty:
            st.info("まだランキングデータがありません")
        else:
            for idx, row in leaderboard.iterrows():
                rank = idx + 1
                if rank == 1:
                    medal = "🥇"
                    rank_class = "rank-1"
                elif rank == 2:
                    medal = "🥈"
                    rank_class = "rank-2"
                elif rank == 3:
                    medal = "🥉"
                    rank_class = "rank-3"
                else:
                    medal = f"#{rank}"
                    rank_class = "rank-other"
                
                is_current_user = row['USER_NAME'] == st.session_state.user_name
                highlight = "⭐ " if is_current_user else ""
                user_name_safe = html.escape(str(row['USER_NAME']))
                
                st.markdown(f"""
                <div class="ranking-row {rank_class}">
                    <div style="flex: 1; font-size: 1.5rem;">{medal}</div>
                    <div style="flex: 4; font-weight: bold;">{highlight}{user_name_safe}</div>
                    <div style="flex: 2;">{int(row['CORRECT_PREDICTIONS'])}勝 / {int(row['TOTAL_PREDICTIONS'])}予想</div>
                    <div style="flex: 2; text-align: right; font-weight: bold;">{row['ACCURACY_RATE']:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)

            if st.session_state.user_name:
                user_data = leaderboard[leaderboard['USER_NAME'] == st.session_state.user_name]
                if not user_data.empty:
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.markdown("### 🌟 あなたの成績")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.markdown(f"""
                        <div class="stats-card">
                            <div class="stats-value">{user_data.index[0] + 1}位</div>
                            <div class="stats-label">現在の順位</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col2:
                        st.markdown(f"""
                        <div class="stats-card">
                            <div class="stats-value">{int(user_data.iloc[0]['CORRECT_PREDICTIONS'])}</div>
                            <div class="stats-label">正解数</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col3:
                        st.markdown(f"""
                        <div class="stats-card">
                            <div class="stats-value">{user_data.iloc[0]['ACCURACY_RATE']:.1f}%</div>
                            <div class="stats-label">正答率</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    user_rank_data = get_user_rank(st.session_state.user_name)
                    if user_rank_data is not None and not user_rank_data.empty:
                        st.markdown("<br>", unsafe_allow_html=True)
                        st.info("あなたはTOP10圏外です")
                        st.markdown("### 🌟 あなたの成績")
                        
                        row = user_rank_data.iloc[0]
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.markdown(f"""
                            <div class="stats-card">
                                <div class="stats-value">{int(row['RANK'])}位</div>
                                <div class="stats-label">現在の順位</div>
                            </div>
                            """, unsafe_allow_html=True)
                        with col2:
                            st.markdown(f"""
                            <div class="stats-card">
                                <div class="stats-value">{int(row['CORRECT_PREDICTIONS'])}</div>
                                <div class="stats-label">正解数</div>
                            </div>
                            """, unsafe_allow_html=True)
                        with col3:
                            st.markdown(f"""
                            <div class="stats-card">
                                <div class="stats-value">{row['ACCURACY_RATE']:.1f}%</div>
                                <div class="stats-label">正答率</div>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.markdown("<br>", unsafe_allow_html=True)
                        st.info("まだランキングに登録されていません。試合結果が確定すると反映されます。")

    except Exception as e:
        st.error(f"データ取得エラー: {str(e)}")