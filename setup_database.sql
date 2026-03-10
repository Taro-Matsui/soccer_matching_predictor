-- =============================================
-- サッカー試合予測アプリ - データベースセットアップSQL
-- 作成日: 2026-03-10
-- バージョン: 3.0.0
-- =============================================

-- =============================================
-- 1. データベース・スキーマ作成
-- =============================================

CREATE DATABASE IF NOT EXISTS SOCCER_APP;
CREATE SCHEMA IF NOT EXISTS SOCCER_APP.MATCH_DATA;

USE DATABASE SOCCER_APP;
USE SCHEMA MATCH_DATA;

-- =============================================
-- 2. テーブル作成
-- =============================================

-- チームマスタ
CREATE TABLE IF NOT EXISTS SOCCER_APP.MATCH_DATA.TEAMS (
    team_id         INTEGER     PRIMARY KEY,
    team_name       VARCHAR(100) NOT NULL UNIQUE,
    short_name      VARCHAR(20),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 試合データ
CREATE TABLE IF NOT EXISTS SOCCER_APP.MATCH_DATA.MATCHES (
    match_id        INTEGER     PRIMARY KEY,
    home_team_id    INTEGER     NOT NULL REFERENCES SOCCER_APP.MATCH_DATA.TEAMS(team_id),
    away_team_id    INTEGER     NOT NULL REFERENCES SOCCER_APP.MATCH_DATA.TEAMS(team_id),
    match_date      DATE        NOT NULL,
    match_datetime  TIMESTAMP_NTZ,
    home_score      INTEGER,
    away_score      INTEGER,
    result          VARCHAR(1)  CHECK (result IN ('H', 'D', 'A')),
    status          VARCHAR(20) DEFAULT 'SCHEDULED',
    matchday        INTEGER,
    season          VARCHAR(10),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ユーザー予想（Hybrid Table推奨だが通常テーブルでも可）
CREATE TABLE IF NOT EXISTS SOCCER_APP.MATCH_DATA.PREDICTIONS (
    prediction_id    INTEGER     AUTOINCREMENT PRIMARY KEY,
    match_id         INTEGER     NOT NULL REFERENCES SOCCER_APP.MATCH_DATA.MATCHES(match_id),
    user_name        VARCHAR(50) NOT NULL,
    predicted_result VARCHAR(1)  NOT NULL CHECK (predicted_result IN ('H', 'D', 'A')),
    is_correct       BOOLEAN,
    created_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (match_id, user_name)
);

-- リーダーボード
CREATE TABLE IF NOT EXISTS SOCCER_APP.MATCH_DATA.LEADERBOARD (
    user_name           VARCHAR(50) PRIMARY KEY,
    total_predictions   INTEGER     DEFAULT 0,
    correct_predictions INTEGER     DEFAULT 0,
    accuracy_rate       DECIMAL(5,2) DEFAULT 0,
    last_updated        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================
-- 3. ビュー作成
-- =============================================

-- 3.1 強化版チームフォーム（直近20試合・重み付け）
CREATE OR REPLACE VIEW SOCCER_APP.MATCH_DATA.TEAM_FORM_ENHANCED AS
WITH recent_matches AS (
    SELECT 
        m.match_id,
        m.match_date,
        m.home_team_id,
        m.away_team_id,
        m.home_score,
        m.away_score,
        m.result,
        ROW_NUMBER() OVER (PARTITION BY m.home_team_id ORDER BY m.match_date DESC) as home_rank,
        ROW_NUMBER() OVER (PARTITION BY m.away_team_id ORDER BY m.match_date DESC) as away_rank
    FROM SOCCER_APP.MATCH_DATA.MATCHES m
    WHERE m.result IS NOT NULL
),
home_form AS (
    SELECT 
        home_team_id as team_id,
        SUM(CASE WHEN result = 'H' THEN (11 - home_rank) * 3 
                 WHEN result = 'D' THEN (11 - home_rank) * 1 
                 ELSE 0 END) as weighted_points,
        SUM(CASE WHEN result = 'H' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN result = 'A' THEN 1 ELSE 0 END) as losses,
        SUM(home_score) as goals_for,
        SUM(away_score) as goals_against,
        COUNT(*) as matches
    FROM recent_matches
    WHERE home_rank <= 10
    GROUP BY home_team_id
),
away_form AS (
    SELECT 
        away_team_id as team_id,
        SUM(CASE WHEN result = 'A' THEN (11 - away_rank) * 3 
                 WHEN result = 'D' THEN (11 - away_rank) * 1 
                 ELSE 0 END) as weighted_points,
        SUM(CASE WHEN result = 'A' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN result = 'H' THEN 1 ELSE 0 END) as losses,
        SUM(away_score) as goals_for,
        SUM(home_score) as goals_against,
        COUNT(*) as matches
    FROM recent_matches
    WHERE away_rank <= 10
    GROUP BY away_team_id
),
combined AS (
    SELECT 
        COALESCE(h.team_id, a.team_id) as team_id,
        COALESCE(h.weighted_points, 0) + COALESCE(a.weighted_points, 0) as total_weighted_points,
        COALESCE(h.wins, 0) + COALESCE(a.wins, 0) as wins_last_20,
        COALESCE(h.draws, 0) + COALESCE(a.draws, 0) as draws_last_20,
        COALESCE(h.losses, 0) + COALESCE(a.losses, 0) as losses_last_20,
        COALESCE(h.goals_for, 0) + COALESCE(a.goals_for, 0) as goals_for_last_20,
        COALESCE(h.goals_against, 0) + COALESCE(a.goals_against, 0) as goals_against_last_20,
        COALESCE(h.matches, 0) + COALESCE(a.matches, 0) as matches_last_20,
        COALESCE(h.weighted_points, 0) as home_weighted_points,
        COALESCE(a.weighted_points, 0) as away_weighted_points
    FROM home_form h
    FULL OUTER JOIN away_form a ON h.team_id = a.team_id
)
SELECT 
    t.team_name,
    c.team_id,
    c.matches_last_20,
    c.wins_last_20,
    c.draws_last_20,
    c.losses_last_20,
    c.goals_for_last_20,
    c.goals_against_last_20,
    c.total_weighted_points,
    ROUND(c.total_weighted_points / NULLIF(c.matches_last_20, 0) / 3.0 * 100, 1) as form_rating,
    ROUND(c.home_weighted_points / 10.0 / 3.0 * 100, 1) as home_form_rating,
    ROUND(c.away_weighted_points / 10.0 / 3.0 * 100, 1) as away_form_rating,
    ROUND(c.goals_for_last_20 * 1.0 / NULLIF(c.matches_last_20, 0), 2) as goals_per_match,
    ROUND(c.goals_against_last_20 * 1.0 / NULLIF(c.matches_last_20, 0), 2) as conceded_per_match
FROM combined c
JOIN SOCCER_APP.MATCH_DATA.TEAMS t ON c.team_id = t.team_id;

-- 3.2 シーズン通算成績
CREATE OR REPLACE VIEW SOCCER_APP.MATCH_DATA.TEAM_SEASON_STATS AS
WITH all_matches AS (
    SELECT 
        home_team_id as team_id,
        match_date,
        home_score as goals_for,
        away_score as goals_against,
        CASE WHEN result = 'H' THEN 3 WHEN result = 'D' THEN 1 ELSE 0 END as points,
        CASE WHEN result = 'H' THEN 1 ELSE 0 END as win,
        CASE WHEN result = 'D' THEN 1 ELSE 0 END as draw,
        CASE WHEN result = 'A' THEN 1 ELSE 0 END as loss,
        1 as is_home,
        result
    FROM SOCCER_APP.MATCH_DATA.MATCHES
    WHERE result IS NOT NULL
    UNION ALL
    SELECT 
        away_team_id as team_id,
        match_date,
        away_score as goals_for,
        home_score as goals_against,
        CASE WHEN result = 'A' THEN 3 WHEN result = 'D' THEN 1 ELSE 0 END as points,
        CASE WHEN result = 'A' THEN 1 ELSE 0 END as win,
        CASE WHEN result = 'D' THEN 1 ELSE 0 END as draw,
        CASE WHEN result = 'H' THEN 1 ELSE 0 END as loss,
        0 as is_home,
        result
    FROM SOCCER_APP.MATCH_DATA.MATCHES
    WHERE result IS NOT NULL
)
SELECT 
    t.team_name,
    a.team_id,
    COUNT(*) as total_matches,
    SUM(a.win) as total_wins,
    SUM(a.draw) as total_draws,
    SUM(a.loss) as total_losses,
    SUM(a.points) as total_points,
    SUM(a.goals_for) as total_goals_for,
    SUM(a.goals_against) as total_goals_against,
    SUM(a.goals_for) - SUM(a.goals_against) as goal_difference,
    ROUND(AVG(a.goals_for), 2) as avg_goals_per_match,
    ROUND(AVG(a.goals_against), 2) as avg_conceded_per_match,
    ROUND(SUM(a.points) * 1.0 / COUNT(*), 2) as points_per_match,
    SUM(CASE WHEN a.is_home = 1 THEN a.win ELSE 0 END) as home_wins,
    SUM(CASE WHEN a.is_home = 0 THEN a.win ELSE 0 END) as away_wins,
    SUM(CASE WHEN a.is_home = 1 THEN a.points ELSE 0 END) as home_points,
    SUM(CASE WHEN a.is_home = 0 THEN a.points ELSE 0 END) as away_points,
    ROUND(SUM(CASE WHEN a.is_home = 1 THEN a.goals_for ELSE 0 END) * 1.0 / 
          NULLIF(SUM(CASE WHEN a.is_home = 1 THEN 1 ELSE 0 END), 0), 2) as home_goals_avg,
    ROUND(SUM(CASE WHEN a.is_home = 0 THEN a.goals_for ELSE 0 END) * 1.0 / 
          NULLIF(SUM(CASE WHEN a.is_home = 0 THEN 1 ELSE 0 END), 0), 2) as away_goals_avg
FROM all_matches a
JOIN SOCCER_APP.MATCH_DATA.TEAMS t ON a.team_id = t.team_id
GROUP BY t.team_name, a.team_id;

-- 3.3 直接対決データ（重み付き）
CREATE OR REPLACE VIEW SOCCER_APP.MATCH_DATA.HEAD_TO_HEAD_ENHANCED AS
WITH h2h_matches AS (
    SELECT 
        m.home_team_id,
        m.away_team_id,
        m.match_date,
        m.home_score,
        m.away_score,
        m.result,
        h.team_name as home_team,
        a.team_name as away_team,
        ROW_NUMBER() OVER (
            PARTITION BY 
                LEAST(m.home_team_id, m.away_team_id), 
                GREATEST(m.home_team_id, m.away_team_id) 
            ORDER BY m.match_date DESC
        ) as recency_rank
    FROM SOCCER_APP.MATCH_DATA.MATCHES m
    JOIN SOCCER_APP.MATCH_DATA.TEAMS h ON m.home_team_id = h.team_id
    JOIN SOCCER_APP.MATCH_DATA.TEAMS a ON m.away_team_id = a.team_id
    WHERE m.result IS NOT NULL
)
SELECT 
    home_team,
    away_team,
    home_team_id,
    away_team_id,
    COUNT(*) as total_meetings,
    SUM(CASE WHEN result = 'H' THEN 1 ELSE 0 END) as home_wins,
    SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
    SUM(CASE WHEN result = 'A' THEN 1 ELSE 0 END) as away_wins,
    ROUND(AVG(home_score), 2) as avg_home_goals,
    ROUND(AVG(away_score), 2) as avg_away_goals,
    SUM(home_score + away_score) as total_goals,
    ROUND(AVG(home_score + away_score), 2) as avg_total_goals,
    SUM(CASE WHEN recency_rank <= 5 AND result = 'H' THEN 2 ELSE 0 END) +
    SUM(CASE WHEN recency_rank > 5 AND result = 'H' THEN 1 ELSE 0 END) as home_weighted_score,
    SUM(CASE WHEN recency_rank <= 5 AND result = 'A' THEN 2 ELSE 0 END) +
    SUM(CASE WHEN recency_rank > 5 AND result = 'A' THEN 1 ELSE 0 END) as away_weighted_score,
    MAX(match_date) as last_meeting
FROM h2h_matches
GROUP BY home_team, away_team, home_team_id, away_team_id;

-- =============================================
-- 4. 予測関数作成
-- =============================================

-- 4.1 V3予測モデル（メイン）
CREATE OR REPLACE FUNCTION SOCCER_APP.MATCH_DATA.GET_MATCH_PREDICTION_V3(
    HOME_TEAM_NAME VARCHAR,
    AWAY_TEAM_NAME VARCHAR
)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
WITH home_stats AS (
    SELECT * FROM SOCCER_APP.MATCH_DATA.TEAM_SEASON_STATS WHERE team_name = HOME_TEAM_NAME
),
away_stats AS (
    SELECT * FROM SOCCER_APP.MATCH_DATA.TEAM_SEASON_STATS WHERE team_name = AWAY_TEAM_NAME
),
home_form AS (
    SELECT * FROM SOCCER_APP.MATCH_DATA.TEAM_FORM_ENHANCED WHERE team_name = HOME_TEAM_NAME
),
away_form AS (
    SELECT * FROM SOCCER_APP.MATCH_DATA.TEAM_FORM_ENHANCED WHERE team_name = AWAY_TEAM_NAME
),
h2h AS (
    SELECT * FROM SOCCER_APP.MATCH_DATA.HEAD_TO_HEAD_ENHANCED 
    WHERE (home_team = HOME_TEAM_NAME AND away_team = AWAY_TEAM_NAME)
),
h2h_reverse AS (
    SELECT * FROM SOCCER_APP.MATCH_DATA.HEAD_TO_HEAD_ENHANCED 
    WHERE (home_team = AWAY_TEAM_NAME AND away_team = HOME_TEAM_NAME)
),
calculations AS (
    SELECT
        COALESCE(hs.points_per_match, 1.0) as home_ppg,
        COALESCE(as2.points_per_match, 1.0) as away_ppg,
        COALESCE(hs.home_goals_avg, 1.0) as home_scoring_home,
        COALESCE(as2.away_goals_avg, 1.0) as away_scoring_away,
        LEAST(100, COALESCE(hf.form_rating, 50)) as home_form_rating,
        LEAST(100, COALESCE(af.form_rating, 50)) as away_form_rating,
        LEAST(100, COALESCE(hf.home_form_rating, 50)) as home_home_form,
        LEAST(100, COALESCE(af.away_form_rating, 50)) as away_away_form,
        COALESCE(hf.goals_per_match, 1.0) as home_recent_gpg,
        COALESCE(af.goals_per_match, 1.0) as away_recent_gpg,
        COALESCE(h2h.home_wins, 0) + COALESCE(h2h_r.away_wins, 0) as h2h_team1_wins,
        COALESCE(h2h.draws, 0) + COALESCE(h2h_r.draws, 0) as h2h_draws,
        COALESCE(h2h.away_wins, 0) + COALESCE(h2h_r.home_wins, 0) as h2h_team2_wins,
        COALESCE(h2h.total_meetings, 0) + COALESCE(h2h_r.total_meetings, 0) as h2h_total,
        COALESCE(hf.wins_last_20, 0) as home_recent_wins,
        COALESCE(hf.goals_for_last_20, 0) as home_goals_for,
        COALESCE(hf.goals_against_last_20, 0) as home_goals_against,
        COALESCE(af.wins_last_20, 0) as away_recent_wins,
        COALESCE(af.goals_for_last_20, 0) as away_goals_for,
        COALESCE(af.goals_against_last_20, 0) as away_goals_against,
        COALESCE(hs.goal_difference, 0) as home_gd,
        COALESCE(as2.goal_difference, 0) as away_gd
    FROM home_stats hs
    CROSS JOIN away_stats as2
    LEFT JOIN home_form hf ON 1=1
    LEFT JOIN away_form af ON 1=1
    LEFT JOIN h2h ON 1=1
    LEFT JOIN h2h_reverse h2h_r ON 1=1
),
raw_scores AS (
    SELECT 
        *,
        (
            home_home_form * 0.25 +
            (home_ppg / 3.0 * 100) * 0.20 +
            (CASE WHEN h2h_total > 0 THEN (h2h_team1_wins * 1.0 / h2h_total * 100) ELSE 50 END) * 0.15 +
            LEAST(100, GREATEST(0, (home_gd + 50))) * 0.15 +
            60 * 0.25
        ) as home_strength,
        (
            away_away_form * 0.25 +
            (away_ppg / 3.0 * 100) * 0.20 +
            (CASE WHEN h2h_total > 0 THEN (h2h_team2_wins * 1.0 / h2h_total * 100) ELSE 40 END) * 0.15 +
            LEAST(100, GREATEST(0, (away_gd + 50))) * 0.15 +
            40 * 0.25
        ) as away_strength
    FROM calculations
),
normalized AS (
    SELECT 
        *,
        home_strength / (home_strength + away_strength + 25) * 100 as norm_home,
        away_strength / (home_strength + away_strength + 25) * 100 as norm_away
    FROM raw_scores
),
final_prob AS (
    SELECT 
        *,
        ROUND(GREATEST(20, LEAST(65, norm_home)), 0) as home_pct,
        ROUND(GREATEST(15, LEAST(50, norm_away)), 0) as away_pct
    FROM normalized
)
SELECT OBJECT_CONSTRUCT(
    'home_pct', home_pct,
    'draw_pct', 100 - home_pct - away_pct,
    'away_pct', away_pct,
    'home_form', ROUND(home_form_rating, 0),
    'away_form', ROUND(away_form_rating, 0),
    'home_home_form', ROUND(home_home_form, 0),
    'away_away_form', ROUND(away_away_form, 0),
    'home_recent_wins', home_recent_wins,
    'away_recent_wins', away_recent_wins,
    'home_goals_for', home_goals_for,
    'home_goals_against', home_goals_against,
    'away_goals_for', away_goals_for,
    'away_goals_against', away_goals_against,
    'h2h_home_wins', h2h_team1_wins,
    'h2h_draws', h2h_draws,
    'h2h_away_wins', h2h_team2_wins,
    'h2h_total', h2h_total,
    'home_ppg', ROUND(home_ppg, 2),
    'away_ppg', ROUND(away_ppg, 2),
    'home_gd', home_gd,
    'away_gd', away_gd,
    'expected_goals', ROUND((home_recent_gpg + away_recent_gpg), 1),
    'confidence', CASE 
        WHEN ABS(home_pct - away_pct) > 25 THEN '高'
        WHEN ABS(home_pct - away_pct) > 12 THEN '中'
        ELSE '低'
    END
) FROM final_prob
$$;

-- 4.2 AI分析テキスト生成関数（Cortex AI使用）
CREATE OR REPLACE FUNCTION SOCCER_APP.MATCH_DATA.GET_AI_ANALYSIS(
    HOME_TEAM_NAME VARCHAR,
    AWAY_TEAM_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    CONCAT(
        'あなたはプレミアリーグ専門のサッカーアナリストです。以下のデータに基づいて試合プレビューを日本語で作成してください。',
        '\n\n【対戦カード】',
        '\nホーム: ', HOME_TEAM_NAME,
        '\nアウェイ: ', AWAY_TEAM_NAME,
        '\n\n【統計データ】',
        '\n', (
            SELECT CONCAT(
                'ホームチーム直近フォーム: ', COALESCE(TO_VARCHAR(preview:home_form), 'N/A'), '% ',
                '(直近勝利: ', COALESCE(TO_VARCHAR(preview:home_recent_wins), '0'), ')',
                '\n得点: ', COALESCE(TO_VARCHAR(preview:home_goals_for), '0'), ' / 失点: ', COALESCE(TO_VARCHAR(preview:home_goals_against), '0'),
                '\n\nアウェイチーム直近フォーム: ', COALESCE(TO_VARCHAR(preview:away_form), 'N/A'), '% ',
                '(直近勝利: ', COALESCE(TO_VARCHAR(preview:away_recent_wins), '0'), ')',
                '\n得点: ', COALESCE(TO_VARCHAR(preview:away_goals_for), '0'), ' / 失点: ', COALESCE(TO_VARCHAR(preview:away_goals_against), '0'),
                '\n\n直接対決: ホーム', COALESCE(TO_VARCHAR(preview:h2h_home_wins), '0'), '勝 / ',
                '引分', COALESCE(TO_VARCHAR(preview:h2h_draws), '0'), ' / ',
                'アウェイ', COALESCE(TO_VARCHAR(preview:h2h_away_wins), '0'), '勝',
                '\n\n【予測確率】',
                '\nホーム勝利: ', COALESCE(TO_VARCHAR(preview:home_pct), '40'), '%',
                ' / 引分: ', COALESCE(TO_VARCHAR(preview:draw_pct), '25'), '%',
                ' / アウェイ勝利: ', COALESCE(TO_VARCHAR(preview:away_pct), '35'), '%'
            )
            FROM (SELECT SOCCER_APP.MATCH_DATA.GET_MATCH_PREDICTION_V3(HOME_TEAM_NAME, AWAY_TEAM_NAME) as preview)
        ),
        '\n\n【出力形式】',
        '\n1. 試合展望（2-3文）',
        '\n2. 注目ポイント（1-2点）',
        '\n3. 予想スコア',
        '\n4. 信頼度（高/中/低）',
        '\n\n簡潔に200文字以内で回答してください。'
    )
)
$$;

-- =============================================
-- 5. インデックス作成（パフォーマンス向上）
-- =============================================

-- 試合日付でのインデックス
CREATE OR REPLACE INDEX IF NOT EXISTS idx_matches_date 
ON SOCCER_APP.MATCH_DATA.MATCHES (match_date);

-- 予想のユーザー名でのインデックス
CREATE OR REPLACE INDEX IF NOT EXISTS idx_predictions_user 
ON SOCCER_APP.MATCH_DATA.PREDICTIONS (user_name);

-- =============================================
-- 6. 確認クエリ
-- =============================================

-- テーブル確認
SELECT 'TEAMS' as table_name, COUNT(*) as row_count FROM SOCCER_APP.MATCH_DATA.TEAMS
UNION ALL
SELECT 'MATCHES', COUNT(*) FROM SOCCER_APP.MATCH_DATA.MATCHES
UNION ALL
SELECT 'PREDICTIONS', COUNT(*) FROM SOCCER_APP.MATCH_DATA.PREDICTIONS
UNION ALL
SELECT 'LEADERBOARD', COUNT(*) FROM SOCCER_APP.MATCH_DATA.LEADERBOARD;

-- ビュー確認
SELECT 
    table_catalog || '.' || table_schema || '.' || table_name as view_name
FROM SOCCER_APP.INFORMATION_SCHEMA.VIEWS
WHERE table_schema = 'MATCH_DATA';

-- 関数確認
SHOW USER FUNCTIONS IN SCHEMA SOCCER_APP.MATCH_DATA;

-- =============================================
-- END OF SETUP
-- =============================================
