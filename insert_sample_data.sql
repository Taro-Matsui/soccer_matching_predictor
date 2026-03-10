-- =============================================
-- サッカー試合予測アプリ - サンプルデータ投入SQL
-- 作成日: 2026-03-10
-- 説明: プレミアリーグのチーム・試合データサンプル
-- =============================================

USE DATABASE SOCCER_APP;
USE SCHEMA MATCH_DATA;

-- =============================================
-- 1. チームデータ
-- =============================================

MERGE INTO SOCCER_APP.MATCH_DATA.TEAMS t
USING (
    SELECT * FROM VALUES
    (57, 'Arsenal FC', 'ARS'),
    (61, 'Chelsea FC', 'CHE'),
    (64, 'Liverpool FC', 'LIV'),
    (65, 'Manchester City FC', 'MCI'),
    (66, 'Manchester United FC', 'MUN'),
    (73, 'Tottenham Hotspur FC', 'TOT'),
    (67, 'Newcastle United FC', 'NEW'),
    (63, 'Fulham FC', 'FUL'),
    (402, 'Brentford FC', 'BRE'),
    (397, 'Brighton & Hove Albion FC', 'BHA'),
    (328, 'Burnley FC', 'BUR'),
    (354, 'Crystal Palace FC', 'CRY'),
    (62, 'Everton FC', 'EVE'),
    (341, 'Leeds United FC', 'LEE'),
    (351, 'Nottingham Forest FC', 'NFO'),
    (563, 'West Ham United FC', 'WHU'),
    (76, 'Wolverhampton Wanderers FC', 'WOL'),
    (1044, 'AFC Bournemouth', 'BOU'),
    (58, 'Aston Villa FC', 'AVL'),
    (1076, 'Sunderland AFC', 'SUN')
) AS s(team_id, team_name, short_name)
ON t.team_id = s.team_id
WHEN NOT MATCHED THEN 
    INSERT (team_id, team_name, short_name) 
    VALUES (s.team_id, s.team_name, s.short_name);

-- =============================================
-- 2. 試合データサンプル（2025-26シーズン 今後の試合）
-- =============================================

MERGE INTO SOCCER_APP.MATCH_DATA.MATCHES m
USING (
    SELECT * FROM VALUES
    -- 2026年3月14日
    (500001, 328, 1044, '2026-03-14', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),  -- Burnley vs Bournemouth
    (500002, 57, 62, '2026-03-14', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),     -- Arsenal vs Everton
    (500003, 1076, 397, '2026-03-14', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),  -- Sunderland vs Brighton
    (500004, 563, 65, '2026-03-14', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),    -- West Ham vs Man City
    (500005, 61, 67, '2026-03-14', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),     -- Chelsea vs Newcastle
    
    -- 2026年3月15日
    (500006, 64, 73, '2026-03-15', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),     -- Liverpool vs Tottenham
    (500007, 354, 341, '2026-03-15', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),   -- Crystal Palace vs Leeds
    (500008, 66, 58, '2026-03-15', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),     -- Man United vs Aston Villa
    (500009, 351, 63, '2026-03-15', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),    -- Nottingham vs Fulham
    
    -- 2026年3月16日
    (500010, 402, 76, '2026-03-16', NULL, NULL, NULL, 'TIMED', 28, '2025-26'),    -- Brentford vs Wolves
    
    -- 2026年3月20日
    (500011, 1044, 66, '2026-03-20', NULL, NULL, NULL, 'TIMED', 29, '2025-26'),   -- Bournemouth vs Man United
    
    -- 2026年3月21日
    (500012, 63, 328, '2026-03-21', NULL, NULL, NULL, 'TIMED', 29, '2025-26'),    -- Fulham vs Burnley
    (500013, 397, 64, '2026-03-21', NULL, NULL, NULL, 'TIMED', 29, '2025-26'),    -- Brighton vs Liverpool
    (500014, 65, 354, '2026-03-21', NULL, NULL, NULL, 'POSTPONED', 29, '2025-26'), -- Man City vs Crystal Palace
    (500015, 341, 402, '2026-03-21', NULL, NULL, NULL, 'TIMED', 29, '2025-26'),   -- Leeds vs Brentford
    (500016, 62, 61, '2026-03-21', NULL, NULL, NULL, 'TIMED', 29, '2025-26'),     -- Everton vs Chelsea
    
    -- 2026年3月22日
    (500017, 67, 1076, '2026-03-22', NULL, NULL, NULL, 'TIMED', 29, '2025-26'),   -- Newcastle vs Sunderland
    (500018, 73, 351, '2026-03-22', NULL, NULL, NULL, 'TIMED', 29, '2025-26'),    -- Tottenham vs Nottingham
    (500019, 58, 563, '2026-03-22', NULL, NULL, NULL, 'TIMED', 29, '2025-26')     -- Aston Villa vs West Ham
    
) AS s(match_id, home_team_id, away_team_id, match_date, home_score, away_score, result, status, matchday, season)
ON m.match_id = s.match_id
WHEN MATCHED AND m.result IS NULL AND s.result IS NOT NULL THEN
    UPDATE SET 
        home_score = s.home_score,
        away_score = s.away_score,
        result = s.result,
        status = s.status
WHEN NOT MATCHED THEN
    INSERT (match_id, home_team_id, away_team_id, match_date, home_score, away_score, result, status, matchday, season)
    VALUES (s.match_id, s.home_team_id, s.away_team_id, s.match_date, s.home_score, s.away_score, s.result, s.status, s.matchday, s.season);

-- =============================================
-- 3. 過去の試合データサンプル（予測モデル訓練用）
-- =============================================

-- 過去の試合結果を追加（直近の試合結果）
MERGE INTO SOCCER_APP.MATCH_DATA.MATCHES m
USING (
    SELECT * FROM VALUES
    -- 2026年3月上旬の試合結果例
    (400001, 64, 57, '2026-03-01', 2, 1, 'H', 'FINISHED', 27, '2025-26'),   -- Liverpool 2-1 Arsenal
    (400002, 65, 61, '2026-03-01', 3, 0, 'H', 'FINISHED', 27, '2025-26'),   -- Man City 3-0 Chelsea
    (400003, 73, 66, '2026-03-02', 1, 1, 'D', 'FINISHED', 27, '2025-26'),   -- Tottenham 1-1 Man United
    (400004, 67, 58, '2026-03-02', 2, 0, 'H', 'FINISHED', 27, '2025-26'),   -- Newcastle 2-0 Aston Villa
    (400005, 397, 563, '2026-03-03', 1, 2, 'A', 'FINISHED', 27, '2025-26'), -- Brighton 1-2 West Ham
    
    -- 2026年2月の試合結果例
    (400006, 57, 65, '2026-02-22', 1, 1, 'D', 'FINISHED', 26, '2025-26'),   -- Arsenal 1-1 Man City
    (400007, 61, 64, '2026-02-22', 0, 2, 'A', 'FINISHED', 26, '2025-26'),   -- Chelsea 0-2 Liverpool
    (400008, 66, 67, '2026-02-23', 2, 1, 'H', 'FINISHED', 26, '2025-26'),   -- Man United 2-1 Newcastle
    (400009, 58, 73, '2026-02-23', 3, 2, 'H', 'FINISHED', 26, '2025-26'),   -- Aston Villa 3-2 Tottenham
    (400010, 563, 397, '2026-02-24', 1, 0, 'H', 'FINISHED', 26, '2025-26')  -- West Ham 1-0 Brighton
    
) AS s(match_id, home_team_id, away_team_id, match_date, home_score, away_score, result, status, matchday, season)
ON m.match_id = s.match_id
WHEN NOT MATCHED THEN
    INSERT (match_id, home_team_id, away_team_id, match_date, home_score, away_score, result, status, matchday, season)
    VALUES (s.match_id, s.home_team_id, s.away_team_id, s.match_date, s.home_score, s.away_score, s.result, s.status, s.matchday, s.season);

-- =============================================
-- 4. 確認クエリ
-- =============================================

-- 投入データ確認
SELECT 'チーム数' as item, COUNT(*) as count FROM SOCCER_APP.MATCH_DATA.TEAMS
UNION ALL
SELECT '総試合数', COUNT(*) FROM SOCCER_APP.MATCH_DATA.MATCHES
UNION ALL
SELECT '完了試合', COUNT(*) FROM SOCCER_APP.MATCH_DATA.MATCHES WHERE result IS NOT NULL
UNION ALL
SELECT '未実施試合', COUNT(*) FROM SOCCER_APP.MATCH_DATA.MATCHES WHERE result IS NULL;

-- 今後の試合一覧
SELECT 
    m.match_date,
    h.team_name as home_team,
    a.team_name as away_team,
    m.status
FROM SOCCER_APP.MATCH_DATA.MATCHES m
JOIN SOCCER_APP.MATCH_DATA.TEAMS h ON m.home_team_id = h.team_id
JOIN SOCCER_APP.MATCH_DATA.TEAMS a ON m.away_team_id = a.team_id
WHERE m.result IS NULL
ORDER BY m.match_date;

-- =============================================
-- END OF SAMPLE DATA
-- =============================================
