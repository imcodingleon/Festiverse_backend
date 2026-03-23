-- ================================================================
-- Festiverse 대시보드 SQL Views (MySQL 8.0)
-- 입력 테이블: event_logs
-- JSON 접근: event_data->>'$.필드명' (MySQL 8.0 단축 연산자)
-- ================================================================

-- ----------------------------------------------------------------
-- P1 — 탐색 퍼널 지표 (15종)
-- ----------------------------------------------------------------

-- v_p1_pv: 탐색 진입 수 (PV)
CREATE OR REPLACE VIEW v_p1_pv AS
SELECT
    DATE(created_at) AS report_date,
    COUNT(DISTINCT session_id) AS pv
FROM event_logs
WHERE event_type = 'search_page_entered'
GROUP BY DATE(created_at);

-- v_p1_fsr: 필터 선택률
CREATE OR REPLACE VIEW v_p1_fsr AS
SELECT
    DATE(s.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT f.session_id) / NULLIF(COUNT(DISTINCT s.session_id), 0),
        4
    ) AS fsr
FROM event_logs s
LEFT JOIN event_logs f
  ON f.session_id = s.session_id
 AND f.event_type = 'filter_option_toggled'
WHERE s.event_type = 'search_page_entered'
GROUP BY DATE(s.created_at);

-- v_p1_far: 필터 적용률
CREATE OR REPLACE VIEW v_p1_far AS
SELECT
    DATE(s.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT f.session_id) / NULLIF(COUNT(DISTINCT s.session_id), 0),
        4
    ) AS far
FROM event_logs s
LEFT JOIN event_logs f
  ON f.session_id = s.session_id
 AND f.event_type = 'filter_apply_button_clicked'
WHERE s.event_type = 'search_page_entered'
GROUP BY DATE(s.created_at);

-- v_p1_dcr: P1 전환율 (탐색→상세)
CREATE OR REPLACE VIEW v_p1_dcr AS
SELECT
    DATE(s.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT c.session_id) / NULLIF(COUNT(DISTINCT s.session_id), 0),
        4
    ) AS dcr
FROM event_logs s
LEFT JOIN event_logs c
  ON c.session_id = s.session_id
 AND c.event_type = 'festival_item_clicked'
WHERE s.event_type = 'search_page_entered'
GROUP BY DATE(s.created_at);

-- v_p1_tft: 첫 필터 선택 소요시간
CREATE OR REPLACE VIEW v_p1_tft AS
SELECT
    report_date,
    ROUND(AVG(first_tft_ms), 0) AS avg_tft_ms
FROM (
    SELECT
        DATE(created_at) AS report_date,
        session_id,
        MIN(CAST(event_data->>'$.time_since_page_entered_ms' AS UNSIGNED)) AS first_tft_ms
    FROM event_logs
    WHERE event_type = 'filter_option_toggled'
    GROUP BY DATE(created_at), session_id
) sub
GROUP BY report_date;

-- v_p1_tfa: 첫 필터 적용 소요시간
CREATE OR REPLACE VIEW v_p1_tfa AS
SELECT
    report_date,
    ROUND(AVG(first_tfa_ms), 0) AS avg_tfa_ms
FROM (
    SELECT
        DATE(created_at) AS report_date,
        session_id,
        MIN(CAST(event_data->>'$.time_since_page_entered_ms' AS UNSIGNED)) AS first_tfa_ms
    FROM event_logs
    WHERE event_type = 'filter_apply_button_clicked'
    GROUP BY DATE(created_at), session_id
) sub
GROUP BY report_date;

-- v_p1_ttd: 상세 도달 소요시간
CREATE OR REPLACE VIEW v_p1_ttd AS
SELECT
    report_date,
    ROUND(AVG(first_ttd_ms), 0) AS avg_ttd_ms
FROM (
    SELECT
        DATE(created_at) AS report_date,
        session_id,
        MIN(CAST(event_data->>'$.time_since_page_entered_ms' AS UNSIGNED)) AS first_ttd_ms
    FROM event_logs
    WHERE event_type = 'festival_item_clicked'
    GROUP BY DATE(created_at), session_id
) sub
GROUP BY report_date;

-- v_p1_time_on_page: 탐색 페이지 체류시간
-- RER 시 동일 세션에 search_page_exited 다중 발생 가능 → SUM 후 AVG
CREATE OR REPLACE VIEW v_p1_time_on_page AS
SELECT
    report_date,
    ROUND(AVG(session_time_ms), 0) AS avg_time_on_page_ms
FROM (
    SELECT
        DATE(created_at) AS report_date,
        session_id,
        SUM(CAST(event_data->>'$.time_on_page_ms' AS UNSIGNED)) AS session_time_ms
    FROM event_logs
    WHERE event_type = 'search_page_exited'
    GROUP BY DATE(created_at), session_id
) sub
GROUP BY report_date;

-- v_p1_fuc: 세션당 필터 사용 횟수
-- filter_apply_button_clicked + calendar_date_clicked + calendar_period_navigated 합산
CREATE OR REPLACE VIEW v_p1_fuc AS
SELECT
    report_date,
    ROUND(AVG(fuc_count), 2) AS avg_fuc
FROM (
    SELECT
        DATE(created_at) AS report_date,
        session_id,
        COUNT(*) AS fuc_count
    FROM event_logs
    WHERE event_type IN ('filter_apply_button_clicked', 'calendar_date_clicked', 'calendar_period_navigated')
    GROUP BY DATE(created_at), session_id
) sub
GROUP BY report_date;

-- v_p1_rer: 탐색 반복률
-- 동일 세션 내 search_page_entered >= 2 AND detail_page_entered >= 1
CREATE OR REPLACE VIEW v_p1_rer AS
SELECT
    DATE(s.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN s_cnt.search_cnt >= 2 AND d_cnt.detail_cnt >= 1
            THEN s.session_id
        END) / NULLIF(COUNT(DISTINCT s.session_id), 0),
        4
    ) AS rer
FROM event_logs s
LEFT JOIN (
    SELECT session_id, COUNT(*) AS search_cnt
    FROM event_logs WHERE event_type = 'search_page_entered'
    GROUP BY session_id
) s_cnt ON s_cnt.session_id = s.session_id
LEFT JOIN (
    SELECT session_id, COUNT(*) AS detail_cnt
    FROM event_logs WHERE event_type = 'detail_page_entered'
    GROUP BY session_id
) d_cnt ON d_cnt.session_id = s.session_id
WHERE s.event_type = 'search_page_entered'
GROUP BY DATE(s.created_at);

-- v_p1_afa: 세션당 필터 적용 횟수
CREATE OR REPLACE VIEW v_p1_afa AS
SELECT
    report_date,
    ROUND(AVG(afa_count), 2) AS avg_afa
FROM (
    SELECT
        DATE(created_at) AS report_date,
        session_id,
        COUNT(*) AS afa_count
    FROM event_logs
    WHERE event_type = 'filter_apply_button_clicked'
    GROUP BY DATE(created_at), session_id
) sub
GROUP BY report_date;

-- v_p1_sur: 검색 사용 세션율
CREATE OR REPLACE VIEW v_p1_sur AS
SELECT
    DATE(s.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT q.session_id) / NULLIF(COUNT(DISTINCT s.session_id), 0),
        4
    ) AS sur
FROM event_logs s
LEFT JOIN event_logs q
  ON q.session_id = s.session_id
 AND q.event_type = 'search_query_submitted'
WHERE s.event_type = 'search_page_entered'
GROUP BY DATE(s.created_at);

-- v_p1_scr: 정렬 변경률
CREATE OR REPLACE VIEW v_p1_scr AS
SELECT
    DATE(s.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT sc.session_id) / NULLIF(COUNT(DISTINCT s.session_id), 0),
        4
    ) AS scr
FROM event_logs s
LEFT JOIN event_logs sc
  ON sc.session_id = s.session_id
 AND sc.event_type = 'sort_changed'
WHERE s.event_type = 'search_page_entered'
GROUP BY DATE(s.created_at);

-- v_p1_time_on_page_seg: 체류시간 Filtered/Non Filtered 세그먼트 비교
-- [주의사항 2] Filtered 판정: 동일 session_id에 filter_apply_button_clicked 행 EXISTS 여부
CREATE OR REPLACE VIEW v_p1_time_on_page_seg AS
SELECT
    report_date,
    segment,
    ROUND(AVG(session_time_ms), 0) AS avg_time_on_page_ms
FROM (
    SELECT
        DATE(e.created_at) AS report_date,
        e.session_id,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM event_logs fa
                WHERE fa.session_id = e.session_id
                  AND fa.event_type = 'filter_apply_button_clicked'
            ) THEN 'Filtered'
            ELSE 'Non Filtered'
        END AS segment,
        SUM(CAST(e.event_data->>'$.time_on_page_ms' AS UNSIGNED)) AS session_time_ms
    FROM event_logs e
    WHERE e.event_type = 'search_page_exited'
    GROUP BY DATE(e.created_at), e.session_id
) sub
GROUP BY report_date, segment;

-- v_p1_ttd_seg: TTD Filtered/Non Filtered 세그먼트 비교
-- is_filtered_session은 festival_item_clicked의 event_data에 존재
CREATE OR REPLACE VIEW v_p1_ttd_seg AS
SELECT
    report_date,
    segment,
    ROUND(AVG(first_ttd_ms), 0) AS avg_ttd_ms
FROM (
    SELECT
        DATE(created_at) AS report_date,
        session_id,
        CASE
            WHEN event_data->>'$.is_filtered_session' = 'true' THEN 'Filtered'
            ELSE 'Non Filtered'
        END AS segment,
        MIN(CAST(event_data->>'$.time_since_page_entered_ms' AS UNSIGNED)) AS first_ttd_ms
    FROM event_logs
    WHERE event_type = 'festival_item_clicked'
    GROUP BY DATE(created_at), session_id, segment
) sub
GROUP BY report_date, segment;


-- ----------------------------------------------------------------
-- P2 — 상세 페이지 지표 (6종)
-- ----------------------------------------------------------------

-- v_p2_section_reach: 섹션별 도달율
CREATE OR REPLACE VIEW v_p2_section_reach AS
SELECT
    DATE(d.created_at) AS report_date,
    sv.section_name,
    ROUND(
        COUNT(DISTINCT sv.session_id) / NULLIF(COUNT(DISTINCT d.session_id), 0),
        4
    ) AS reach_rate
FROM event_logs d
LEFT JOIN (
    SELECT session_id, event_data->>'$.section_name' AS section_name, created_at
    FROM event_logs
    WHERE event_type = 'section_viewed'
) sv ON sv.session_id = d.session_id
WHERE d.event_type = 'detail_page_entered'
GROUP BY DATE(d.created_at), sv.section_name;

-- v_p2_blog_click: 블로그 리뷰 링크 클릭율
CREATE OR REPLACE VIEW v_p2_blog_click AS
SELECT
    DATE(d.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT b.session_id) / NULLIF(COUNT(DISTINCT d.session_id), 0),
        4
    ) AS blog_click_rate
FROM event_logs d
LEFT JOIN event_logs b
  ON b.session_id = d.session_id
 AND b.event_type = 'blog_review_clicked'
WHERE d.event_type = 'detail_page_entered'
GROUP BY DATE(d.created_at);

-- v_p2_immediate_bounce: 상세 페이지 즉시 이탈율
CREATE OR REPLACE VIEW v_p2_immediate_bounce AS
SELECT
    DATE(d.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN CAST(ex.event_data->>'$.sections_viewed_count' AS UNSIGNED) = 0
            THEN ex.session_id
        END) / NULLIF(COUNT(DISTINCT d.session_id), 0),
        4
    ) AS immediate_bounce_rate
FROM event_logs d
LEFT JOIN event_logs ex
  ON ex.session_id = d.session_id
 AND ex.event_type = 'detail_page_exited'
WHERE d.event_type = 'detail_page_entered'
GROUP BY DATE(d.created_at);

-- v_p2_review_position: 리뷰 포지션별 클릭 분포
CREATE OR REPLACE VIEW v_p2_review_position AS
SELECT
    DATE(b.created_at) AS report_date,
    CAST(b.event_data->>'$.review_index' AS UNSIGNED) AS review_index,
    ROUND(
        COUNT(DISTINCT b.session_id) / NULLIF(total.total_sessions, 0),
        4
    ) AS click_share
FROM event_logs b
JOIN (
    SELECT
        DATE(created_at) AS report_date,
        COUNT(DISTINCT session_id) AS total_sessions
    FROM event_logs
    WHERE event_type = 'blog_review_clicked'
    GROUP BY DATE(created_at)
) total ON total.report_date = DATE(b.created_at)
WHERE b.event_type = 'blog_review_clicked'
GROUP BY DATE(b.created_at), CAST(b.event_data->>'$.review_index' AS UNSIGNED), total.total_sessions;

-- v_p2_blog_return: 블로그 클릭 후 복귀율
-- blog_review_clicked 후 동일 세션에서 created_at이 더 큰 section_viewed 존재
CREATE OR REPLACE VIEW v_p2_blog_return AS
SELECT
    DATE(b.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN sv.id IS NOT NULL THEN b.session_id
        END) / NULLIF(COUNT(DISTINCT b.session_id), 0),
        4
    ) AS return_rate
FROM event_logs b
LEFT JOIN event_logs sv
  ON sv.session_id = b.session_id
 AND sv.event_type = 'section_viewed'
 AND sv.created_at > b.created_at
WHERE b.event_type = 'blog_review_clicked'
GROUP BY DATE(b.created_at);

-- v_p2_share: 공유 버튼 클릭율
CREATE OR REPLACE VIEW v_p2_share AS
SELECT
    DATE(d.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT sh.session_id) / NULLIF(COUNT(DISTINCT d.session_id), 0),
        4
    ) AS share_rate
FROM event_logs d
LEFT JOIN event_logs sh
  ON sh.session_id = d.session_id
 AND sh.event_type = 'share_button_clicked'
WHERE d.event_type = 'detail_page_entered'
GROUP BY DATE(d.created_at);


-- ----------------------------------------------------------------
-- P3 — 전환 지표 (5종)
-- ----------------------------------------------------------------

-- v_p3_conversion: P3 전환율 (상세→예매 의사)
CREATE OR REPLACE VIEW v_p3_conversion AS
SELECT
    DATE(d.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT t.session_id) / NULLIF(COUNT(DISTINCT d.session_id), 0),
        4
    ) AS p3_rate
FROM event_logs d
LEFT JOIN event_logs t
  ON t.session_id = d.session_id
 AND t.event_type = 'ticket_button_clicked'
WHERE d.event_type = 'detail_page_entered'
GROUP BY DATE(d.created_at);

-- v_p3_review_to_ticket: 리뷰 클릭 후 예매처 전환율
CREATE OR REPLACE VIEW v_p3_review_to_ticket AS
SELECT
    DATE(b.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN t.session_id IS NOT NULL THEN b.session_id
        END) / NULLIF(COUNT(DISTINCT b.session_id), 0),
        4
    ) AS review_to_ticket_rate
FROM event_logs b
LEFT JOIN event_logs t
  ON t.session_id = b.session_id
 AND t.event_type = 'ticket_button_clicked'
WHERE b.event_type = 'blog_review_clicked'
GROUP BY DATE(b.created_at);

-- v_p3_no_review_ticket: 리뷰 미클릭 세션의 예매처 전환율
CREATE OR REPLACE VIEW v_p3_no_review_ticket AS
SELECT
    DATE(d.created_at) AS report_date,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN t.session_id IS NOT NULL THEN d.session_id
        END) / NULLIF(COUNT(DISTINCT d.session_id), 0),
        4
    ) AS no_review_ticket_rate
FROM event_logs d
LEFT JOIN event_logs b
  ON b.session_id = d.session_id
 AND b.event_type = 'blog_review_clicked'
LEFT JOIN event_logs t
  ON t.session_id = d.session_id
 AND t.event_type = 'ticket_button_clicked'
WHERE d.event_type = 'detail_page_entered'
  AND b.id IS NULL
GROUP BY DATE(d.created_at);

-- v_p3_review_count_conv: 리뷰 클릭 개수별 예매처 전환율
CREATE OR REPLACE VIEW v_p3_review_count_conv AS
SELECT
    report_date,
    review_count,
    ROUND(
        SUM(has_ticket) / NULLIF(COUNT(*), 0),
        4
    ) AS conversion_rate
FROM (
    SELECT
        DATE(d.created_at) AS report_date,
        d.session_id,
        COALESCE(bc.blog_cnt, 0) AS review_count,
        CASE WHEN t.session_id IS NOT NULL THEN 1 ELSE 0 END AS has_ticket
    FROM event_logs d
    LEFT JOIN (
        SELECT session_id, COUNT(*) AS blog_cnt
        FROM event_logs WHERE event_type = 'blog_review_clicked'
        GROUP BY session_id
    ) bc ON bc.session_id = d.session_id
    LEFT JOIN (
        SELECT DISTINCT session_id
        FROM event_logs WHERE event_type = 'ticket_button_clicked'
    ) t ON t.session_id = d.session_id
    WHERE d.event_type = 'detail_page_entered'
    GROUP BY DATE(d.created_at), d.session_id, bc.blog_cnt, t.session_id
) sub
GROUP BY report_date, review_count;

-- v_p3_section_x_ticket: 섹션 도달 × 예매 전환 교차
CREATE OR REPLACE VIEW v_p3_section_x_ticket AS
SELECT
    DATE(d.created_at) AS report_date,
    sec.section_name,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN sv.session_id IS NOT NULL AND t.session_id IS NOT NULL
            THEN d.session_id
        END) / NULLIF(COUNT(DISTINCT CASE
            WHEN sv.session_id IS NOT NULL THEN d.session_id
        END), 0),
        4
    ) AS reached_ticket_rate,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN sv.session_id IS NULL AND t.session_id IS NOT NULL
            THEN d.session_id
        END) / NULLIF(COUNT(DISTINCT CASE
            WHEN sv.session_id IS NULL THEN d.session_id
        END), 0),
        4
    ) AS not_reached_ticket_rate
FROM event_logs d
CROSS JOIN (
    SELECT 'hero' AS section_name
    UNION ALL SELECT 'basic_info'
    UNION ALL SELECT 'lineup'
    UNION ALL SELECT 'ticket_price'
    UNION ALL SELECT 'ticket_booking'
    UNION ALL SELECT 'blog_review'
) sec
LEFT JOIN event_logs sv
  ON sv.session_id = d.session_id
 AND sv.event_type = 'section_viewed'
 AND sv.event_data->>'$.section_name' = sec.section_name
LEFT JOIN (
    SELECT DISTINCT session_id
    FROM event_logs WHERE event_type = 'ticket_button_clicked'
) t ON t.session_id = d.session_id
WHERE d.event_type = 'detail_page_entered'
GROUP BY DATE(d.created_at), sec.section_name;
