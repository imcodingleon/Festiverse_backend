# E2E 테스트 결과 보고서

> **실행일**: 2026-03-23
> **환경**: Python 3.14.3, Node.js, MySQL 8.0 (Docker), Playwright 1.58

---

## 1. 테스트 구성

### 레이어별 테스트 범위

```
[Playwright E2E]  브라우저 클릭 → FE trackEvent() → POST /api/events → DB → Dashboard API
[BE E2E]          POST /api/events (httpx) → DB → Dashboard API (httpx)
[BE Integration]  DB 직접 삽입 → SQL View 직접 쿼리
```

| 테스트 | 파일 | 검증 범위 | 수 |
|--------|------|----------|------|
| Playwright E2E | `Festiverse_frontend/e2e/event-tracking.spec.ts` | FE→BE 전체 파이프라인 | 8 |
| BE E2E | `app/domains/event_log/tests/test_event_log_e2e.py` | API→DB→Dashboard | 32 |
| BE Integration | `app/domains/event_log/tests/test_event_log_integration.py` | DB→SQL View | 35 |

---

## 2. Playwright E2E 테스트 (8건)

실제 브라우저(Chromium)에서 사용자 시나리오를 수행하고, FE가 보내는 이벤트와 BE 대시보드 지표를 검증.

### 탐색 페이지 이벤트 트래킹 (3건)

| 테스트 | 검증 내용 | 결과 |
|--------|----------|------|
| 페이지 진입 | `app_session_started` + `search_page_entered` 발화, anonymous_id/session_id 존재 | PASS |
| 필터 선택+적용 | `filter_option_toggled` (filter_type, filter_value) + `filter_apply_button_clicked` (applied_filters, filter_count, time_since_page_entered_ms) | PASS |
| 카드 클릭 | `festival_item_clicked` (festival_id, festival_name, list_position, time_since_page_entered_ms) | PASS |

### 상세 페이지 이벤트 트래킹 (3건)

| 테스트 | 검증 내용 | 결과 |
|--------|----------|------|
| 상세 진입 | `detail_page_entered` (festival_id, festival_name) + `section_viewed` (hero 포함) | PASS |
| 블로그 클릭 | `blog_review_clicked` (review_index, review_title, review_url) | PASS |
| 티켓 클릭 | `ticket_button_clicked` (festival_id, ticket_provider, review_clicked_in_session, sections_viewed_in_session) | PASS |

### 풀 퍼널 → 대시보드 정합성 (1건)

| 테스트 | 검증 내용 | 결과 |
|--------|----------|------|
| 전체 시나리오 | 필터→상세→스크롤→티켓 수행 후, 모든 이벤트 동일 session_id 확인, `v_p1_pv` >= 1, `v_p1_dcr` > 0 | PASS |

### 페이로드 스키마 검증 (1건)

| 테스트 | 검증 내용 | 결과 |
|--------|----------|------|
| 필수 필드 | 모든 이벤트에 id, anonymous_id, session_id, event_type, page_url, device_type 포함 | PASS |

---

## 3. BE E2E 테스트 (32건)

httpx AsyncClient로 `POST /api/events` → `GET /api/dashboard/{view}` 파이프라인 검증.

### 이벤트 수집 (2건)

| 테스트 | 기대값 | 실제값 | 결과 |
|--------|--------|--------|------|
| 전체 이벤트 수 | 26건 | 26건 | PASS |
| 오늘 이벤트 수 | 22건 | 22건 | PASS |

### P1 탐색 퍼널 (15건) — 모두 PASS

| View | 핵심 컬럼 | 기대값 | 결과 |
|------|----------|--------|------|
| v_p1_pv | pv | 2 | PASS |
| v_p1_fsr | fsr | 0.5 | PASS |
| v_p1_far | far | 0.5 | PASS |
| v_p1_dcr | dcr | 1.0 | PASS |
| v_p1_tft | avg_tft_ms | 3000 | PASS |
| v_p1_tfa | avg_tfa_ms | 5000 | PASS |
| v_p1_ttd | avg_ttd_ms | 9000 | PASS |
| v_p1_time_on_page | avg_time_on_page_ms | 27500 | PASS |
| v_p1_fuc | avg_fuc | >= 1.0 | PASS |
| v_p1_rer | rer | 0.0 | PASS |
| v_p1_afa | avg_afa | 1.0 | PASS |
| v_p1_sur | sur | 0.0 | PASS |
| v_p1_scr | scr | 0.0 | PASS |
| v_p1_time_on_page_seg | Filtered/Non Filtered | 35000/20000 | PASS |
| v_p1_ttd_seg | Filtered/Non Filtered | 10000/8000 | PASS |

### P2 상세 페이지 (6건), P3 전환 (5건), P4 재방문 (4건) — 모두 PASS

---

## 4. 실행 로그

### Playwright E2E
```
Running 8 tests using 1 worker
8 passed (43.2s)
```

### BE 전체 테스트
```
67 passed in 2.66s
```

---

## 5. 결론

**전체 파이프라인 정합성 확인 완료:**

```
브라우저 클릭 → FE trackEvent() → POST /api/events → DB 저장 → SQL View → Dashboard API
     ✅              ✅                  ✅              ✅          ✅           ✅
```

- FE가 올바른 event_type과 event_data를 전송함을 Playwright로 검증
- BE가 이벤트를 정확히 저장하고 대시보드 지표를 올바르게 산출함을 확인
- 모든 이벤트가 동일한 session_id/anonymous_id를 공유하여 세션 추적이 정상 동작
- P1~P4 퍼널 30종 지표가 기대값과 일치
