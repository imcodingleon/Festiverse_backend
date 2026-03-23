# Festiverse 대시보드 API 명세서

> **작성 기준**: Step 4 BE 구현 코드 (2026-03-22)
> **용도**: FE 대시보드 개발 시 API 응답 구조 참고 문서
> **BE 기술**: FastAPI + MySQL 8.0 + SQLAlchemy (헥사고날 아키텍처)

---

## 1. 공통 사항

### 기본 URL

```
GET /api/dashboard/{view_name}
```

### 쿼리 파라미터

| 파라미터 | 타입 | 필수 | 설명 |
|----------|------|------|------|
| `date_from` | `YYYY-MM-DD` | 아니오 | 조회 시작일 (report_date >= date_from) |
| `date_to` | `YYYY-MM-DD` | 아니오 | 조회 종료일 (report_date <= date_to) |

### 공통 응답 구조

모든 대시보드 API는 동일한 래핑 구조를 사용한다.

```json
{
  "view_name": "v_p1_pv",
  "rows": [
    { "report_date": "2026-03-22", "pv": 142 },
    { "report_date": "2026-03-23", "pv": 198 }
  ],
  "total": 2
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| `view_name` | string | 요청한 View 이름 |
| `rows` | array | View별 결과 행 배열. 컬럼 구조는 View마다 다름 |
| `total` | number | rows 배열 길이 |

- 데이터 없을 시: `{ "view_name": "...", "rows": [], "total": 0 }`
- `rows`는 `report_date` 기준 오름차순(ASC) 정렬
- P1~P3의 `report_date`는 `DATE(created_at)` — 이벤트 발생일 기준 일별 집계
- P4의 `report_date`는 **집계 시점 D** — 아래 P4 섹션 참고

### View 목록 조회

```
GET /api/dashboard/views
```

응답: 사용 가능한 전체 View 이름 배열 (30종, 알파벳 정렬)

```json
["v_p1_afa", "v_p1_dcr", "v_p1_far", "v_p1_fsr", "v_p1_fuc", ...]
```

### 에러 응답

| 상태 코드 | 조건 |
|-----------|------|
| `404` | 존재하지 않는 view_name |
| `400` | 잘못된 파라미터 |

---

## 2. P1 — 탐색 퍼널 지표 (15종)

### v_p1_pv — 탐색 진입 수

```
GET /api/dashboard/v_p1_pv
```

일별 탐색 페이지(`/`) 진입 고유 세션 수.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "pv": 142 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string (YYYY-MM-DD) | 집계 날짜 |
| `pv` | number (integer) | 고유 세션 수 (DISTINCT session_id) |

---

### v_p1_fsr — 필터 선택률

```
GET /api/dashboard/v_p1_fsr
```

탐색 세션 중 필터 칩을 1회 이상 선택(토글)한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "fsr": 0.4523 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `fsr` | number (float, 소수점 4자리) | 필터 선택률 (0.0 ~ 1.0) |

---

### v_p1_far — 필터 적용률

```
GET /api/dashboard/v_p1_far
```

탐색 세션 중 "필터 적용하기" 버튼을 1회 이상 클릭한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "far": 0.3214 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `far` | number (float, 소수점 4자리) | 필터 적용률 (0.0 ~ 1.0) |

---

### v_p1_dcr — P1 전환율 (탐색 → 상세)

```
GET /api/dashboard/v_p1_dcr
```

탐색 세션 중 페스티벌 카드를 클릭하여 상세 페이지로 진입한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "dcr": 0.6789 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `dcr` | number (float, 소수점 4자리) | Detail Click Rate (0.0 ~ 1.0) |

---

### v_p1_tft — 첫 필터 선택 소요시간

```
GET /api/dashboard/v_p1_tft
```

세션별 첫 필터 칩 선택까지 걸린 시간의 일별 평균.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "avg_tft_ms": 3200 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `avg_tft_ms` | number (integer, ms) | 평균 Time to First Toggle (밀리초) |

---

### v_p1_tfa — 첫 필터 적용 소요시간

```
GET /api/dashboard/v_p1_tfa
```

세션별 첫 "필터 적용하기" 클릭까지 걸린 시간의 일별 평균.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "avg_tfa_ms": 8500 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `avg_tfa_ms` | number (integer, ms) | 평균 Time to First Apply (밀리초) |

---

### v_p1_ttd — 상세 도달 소요시간

```
GET /api/dashboard/v_p1_ttd
```

세션별 첫 카드 클릭(상세 진입)까지 걸린 시간의 일별 평균.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "avg_ttd_ms": 12000 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `avg_ttd_ms` | number (integer, ms) | 평균 Time to Detail (밀리초) |

---

### v_p1_time_on_page — 탐색 페이지 체류시간

```
GET /api/dashboard/v_p1_time_on_page
```

세션별 탐색 페이지 누적 체류시간의 일별 평균. 동일 세션에서 탐색→상세→탐색 복귀 시 체류시간이 합산된다.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "avg_time_on_page_ms": 45000 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `avg_time_on_page_ms` | number (integer, ms) | 평균 체류시간 (밀리초). visibility 기반 실질 활성 시간 |

---

### v_p1_fuc — 세션당 필터 사용 횟수

```
GET /api/dashboard/v_p1_fuc
```

세션당 필터 관련 액션(필터 적용 + 캘린더 날짜 클릭 + 캘린더 월 이동) 합산 횟수의 일별 평균.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "avg_fuc": 2.35 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `avg_fuc` | number (float, 소수점 2자리) | 평균 Filter Usage Count |

---

### v_p1_rer — 탐색 반복률

```
GET /api/dashboard/v_p1_rer
```

탐색 세션 중 "탐색 페이지 2회 이상 진입 + 상세 1회 이상 진입"한 세션의 비율 (상세 봤다가 탐색으로 돌아온 경우).

```json
{
  "rows": [
    { "report_date": "2026-03-22", "rer": 0.1234 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `rer` | number (float, 소수점 4자리) | Re-Exploration Rate (0.0 ~ 1.0) |

---

### v_p1_afa — 세션당 필터 적용 횟수

```
GET /api/dashboard/v_p1_afa
```

세션당 "필터 적용하기" 버튼 클릭 횟수의 일별 평균.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "avg_afa": 1.50 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `avg_afa` | number (float, 소수점 2자리) | 평균 Apply Filter Actions |

---

### v_p1_sur — 검색 사용 세션율

```
GET /api/dashboard/v_p1_sur
```

탐색 세션 중 검색 쿼리를 1회 이상 제출한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "sur": 0.0892 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `sur` | number (float, 소수점 4자리) | Search Usage Rate (0.0 ~ 1.0) |

---

### v_p1_scr — 정렬 변경률

```
GET /api/dashboard/v_p1_scr
```

탐색 세션 중 정렬 기준을 1회 이상 변경한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "scr": 0.0456 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `scr` | number (float, 소수점 4자리) | Sort Change Rate (0.0 ~ 1.0) |

---

### v_p1_time_on_page_seg — 체류시간 Filtered/Non Filtered 비교

```
GET /api/dashboard/v_p1_time_on_page_seg
```

탐색 페이지 체류시간을 Filtered(필터 적용 세션) / Non Filtered(미적용 세션)으로 분리하여 비교.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "segment": "Filtered", "avg_time_on_page_ms": 52000 },
    { "report_date": "2026-03-22", "segment": "Non Filtered", "avg_time_on_page_ms": 31000 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `segment` | string | `"Filtered"` 또는 `"Non Filtered"` |
| `avg_time_on_page_ms` | number (integer, ms) | 해당 세그먼트의 평균 체류시간 |

> **FE 구현 참고**: 동일 report_date에 segment가 2행으로 나뉘므로, 차트에서 두 시계열을 분리하여 표시해야 한다. segment별로 그룹화 후 report_date 축 line chart 권장.

> **Filtered 판정 기준**: 동일 session_id로 event_logs 테이블에 `filter_apply_button_clicked` 행이 존재하는지 여부 (event_data 내부 필드가 아님).

---

### v_p1_ttd_seg — 상세 도달 소요시간 Filtered/Non Filtered 비교

```
GET /api/dashboard/v_p1_ttd_seg
```

상세 도달 소요시간을 Filtered/Non Filtered 세그먼트로 분리.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "segment": "Filtered", "avg_ttd_ms": 15000 },
    { "report_date": "2026-03-22", "segment": "Non Filtered", "avg_ttd_ms": 8000 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `segment` | string | `"Filtered"` 또는 `"Non Filtered"` |
| `avg_ttd_ms` | number (integer, ms) | 해당 세그먼트의 평균 TTD |

> **FE 구현 참고**: `v_p1_time_on_page_seg`와 동일한 segment 분리 패턴. 차트에서 두 시계열 비교.

> **Filtered 판정 기준**: festival_item_clicked 이벤트의 `event_data.is_filtered_session` 값 기반.

---

## 3. P2 — 상세 페이지 지표 (6종)

### v_p2_section_reach — 섹션별 도달율

```
GET /api/dashboard/v_p2_section_reach
```

상세 페이지 진입 세션 중 각 섹션까지 스크롤한 세션 비율. 섹션 6종별로 행이 분리된다.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "section_name": "hero", "reach_rate": 0.9800 },
    { "report_date": "2026-03-22", "section_name": "basic_info", "reach_rate": 0.8500 },
    { "report_date": "2026-03-22", "section_name": "lineup", "reach_rate": 0.6200 },
    { "report_date": "2026-03-22", "section_name": "ticket_price", "reach_rate": 0.5400 },
    { "report_date": "2026-03-22", "section_name": "ticket_booking", "reach_rate": 0.4100 },
    { "report_date": "2026-03-22", "section_name": "blog_review", "reach_rate": 0.3500 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `section_name` | string | 섹션 식별자. `"hero"` / `"basic_info"` / `"lineup"` / `"ticket_price"` / `"ticket_booking"` / `"blog_review"` |
| `reach_rate` | number (float, 소수점 4자리) | 도달율 (0.0 ~ 1.0) |

> **FE 구현 참고**: section_name별로 그룹화하여 funnel chart 또는 bar chart로 표시. 섹션 순서는 `hero → basic_info → lineup → ticket_price → ticket_booking → blog_review`.

---

### v_p2_blog_click — 블로그 리뷰 링크 클릭율

```
GET /api/dashboard/v_p2_blog_click
```

상세 페이지 진입 세션 중 블로그 리뷰 링크를 1회 이상 클릭한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "blog_click_rate": 0.2345 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `blog_click_rate` | number (float, 소수점 4자리) | 블로그 클릭율 (0.0 ~ 1.0) |

---

### v_p2_immediate_bounce — 상세 페이지 즉시 이탈율

```
GET /api/dashboard/v_p2_immediate_bounce
```

상세 페이지에 진입했으나 어떤 섹션도 보지 않고(sections_viewed_count=0) 이탈한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "immediate_bounce_rate": 0.0523 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `immediate_bounce_rate` | number (float, 소수점 4자리) | 즉시 이탈율 (0.0 ~ 1.0) |

---

### v_p2_review_position — 리뷰 포지션별 클릭 분포

```
GET /api/dashboard/v_p2_review_position
```

블로그 리뷰 클릭 세션에서 리뷰 위치(1~3번째)별 클릭 점유율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "review_index": 1, "click_share": 0.6000 },
    { "report_date": "2026-03-22", "review_index": 2, "click_share": 0.3000 },
    { "report_date": "2026-03-22", "review_index": 3, "click_share": 0.1000 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `review_index` | number (integer) | 리뷰 위치 (1, 2, 3) |
| `click_share` | number (float, 소수점 4자리) | 해당 위치의 클릭 점유율. 동일 날짜 내 합계 = 1.0 |

---

### v_p2_blog_return — 블로그 클릭 후 복귀율

```
GET /api/dashboard/v_p2_blog_return
```

블로그 리뷰 링크를 클릭한 세션 중, 클릭 이후 다시 상세 페이지 섹션을 본 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "return_rate": 0.4500 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `return_rate` | number (float, 소수점 4자리) | 복귀율 (0.0 ~ 1.0) |

---

### v_p2_share — 공유 버튼 클릭율

```
GET /api/dashboard/v_p2_share
```

상세 페이지 진입 세션 중 공유 버튼을 클릭한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "share_rate": 0.0312 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `share_rate` | number (float, 소수점 4자리) | 공유 클릭율 (0.0 ~ 1.0) |

---

## 4. P3 — 전환 지표 (5종)

### v_p3_conversion — P3 전환율 (상세 → 예매 의사)

```
GET /api/dashboard/v_p3_conversion
```

상세 페이지 진입 세션 중 "예매하기" 버튼을 클릭한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "p3_rate": 0.1567 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `p3_rate` | number (float, 소수점 4자리) | P3 전환율 (0.0 ~ 1.0) |

---

### v_p3_review_to_ticket — 리뷰 클릭 후 예매처 전환율

```
GET /api/dashboard/v_p3_review_to_ticket
```

블로그 리뷰를 클릭한 세션 중 "예매하기"도 클릭한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "review_to_ticket_rate": 0.4200 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `review_to_ticket_rate` | number (float, 소수점 4자리) | 리뷰→예매 전환율 (0.0 ~ 1.0) |

---

### v_p3_no_review_ticket — 리뷰 미클릭 세션의 예매처 전환율

```
GET /api/dashboard/v_p3_no_review_ticket
```

블로그 리뷰를 클릭하지 않은 상세 페이지 세션 중 "예매하기"를 클릭한 세션 비율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "no_review_ticket_rate": 0.0890 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `no_review_ticket_rate` | number (float, 소수점 4자리) | 리뷰 미클릭 예매 전환율 (0.0 ~ 1.0). 데이터 없으면 `null` 가능 |

---

### v_p3_review_count_conv — 리뷰 클릭 개수별 예매처 전환율

```
GET /api/dashboard/v_p3_review_count_conv
```

상세 페이지 세션을 리뷰 클릭 횟수(0, 1, 2, 3)로 분류 후, 각 그룹의 예매 전환율.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "review_count": 0, "conversion_rate": 0.0800 },
    { "report_date": "2026-03-22", "review_count": 1, "conversion_rate": 0.3500 },
    { "report_date": "2026-03-22", "review_count": 2, "conversion_rate": 0.5200 },
    { "report_date": "2026-03-22", "review_count": 3, "conversion_rate": 0.6800 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `review_count` | number (integer) | 리뷰 클릭 횟수 (0, 1, 2, 3) |
| `conversion_rate` | number (float, 소수점 4자리) | 해당 그룹의 예매 전환율 (0.0 ~ 1.0) |

> **FE 구현 참고**: review_count별 grouped bar chart 또는 line chart로 "리뷰를 많이 볼수록 전환율이 높아지는가?" 분석용.

---

### v_p3_section_x_ticket — 섹션 도달 x 예매 전환 교차

```
GET /api/dashboard/v_p3_section_x_ticket
```

각 섹션별로 "도달한 세션의 예매 전환율" vs "미도달 세션의 예매 전환율"을 비교.

```json
{
  "rows": [
    { "report_date": "2026-03-22", "section_name": "hero", "reached_ticket_rate": 0.1600, "not_reached_ticket_rate": 0.0200 },
    { "report_date": "2026-03-22", "section_name": "basic_info", "reached_ticket_rate": 0.1800, "not_reached_ticket_rate": 0.0300 },
    { "report_date": "2026-03-22", "section_name": "lineup", "reached_ticket_rate": 0.2200, "not_reached_ticket_rate": 0.0500 },
    { "report_date": "2026-03-22", "section_name": "ticket_price", "reached_ticket_rate": 0.2800, "not_reached_ticket_rate": 0.0400 },
    { "report_date": "2026-03-22", "section_name": "ticket_booking", "reached_ticket_rate": 0.3500, "not_reached_ticket_rate": 0.0300 },
    { "report_date": "2026-03-22", "section_name": "blog_review", "reached_ticket_rate": 0.4000, "not_reached_ticket_rate": 0.0350 }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 날짜 |
| `section_name` | string | 섹션 식별자 (6종) |
| `reached_ticket_rate` | number (float, 소수점 4자리) | 해당 섹션 도달 세션의 예매 전환율. `null` 가능 (도달 세션 0건) |
| `not_reached_ticket_rate` | number (float, 소수점 4자리) | 해당 섹션 미도달 세션의 예매 전환율. `null` 가능 (미도달 세션 0건) |

> **FE 구현 참고**: 섹션별 도달/미도달 2 bar를 나란히 표시하는 grouped bar chart 권장. "어떤 섹션까지 보면 전환율이 올라가는가?" 분석용.

---

## 5. P4 — 재방문 지표 (4종)

> **P4 report_date의 의미가 다르다**: P1~P3의 `report_date`는 `DATE(created_at)` (이벤트 발생일)이지만, P4의 `report_date`는 **집계 시점 D**이다. 집계 시점 D를 기준으로 D-21 ~ D-14 (7일 Intent 윈도우) 내 예매 의사를 보인 사용자를 추출하고, 각 사용자의 마지막 예매 클릭 시점(anchor_time)으로부터 14일 이내 재방문 여부를 판정한다.
>
> FE에서 `date_to` 파라미터로 집계 시점을 지정한다. 미지정 시 오늘 날짜 기준.

### v_p4_intent_users — Intent 사용자 목록

```
GET /api/dashboard/v_p4_intent_users?date_to=2026-03-22
```

Intent 윈도우(D-21 ~ D-14) 내 "예매하기" 클릭한 고유 사용자 목록. **행 단위** (anonymous_id별 1행).

```json
{
  "rows": [
    { "report_date": "2026-03-22", "anonymous_id": "a1b2c3d4-...", "anchor_time": "2026-03-05T14:23:00" },
    { "report_date": "2026-03-22", "anonymous_id": "e5f6g7h8-...", "anchor_time": "2026-03-04T09:15:00" }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string (YYYY-MM-DD) | 집계 시점 D (요청한 date_to) |
| `anonymous_id` | string (UUID) | 사용자 식별자 |
| `anchor_time` | string (datetime) | Intent 윈도우 내 마지막 ticket_button_clicked 시각 |

> **FE 구현 참고**: 이 View는 다른 View와 달리 집계 값이 아닌 **행 단위 데이터**. 총 Intent 사용자 수는 `total` 필드 또는 `rows.length`로 확인.

---

### v_p4_reuse_broad — ReuseUsers (broad)

```
GET /api/dashboard/v_p4_reuse_broad?date_to=2026-03-22
```

Intent 사용자 중 anchor_time 이후 14일 이내에 `app_session_started` 이벤트가 1회 이상 존재하는 사용자 수. **단일 행** 집계.

```json
{
  "rows": [
    {
      "report_date": "2026-03-22",
      "intent_users": 45,
      "reuse_users_broad": 12
    }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 시점 D |
| `intent_users` | number (integer) | Intent 사용자 수 (분모) |
| `reuse_users_broad` | number (integer) | 재방문 사용자 수 — broad 기준 (분자) |

---

### v_p4_reuse_strict — ReuseUsers (strict)

```
GET /api/dashboard/v_p4_reuse_strict?date_to=2026-03-22
```

Intent 사용자 중 anchor_time 이후 14일 이내에 `search_page_entered` 또는 `detail_page_entered` 이벤트가 1회 이상 존재하는 사용자 수. **단일 행** 집계.

```json
{
  "rows": [
    {
      "report_date": "2026-03-22",
      "intent_users": 45,
      "reuse_users_strict": 8
    }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 시점 D |
| `intent_users` | number (integer) | Intent 사용자 수 (분모) |
| `reuse_users_strict` | number (integer) | 재방문 사용자 수 — strict 기준 (분자) |

---

### v_p4_conversion — P4 전환율 (재방문율)

```
GET /api/dashboard/v_p4_conversion?date_to=2026-03-22
```

P4 broad/strict 전환율을 한 번에 조회. **단일 행** 집계.

```json
{
  "rows": [
    {
      "report_date": "2026-03-22",
      "intent_users": 45,
      "reuse_broad": 12,
      "reuse_strict": 8,
      "p4_broad_rate": 0.2667,
      "p4_strict_rate": 0.1778
    }
  ]
}
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `report_date` | string | 집계 시점 D |
| `intent_users` | number (integer) | Intent 사용자 수 (분모) |
| `reuse_broad` | number (integer) | 재방문 broad 수 |
| `reuse_strict` | number (integer) | 재방문 strict 수 |
| `p4_broad_rate` | number (float, 소수점 4자리) | broad 전환율 (reuse_broad / intent_users) |
| `p4_strict_rate` | number (float, 소수점 4자리) | strict 전환율 (reuse_strict / intent_users) |

> **FE 구현 참고**: Intent 사용자가 0명이면 rate는 0. 일별 추이를 보려면 FE에서 date_to를 변경하며 반복 호출하거나, 날짜 범위를 루프 처리해야 한다.

---

## 6. 전체 View 요약 테이블

### P1 — 탐색 퍼널 (15종)

| # | View | 설명 | 핵심 컬럼 | 타입 |
|---|------|------|-----------|------|
| 1 | `v_p1_pv` | 탐색 진입 수 | `pv` | integer |
| 2 | `v_p1_fsr` | 필터 선택률 | `fsr` | float |
| 3 | `v_p1_far` | 필터 적용률 | `far` | float |
| 4 | `v_p1_dcr` | P1 전환율 | `dcr` | float |
| 5 | `v_p1_tft` | 첫 필터 선택 소요시간 | `avg_tft_ms` | integer (ms) |
| 6 | `v_p1_tfa` | 첫 필터 적용 소요시간 | `avg_tfa_ms` | integer (ms) |
| 7 | `v_p1_ttd` | 상세 도달 소요시간 | `avg_ttd_ms` | integer (ms) |
| 8 | `v_p1_time_on_page` | 체류시간 | `avg_time_on_page_ms` | integer (ms) |
| 9 | `v_p1_fuc` | 필터 사용 횟수 | `avg_fuc` | float |
| 10 | `v_p1_rer` | 탐색 반복률 | `rer` | float |
| 11 | `v_p1_afa` | 필터 적용 횟수 | `avg_afa` | float |
| 12 | `v_p1_sur` | 검색 사용률 | `sur` | float |
| 13 | `v_p1_scr` | 정렬 변경률 | `scr` | float |
| 14 | `v_p1_time_on_page_seg` | 체류시간 세그먼트 비교 | `segment`, `avg_time_on_page_ms` | string, integer |
| 15 | `v_p1_ttd_seg` | TTD 세그먼트 비교 | `segment`, `avg_ttd_ms` | string, integer |

### P2 — 상세 페이지 (6종)

| # | View | 설명 | 핵심 컬럼 | 타입 |
|---|------|------|-----------|------|
| 16 | `v_p2_section_reach` | 섹션별 도달율 | `section_name`, `reach_rate` | string, float |
| 17 | `v_p2_blog_click` | 블로그 클릭율 | `blog_click_rate` | float |
| 18 | `v_p2_immediate_bounce` | 즉시 이탈율 | `immediate_bounce_rate` | float |
| 19 | `v_p2_review_position` | 리뷰 포지션별 클릭 분포 | `review_index`, `click_share` | integer, float |
| 20 | `v_p2_blog_return` | 블로그 클릭 후 복귀율 | `return_rate` | float |
| 21 | `v_p2_share` | 공유 버튼 클릭율 | `share_rate` | float |

### P3 — 전환 (5종)

| # | View | 설명 | 핵심 컬럼 | 타입 |
|---|------|------|-----------|------|
| 22 | `v_p3_conversion` | P3 전환율 | `p3_rate` | float |
| 23 | `v_p3_review_to_ticket` | 리뷰→예매 전환율 | `review_to_ticket_rate` | float |
| 24 | `v_p3_no_review_ticket` | 리뷰 미클릭 예매 전환율 | `no_review_ticket_rate` | float |
| 25 | `v_p3_review_count_conv` | 리뷰 개수별 예매 전환율 | `review_count`, `conversion_rate` | integer, float |
| 26 | `v_p3_section_x_ticket` | 섹션 도달 x 예매 교차 | `section_name`, `reached_ticket_rate`, `not_reached_ticket_rate` | string, float, float |

### P4 — 재방문 (4종)

| # | View | 설명 | 핵심 컬럼 | 타입 | 행 구조 |
|---|------|------|-----------|------|---------|
| 27 | `v_p4_intent_users` | Intent 사용자 목록 | `anonymous_id`, `anchor_time` | string, datetime | **행 단위** (사용자별 1행) |
| 28 | `v_p4_reuse_broad` | 재방문 broad | `intent_users`, `reuse_users_broad` | integer, integer | 단일 행 |
| 29 | `v_p4_reuse_strict` | 재방문 strict | `intent_users`, `reuse_users_strict` | integer, integer | 단일 행 |
| 30 | `v_p4_conversion` | P4 전환율 | `p4_broad_rate`, `p4_strict_rate` | float, float | 단일 행 |

---

*이 문서는 Step 5(FE 대시보드 구현) 시 FE Cursor에게 함께 전달하여 API 호출 및 데이터 바인딩 기준으로 사용한다.*
