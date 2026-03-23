# Step 3 — FE 실제 event_data 구조 확정서

> **작성 기준**: Step 2 구현 코드 (2026-03-22)
> **비교 대상**: `event-tracking-spec.md` 섹션 4 이벤트 정의
> **이벤트 총 17종**: 공통 2 + P1 탐색 9 + P2 상세 5 + P3 전환 1

---

## 0. 공통 payload 구조 (모든 이벤트에 동일 적용)

모든 이벤트는 `trackEvent()` 또는 `sendBeaconEvent()`를 통해 아래 구조로 발화된다.

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "anonymous_id": "a1b2c3d4-5678-9abc-def0-123456789abc",
  "session_id": "f9e8d7c6-5432-1abc-def0-abcdef012345",
  "event_type": "이벤트명",
  "event_data": { /* 이벤트별 상이 — 아래 참조 */ },
  "page_url": "/",
  "device_type": "mobile"
}
```

| 필드 | 타입 | 생성 방식 |
|------|------|-----------|
| `id` | string (UUID) | `crypto.randomUUID()` — 매 이벤트마다 새로 생성 |
| `anonymous_id` | string (UUID) | `localStorage[festiverse_anon_id]` — 없으면 생성 후 저장 |
| `session_id` | string (UUID) | `sessionStorage[festiverse_session_id]` — 30분 미활동 시 갱신 |
| `event_type` | string | snake_case 이벤트명 |
| `event_data` | object | 이벤트별 JSON (아래 정의) |
| `page_url` | string | `window.location.pathname` 또는 `pageUrlOverride` |
| `device_type` | `"mobile"` \| `"desktop"` | `window.innerWidth < 1024 → "mobile"`, `≥ 1024 → "desktop"` |

### 명세 대비 차이점

| 항목 | 명세 | 구현 | 비고 |
|------|------|------|------|
| `pageUrlOverride` | 없음 | `trackEvent(type, data, pageUrlOverride?)` 세 번째 인자로 page_url 오버라이드 가능 | exit 이벤트에서 cleanup 시점에 pathname이 이미 변경되었을 수 있어 enter 시 캡처한 pathname을 전달 |
| `sendBeaconEvent` | `navigator.sendBeacon()` 또는 `keepalive` 사용 | Phase 1에서는 `console.log("[TrackEvent:beacon]", payload)`만 출력. Phase 2에서 실제 beacon 전송 예정 | 동작은 동일, 전송만 로그 |

---

## 1. 공통 이벤트 (2종)

### 1-1. `app_session_started`

**발화 위치**: `useAppTracking()` — `providers.tsx` 마운트 시 1회

```json
{
  "is_return_user": true,
  "days_since_last_visit": 3,
  "referrer": "https://example.com"
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `is_return_user` | boolean | `localStorage[festiverse_last_visit_date]` 존재 여부 | `true` |
| `days_since_last_visit` | number \| null | 마지막 방문 이후 경과 일수. 첫 방문이면 `null` | `3` |
| `referrer` | string \| null | `document.referrer`. 빈 문자열이면 `null`로 변환 | `"https://google.com"` |

**명세 대비 차이: 없음** ✅

---

### 1-2. `favorite_toggled`

**발화 위치**: `trackFavoriteToggled()` — `PerformanceCard`, `DetailHeader`에서 호출

```json
{
  "festival_id": "PF286798",
  "is_favorited": true,
  "source": "search"
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | KOPIS mt20id | `"PF286798"` |
| `is_favorited` | boolean | 토글 후 상태 (true=추가, false=해제) | `true` |
| `source` | `"search"` \| `"detail"` | 발생 페이지 | `"search"` |

**명세 대비 차이: 없음** ✅

---

## 2. P1 — 탐색 페이지 이벤트 (9종)

### 2-1. `search_page_entered`

**발화 위치**: `useSearchPageLifecycle()` — pathname이 `"/"` 일 때

```json
{}
```

**event_data 없음** (공통 payload의 `page_url`, `session_id` 등으로 충분)

**명세 대비 차이: 없음** ✅

---

### 2-2. `search_page_exited`

**발화 위치**: `useSearchPageLifecycle()` cleanup / `visibilitychange` / `pagehide`
**전송 방식**: `sendBeaconEvent()` + `pageUrlOverride`(enter 시점 캡처 pathname)

```json
{
  "time_on_page_ms": 45000
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `time_on_page_ms` | number | 탐색 페이지 누적 체류시간(ms). visibility 기반 일시정지/재개 적용 | `45000` |

**명세 대비 차이:**

| 항목 | 명세 | 구현 | 비고 |
|------|------|------|------|
| 체류시간 계산 | 단순 `Date.now() - pageEnteredAt` | `visibilitychange` 기반 누적 타이머 (탭 비활성 시 일시정지, 복귀 시 재개) | 구현이 명세보다 정밀. 탭 백그라운드 시간 제외하여 실질 체류시간만 측정 |
| page_url | `window.location.pathname` | enter 시 캡처한 pathname을 `pageUrlOverride`로 전달 | cleanup 시 pathname이 변경되어 있을 수 있으므로 |

---

### 2-3. `filter_option_toggled`

**발화 위치**: `trackFilterOptionToggled()` — `FilterSection.tsx` 칩 클릭

```json
{
  "filter_type": "region",
  "filter_value": "서울",
  "is_selected": true,
  "time_since_page_entered_ms": 3200
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `filter_type` | `"region"` \| `"genre"` | 필터 카테고리 | `"region"` |
| `filter_value` | string | 선택/해제된 옵션명 | `"서울"` |
| `is_selected` | boolean | 토글 후 상태 | `true` |
| `time_since_page_entered_ms` | number | `search_page_entered` 이후 경과(ms) | `3200` |

**명세 대비 차이: 없음** ✅

---

### 2-4. `filter_apply_button_clicked`

**발화 위치**: `trackFilterApplyButtonClicked(region, genre)` — `FilterSection.tsx`

```json
{
  "applied_filters": {
    "region": ["서울"],
    "genre": ["EDM"]
  },
  "filter_count": 2,
  "time_since_page_entered_ms": 8500
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `applied_filters` | object | `{ region: string[], genre: string[] }` | `{ "region": ["서울"], "genre": [] }` |
| `filter_count` | number | `region.length + genre.length` | `2` |
| `time_since_page_entered_ms` | number | `search_page_entered` 이후 경과(ms) | `8500` |

**명세 대비 차이:**

| 항목 | 명세 | 구현 | 비고 |
|------|------|------|------|
| `applied_filters` 구조 | `{ region: ["서울", "경기"], genre: ["EDM"] }` (복수 선택 가능 시사) | `region ? [region] : []`, `genre ? [genre] : []` — **각 카테고리 최대 1개** 배열 | 현재 UI가 단일 선택(라디오) 방식이므로 배열 원소 0~1개. 명세는 복수 선택 가능 구조. **호환성 문제 없음** (배열이므로 BE에서 동일 구조로 처리 가능) |
| 부수효과 | 언급 없음 | 발화 시 `markFilteredSession()` 호출 → `isFilteredSession = true` 설정 | `festival_item_clicked`의 `is_filtered_session`에 사용 |

---

### 2-5. `calendar_date_clicked`

**발화 위치**: `trackCalendarDateClicked()` — `StreakCalendar` 날짜 셀 클릭

```json
{
  "selected_date": "2026-03-21",
  "calendar_year": 2026,
  "calendar_month": 3
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `selected_date` | string | `YYYY-MM-DD` | `"2026-03-21"` |
| `calendar_year` | number | 캘린더 표시 연도 | `2026` |
| `calendar_month` | number | 캘린더 표시 월 | `3` |

**명세 대비 차이: 없음** ✅

---

### 2-6. `calendar_period_navigated`

**발화 위치**: `trackCalendarPeriodNavigated()` — `StreakCalendar` `<` `>` 클릭

```json
{
  "direction": "next",
  "from_year_month": "2026-03",
  "to_year_month": "2026-04"
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `direction` | `"next"` \| `"prev"` | 이동 방향 | `"next"` |
| `from_year_month` | string | `YYYY-MM` (이동 전) | `"2026-03"` |
| `to_year_month` | string | `YYYY-MM` (이동 후) | `"2026-04"` |

**명세 대비 차이: 없음** ✅

---

### 2-7. `festival_item_clicked`

**발화 위치**: `trackFestivalItemClicked()` — `PerformanceCard` 클릭

```json
{
  "festival_id": "PF286798",
  "festival_name": "제18회 서울재즈페스티벌",
  "list_position": 3,
  "active_filters": {
    "region": ["서울"],
    "genre": [],
    "selected_date": null,
    "keyword": ""
  },
  "is_filtered_session": true,
  "time_since_page_entered_ms": 12000
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | KOPIS mt20id | `"PF286798"` |
| `festival_name` | string | 페스티벌 이름 | `"제18회 서울재즈페스티벌"` |
| `list_position` | number | 카드의 목록 내 위치 (0-based) | `3` |
| `active_filters` | object | 클릭 시점 Jotai `filterAtom` 상태 | 아래 참조 |
| `active_filters.region` | string[] | 선택된 지역 (0~1개) | `["서울"]` |
| `active_filters.genre` | string[] | 선택된 장르 (0~1개) | `[]` |
| `active_filters.selected_date` | string \| null | 캘린더 선택 날짜 또는 null | `null` |
| `active_filters.keyword` | string | 검색 키워드 (빈 문자열 가능) | `""` |
| `is_filtered_session` | boolean | 세션 내 `filter_apply_button_clicked` 1회+ 발생 여부 | `true` |
| `time_since_page_entered_ms` | number | `search_page_entered` 이후 경과(ms) | `12000` |

**명세 대비 차이:**

| 항목 | 명세 | 구현 | 비고 |
|------|------|------|------|
| `active_filters.region` / `genre` | 복수 선택 배열 시사 | 현재 UI 단일 선택이므로 최대 1개 원소 | 배열 구조 동일 |
| `active_filters.selected_date` | 명세에 포함 | 구현에도 포함 | ✅ |
| `active_filters.keyword` | 명세에 포함 | 구현에도 포함 (`filters.keyword`) | ✅ |
| `is_filtered_session` 출처 | `useRef` 기반 시사 | `trackingState.ts` 모듈 레벨 변수 직접 참조 | 동작 동일 |

---

### 2-8. `search_query_submitted`

**발화 위치**: `trackSearchQuerySubmitted()` — `Header.tsx`, `FilterSection.tsx`

```json
{
  "query_text": "서울 재즈",
  "results_count": 5,
  "source": "header"
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `query_text` | string | 입력된 검색어 | `"서울 재즈"` |
| `results_count` | number \| null | 검색 결과 수 (불가 시 null) | `5` |
| `source` | `"header"` \| `"filter_section"` | 발생 위치 | `"header"` |

**명세 대비 차이: 없음** ✅

---

### 2-9. `sort_changed`

**발화 위치**: `trackSortChanged()` — `PerformanceList.tsx` 정렬 드롭다운

```json
{
  "sort_value": "latest",
  "previous_sort_value": "popular"
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `sort_value` | string | 변경 후 정렬 기준 | `"latest"` |
| `previous_sort_value` | string | 변경 전 정렬 기준 | `"popular"` |

**명세 대비 차이: 없음** ✅

---

## 3. P2 — 상세 페이지 이벤트 (5종)

### 3-1. `detail_page_entered`

**발화 위치**: `useDetailPageLifecycle(id, festivalName?)` — `useEffect([id])`

```json
{
  "festival_id": "PF286798",
  "festival_name": "제18회 서울재즈페스티벌"
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | URL 파라미터 `[id]`에서 추출 | `"PF286798"` |
| `festival_name` | string | API 응답 데이터. 초기 마운트 시 빈 문자열일 수 있음 | `"제18회 서울재즈페스티벌"` |

**명세 대비 차이:**

| 항목 | 명세 | 구현 | 비고 |
|------|------|------|------|
| `festival_name` 타이밍 | API 응답에서 추출 | `useEffect([id])`가 API 응답보다 먼저 실행될 수 있어, 초기에 `""` 전달 가능. 이후 `setDetailFestivalName()`으로 state 갱신 | `detail_page_entered` 시점에 name이 빈 문자열일 수 있음. **BE에서 빈 문자열 허용 필요** |

---

### 3-2. `detail_page_exited`

**발화 위치**: `useDetailPageLifecycle()` cleanup / `visibilitychange` / `pagehide`
**전송 방식**: `sendBeaconEvent()` + `pageUrlOverride`

```json
{
  "festival_id": "PF286798",
  "time_on_page_ms": 32000,
  "last_section_viewed": "ticket_price",
  "sections_viewed_list": ["hero", "basic_info", "lineup", "ticket_price"],
  "sections_viewed_count": 4
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | `trackingState`에서 읽음 | `"PF286798"` |
| `time_on_page_ms` | number | visibility 기반 누적 체류시간(ms) | `32000` |
| `last_section_viewed` | string \| null | `sectionsViewedList`의 마지막 원소. 미발생 시 `null` | `"ticket_price"` |
| `sections_viewed_list` | string[] | Set → Array 변환 (삽입 순서 유지) | `["hero", "basic_info", "lineup", "ticket_price"]` |
| `sections_viewed_count` | number | `sections_viewed_list.length` | `4` |

**명세 대비 차이:**

| 항목 | 명세 | 구현 | 비고 |
|------|------|------|------|
| 체류시간 계산 | 단순 계산 시사 | visibility 기반 누적 타이머 (탭 비활성 시 제외) | `search_page_exited`와 동일 패턴 |
| `last_section_viewed` | `useRef` 기반 시사 | `trackingState.getSectionsViewedList()` 배열의 마지막 원소 | 동작 동일 |

---

### 3-3. `section_viewed`

**발화 위치**: `useDetailPageLifecycle()` — IntersectionObserver (threshold: 0.5)
**중복 방지**: `trackingState.addSectionViewed()` — Set 기반 최초 1회만

```json
{
  "festival_id": "PF286798",
  "section_name": "lineup",
  "section_index": 2,
  "time_since_page_entered_ms": 4200,
  "is_section_rendered": true
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | `trackingState`에서 읽음 | `"PF286798"` |
| `section_name` | string | `"hero"` \| `"basic_info"` \| `"lineup"` \| `"ticket_price"` \| `"ticket_booking"` \| `"blog_review"` | `"lineup"` |
| `section_index` | number | 0=hero, 1=basic_info, 2=lineup, 3=ticket_price, 4=ticket_booking, 5=blog_review | `2` |
| `time_since_page_entered_ms` | number | `detail_page_entered` 이후 경과(ms) | `4200` |
| `is_section_rendered` | boolean | 섹션이 실제 렌더링되었는지 (data-track-rendered 속성) | `true` |

**명세 대비 차이: 없음** ✅

---

### 3-4. `blog_review_clicked`

**발화 위치**: `trackBlogReviewClicked()` — `BlogReviewItem` 링크 클릭

```json
{
  "festival_id": "PF286798",
  "review_index": 1,
  "review_title": "서울재즈페스티벌 후기",
  "review_url": "https://blog.naver.com/...",
  "time_since_page_entered_ms": 18000
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | `trackingState`에서 읽음 | `"PF286798"` |
| `review_index` | number | 클릭한 리뷰의 위치 (1부터 시작, 최대 3) | `1` |
| `review_title` | string | 블로그 리뷰 제목 | `"서울재즈페스티벌 후기"` |
| `review_url` | string | 블로그 리뷰 URL | `"https://blog.naver.com/..."` |
| `time_since_page_entered_ms` | number | `detail_page_entered` 이후 경과(ms) | `18000` |

**부수효과**: 발화 시 `markReviewClicked()` → `reviewClicked = true`, `reviewClickCount += 1`

**명세 대비 차이: 없음** ✅

---

### 3-5. `share_button_clicked`

**발화 위치**: `trackShareButtonClicked()` — `DetailHeader` 공유 아이콘

```json
{
  "festival_id": "PF286798",
  "festival_name": "제18회 서울재즈페스티벌"
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | `trackingState`에서 읽음 | `"PF286798"` |
| `festival_name` | string | `trackingState`에서 읽음 | `"제18회 서울재즈페스티벌"` |

**명세 대비 차이: 없음** ✅

---

## 4. P3 — 전환 이벤트 (1종)

### 4-1. `ticket_button_clicked`

**발화 위치**: `trackTicketButtonClicked(ticketProvider)` — `TicketSection` 예매 버튼

```json
{
  "festival_id": "PF286798",
  "festival_name": "제18회 서울재즈페스티벌",
  "ticket_provider": "melon",
  "review_clicked_in_session": true,
  "review_click_count_in_session": 2,
  "sections_viewed_in_session": ["hero", "basic_info", "lineup", "ticket_price", "ticket_booking", "blog_review"],
  "sections_viewed_count_in_session": 6,
  "time_since_page_entered_ms": 45000
}
```

| 필드 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `festival_id` | string | `trackingState`에서 읽음 | `"PF286798"` |
| `festival_name` | string | `trackingState`에서 읽음 | `"제18회 서울재즈페스티벌"` |
| `ticket_provider` | string | 예매처 식별자 (현재 `"melon"`) | `"melon"` |
| `review_clicked_in_session` | boolean | 상세 페이지 진입 후 `blog_review_clicked` 발생 여부 | `true` |
| `review_click_count_in_session` | number | `blog_review_clicked` 누적 횟수 | `2` |
| `sections_viewed_in_session` | string[] | 상세 페이지 진입 후 본 섹션 목록 | `["hero", "basic_info", ...]` |
| `sections_viewed_count_in_session` | number | `sections_viewed_in_session.length` (Set.size 기준) | `6` |
| `time_since_page_entered_ms` | number | `detail_page_entered` 이후 경과(ms) | `45000` |

**명세 대비 차이: 없음** ✅

---

## 5. 명세 대비 구현 차이점 요약

### 5-1. 구조적 차이 (호환성 문제 없음)

| # | 항목 | 명세 | 구현 | 영향 |
|---|------|------|------|------|
| 1 | 세션 상태 저장 방식 | `useRef` 기반 시사 | `trackingState.ts` 모듈 레벨 변수 + getter/setter 함수 | 동작 동일. 테스트 용이성 향상 |
| 2 | 체류시간 계산 | 단순 `Date.now() - enteredAt` | visibility 기반 누적 타이머 (비활성 시간 제외) | **더 정밀**. BE에서 받는 값의 의미가 "실질 활성 체류시간"으로 변경 |
| 3 | `pageUrlOverride` | 없음 | exit 이벤트에서 enter 시 캡처한 pathname 전달 | cleanup 시점의 pathname 변경 문제 해결 |
| 4 | `sendBeaconEvent` | `navigator.sendBeacon()` 또는 `keepalive` | Phase 1: console.log. Phase 2에서 실제 전송 코드 주석으로 준비됨 | Phase 2 전환 시 주석 해제만 필요 |
| 5 | `applied_filters` 원소 수 | 복수 선택 가능 시사 | 현재 UI 단일 선택 → 배열 원소 0~1개 | 배열 구조이므로 BE 호환성 문제 없음. UI가 복수 선택으로 변경 시 자동 대응 |

### 5-2. 데이터 주의사항 (BE 수신 시 고려)

| # | 이벤트 | 필드 | 주의사항 |
|---|--------|------|----------|
| 1 | `detail_page_entered` | `festival_name` | 마운트 시점에 API 응답이 아직 안 왔으면 `""` (빈 문자열). 이후 `setDetailFestivalName()`으로 갱신되나, enter 이벤트에는 빈값이 기록될 수 있음 |
| 2 | `search_page_exited` / `detail_page_exited` | `time_on_page_ms` | visibility 기반이므로, 탭 백그라운드 시간이 제외된 **실질 활성 시간**. 벽시계 시간과 다를 수 있음 |
| 3 | `festival_item_clicked` | `active_filters.keyword` | 빈 문자열 `""` 가능 (필터링 없는 상태) |
| 4 | `festival_item_clicked` | `active_filters.selected_date` | 캘린더 미선택 시 `null` |
| 5 | `app_session_started` | `referrer` | 직접 접속 또는 `document.referrer`가 빈 문자열이면 `null` |

### 5-3. 이벤트 수 비교

| 기준 | 명세 | 구현 |
|------|------|------|
| 명세 원문 | 16종 | — |
| 실제 구현 | — | 17종 |
| 차이 원인 | `favorite_toggled`를 공통 1종으로 카운트 | 탐색/상세 양쪽에서 re-export하여 사용하나, event_type은 동일 1종. 17종은 **발화 지점(handler) 기준** 카운트 |
| **event_type 기준** | **16종** | **16종** ✅ |

> event_type(DB `event_type` 컬럼 기준)은 16종으로 명세와 동일하다. 17종은 핸들러 함수 기준 카운트(favorite_toggled가 search/detail 2개 소스에서 발화).

---

## 6. BE API 수신 시 필요한 event_data JSON Schema (참고)

BE `POST /api/events`가 수신할 때, `event_data` 필드에 대한 타입 검증 참고용이다.

```
app_session_started     → { is_return_user: bool, days_since_last_visit: int|null, referrer: str|null }
favorite_toggled        → { festival_id: str, is_favorited: bool, source: str }
search_page_entered     → {}
search_page_exited      → { time_on_page_ms: int }
filter_option_toggled   → { filter_type: str, filter_value: str, is_selected: bool, time_since_page_entered_ms: int }
filter_apply_button_clicked → { applied_filters: { region: str[], genre: str[] }, filter_count: int, time_since_page_entered_ms: int }
calendar_date_clicked   → { selected_date: str, calendar_year: int, calendar_month: int }
calendar_period_navigated → { direction: str, from_year_month: str, to_year_month: str }
festival_item_clicked   → { festival_id: str, festival_name: str, list_position: int, active_filters: { region: str[], genre: str[], selected_date: str|null, keyword: str }, is_filtered_session: bool, time_since_page_entered_ms: int }
search_query_submitted  → { query_text: str, results_count: int|null, source: str }
sort_changed            → { sort_value: str, previous_sort_value: str }
detail_page_entered     → { festival_id: str, festival_name: str }
detail_page_exited      → { festival_id: str, time_on_page_ms: int, last_section_viewed: str|null, sections_viewed_list: str[], sections_viewed_count: int }
section_viewed          → { festival_id: str, section_name: str, section_index: int, time_since_page_entered_ms: int, is_section_rendered: bool }
blog_review_clicked     → { festival_id: str, review_index: int, review_title: str, review_url: str, time_since_page_entered_ms: int }
share_button_clicked    → { festival_id: str, festival_name: str }
ticket_button_clicked   → { festival_id: str, festival_name: str, ticket_provider: str, review_clicked_in_session: bool, review_click_count_in_session: int, sections_viewed_in_session: str[], sections_viewed_count_in_session: int, time_since_page_entered_ms: int }
```

---

*이 문서는 Step 4(BE 구현) 시 BE Cursor에게 함께 전달하여 API 스키마 설계 기준으로 사용한다.*
