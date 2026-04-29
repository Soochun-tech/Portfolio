# Day 4 — 분석 SQL 가이드

비즈니스 질문 4개를 직접 풀어보세요. 각 문제는 다음 구조로 돼있어요:

1. **비즈니스 질문** — 답하면 뭐가 바뀌는지
2. **사용할 테이블** — 어디서 데이터 가져올지
3. **분석 접근** — 단계별 사고 흐름 (CTE 단위)
4. **SQL 기법 힌트** — 윈도우/재귀/서브쿼리 중 뭘 쓸지
5. **기대 출력** — 컬럼 구조 + 샘플 row
6. **스트레치** — 더 깊게 파볼 거리

각 문제는 별도 SQL 파일에 답안을 작성하세요. 막히면 알려주세요.

---

## 문제 1 — NYC Taxi: "어느 zone이 deadhead 손실의 주범인가?"

### 비즈니스 질문

택시 기사가 손님을 내려준 zone에서 다음 손님을 잡기까지의 빈 차량 운행이 **deadhead loss**예요. 만약 어느 zone에서 **dropoff는 많은데 pickup이 적다면**, 그 zone에서 끝난 trip의 기사들은 다른 zone까지 빈 차로 이동해야 해요. 

→ Fleet 운영자가 "어느 zone에 pickup 차량을 미리 배치할 것인가"를 데이터로 결정할 수 있게 됩니다.

### 사용할 테이블

```
mart_taxi_daily_zone           -- 일별 × pickup zone
mart_taxi_daily_zone_pair      -- 일별 × pickup zone × dropoff zone
raw_taxi_zone_lookup           -- zone 이름 (Borough, Zone, service_zone)
```

### 분석 접근 (CTE 단계)

작성할 답안 파일: `sql/04_analysis/01_taxi_deadhead.sql`

```sql
USE portfolio;

WITH
-- 1단계: zone별 1월 전체 pickup/dropoff 합계
zone_totals AS (
    SELECT
        zl.locationid AS zone_id,
        zl.zone       AS zone_name,
        zl.borough,
        -- TODO: pickup 횟수 합계 (mart_taxi_daily_zone)
        -- TODO: pickup 매출 합계
        -- TODO: dropoff 횟수 합계 (mart_taxi_daily_zone_pair에서 dropoff_zone_id 기준)
    FROM raw_taxi_zone_lookup zl
    LEFT JOIN ...
    LEFT JOIN ...
    GROUP BY ...
),
-- 2단계: deadhead ratio 계산
deadhead_score AS (
    SELECT
        *,
        -- TODO: dropoff_count - pickup_count = "남는 차량 수" (양수 = 차 남음, 음수 = 차 부족)
        -- TODO: 비율로도 계산: (dropoff - pickup) / pickup
    FROM zone_totals
)
-- 3단계: 윈도우 함수로 ranking
SELECT
    zone_name,
    borough,
    pickup_count,
    dropoff_count,
    surplus,                              -- dropoff - pickup
    surplus_ratio,
    -- TODO: ROW_NUMBER() / RANK()로 deadhead 심각도 순위
    -- TODO: PERCENT_RANK()로 상위 N% 식별
FROM deadhead_score
ORDER BY surplus DESC
LIMIT 20;
```

### SQL 기법

- **CTE 3단계** — 가독성 + 단계별 검증 가능
- **LEFT JOIN x2** — pickup/dropoff 양쪽 합쳐야 함
- **윈도우 함수**: `ROW_NUMBER()`, `RANK()`, `PERCENT_RANK()`
- 추가 점수: `LAG()` / `LEAD()`로 전일 대비 변화 추가

### 기대 출력 (예시)

| zone_name | borough | pickup_count | dropoff_count | surplus | surplus_rank |
|---|---|---|---|---|---|
| Newark Airport (EWR) | EWR | 1,200 | 18,500 | 17,300 | 1 |
| LaGuardia Airport | Queens | 25,000 | 38,000 | 13,000 | 2 |
| ... | ... | ... | ... | ... | ... |

→ 공항이 deadhead 1위로 나올 가능성 큼 (도착 트립 많지만 출발 트립은 다른 곳에서 옴).

### 스트레치

- **시간대별 분석**: pickup_hour를 추가해서 "오전 9시에 어디로 차를 보내야 하나"
- **요일 패턴**: `DAYOFWEEK(partition_date)`로 평일/주말 비교
- **금액 환산**: deadhead 1km당 평균 운영비를 가정해서 손실액 추정

---

## 문제 2 — Olist: "셀러 매출이 어디서 새고 있나?"

### 비즈니스 질문

late delivery → bad review → repeat purchase 안 함 → LTV 손실. 이 funnel이 **어떤 셀러/카테고리에서 가장 심한가**를 찾아야 fleet/물류 우선순위를 정할 수 있어요.

### 사용할 테이블

```
staging_olist_orders              -- order_status, is_late, days_to_deliver
staging_olist_order_items         -- order_id ↔ seller_id, item_total
staging_olist_reviews             -- review_score, is_negative
staging_olist_sellers             -- seller_state
staging_olist_products            -- category_en
staging_olist_customers           -- customer_unique_id (재구매 식별 키)
```

### 분석 접근 (CTE 단계)

작성할 답안 파일: `sql/04_analysis/02_olist_seller_leak.sql`

```sql
USE portfolio;

WITH
-- 1단계: 셀러 × 주문 단위로 평탄화 (late, review, customer 다 합치기)
seller_orders AS (
    SELECT
        oi.seller_id,
        s.seller_state,
        o.order_id,
        c.customer_unique_id,
        o.is_late,
        r.review_score,
        r.is_negative,
        SUM(oi.item_total) OVER (PARTITION BY o.order_id) AS order_value
    FROM staging_olist_orders o
    JOIN staging_olist_order_items oi ON oi.order_id = o.order_id
    LEFT JOIN staging_olist_sellers s ON s.seller_id = oi.seller_id
    LEFT JOIN staging_olist_reviews r ON r.order_id = o.order_id
    LEFT JOIN staging_olist_customers c ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
),
-- 2단계: 셀러별 funnel 지표 집계
seller_funnel AS (
    SELECT
        seller_id,
        seller_state,
        COUNT(DISTINCT order_id)                                   AS total_orders,
        SUM(is_late = TRUE)                                        AS late_orders,
        -- TODO: late_rate = late / total
        -- TODO: avg_review_score
        -- TODO: negative_review_count
        -- TODO: 늦은 주문에서 부정 리뷰 비율 → "late가 review에 미치는 영향"
        -- TODO: customer_unique_id로 다른 주문이 있는지 → 재구매 비율 (윈도우 함수 활용)
        SUM(order_value)                                           AS gmv
    FROM seller_orders
    GROUP BY seller_id, seller_state
),
-- 3단계: 손실 추정 + 랭킹
ranked AS (
    SELECT
        *,
        -- TODO: 추정 손실액 = late_orders × avg_order_value × (1 - 재구매율)
        -- TODO: 손실액 NTILE로 4분위
        -- TODO: 셀러 state별 평균과 비교 (윈도우 함수 PARTITION BY state)
        ROW_NUMBER() OVER (ORDER BY ...) AS leak_rank
    FROM seller_funnel
    WHERE total_orders >= 50    -- 노이즈 제거
)
SELECT *
FROM ranked
ORDER BY leak_rank
LIMIT 20;
```

### SQL 기법

- **다중 LEFT JOIN** — 6개 테이블 결합
- **윈도우 함수**: `SUM() OVER`, `NTILE`, `ROW_NUMBER`, `PARTITION BY`
- **HAVING/WHERE 차이** — 노이즈 셀러 거르기
- **CASE WHEN + AVG** — 비율 계산 패턴

### 기대 출력

| seller_id | state | total_orders | late_rate | avg_review | repeat_rate | est_leak_usd | rank |
|---|---|---|---|---|---|---|---|
| ABC123... | SP | 230 | 18% | 3.1 | 12% | $5,400 | 1 |

### 스트레치

- **카테고리 join** 추가해서 "스포츠 용품 셀러가 가장 심하다" 결론 도출
- **statistical significance**: late_rate가 평균보다 **얼마나** 높은지 z-score
- 위에서 구한 `repeat_rate`를 **윈도우 함수**로 계산 (LAG/LEAD 활용)

---

## 문제 3 — Olist 코호트 리텐션

### 비즈니스 질문

"1월에 첫 구매한 사람들 중 N개월 후에도 구매하는 비율은?"

### 사용할 테이블

```
staging_olist_orders
staging_olist_customers   -- customer_unique_id
```

### 분석 접근

작성할 답안 파일: `sql/04_analysis/03_olist_cohort.sql`

```sql
USE portfolio;

WITH
-- 1단계: 각 구매에 customer_unique_id 매핑
purchases AS (
    SELECT
        c.customer_unique_id,
        o.purchase_date,
        DATE_FORMAT(o.purchase_date, '%Y-%m-01') AS purchase_month
    FROM staging_olist_orders o
    JOIN staging_olist_customers c ON c.customer_id = o.customer_id
),
-- 2단계: 각 customer의 첫 구매 월 식별
first_purchase AS (
    SELECT
        customer_unique_id,
        -- TODO: MIN(purchase_month) — 가장 첫 구매 월 = "코호트"
    FROM purchases
    GROUP BY customer_unique_id
),
-- 3단계: 각 구매에 코호트 + 경과 월수 부여
purchase_with_cohort AS (
    SELECT
        p.customer_unique_id,
        fp.cohort_month,
        p.purchase_month,
        -- TODO: TIMESTAMPDIFF(MONTH, fp.cohort_month, p.purchase_month) — 경과 월
    FROM purchases p
    JOIN first_purchase fp USING (customer_unique_id)
),
-- 4단계: 코호트 × 경과월별 활성 고객 수
cohort_activity AS (
    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM purchase_with_cohort
    GROUP BY cohort_month, months_since_first
),
-- 5단계: 코호트 사이즈 (0개월차 = 첫 구매 시)
cohort_size AS (
    SELECT
        cohort_month,
        active_customers AS cohort_size
    FROM cohort_activity
    WHERE months_since_first = 0
)
-- 최종: 리텐션율 매트릭스
SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.months_since_first,
    ca.active_customers,
    -- TODO: 리텐션율 = active / cohort_size (소수점 4자리)
FROM cohort_activity ca
JOIN cohort_size cs USING (cohort_month)
WHERE ca.months_since_first <= 6
ORDER BY ca.cohort_month, ca.months_since_first;
```

### SQL 기법

- **5단계 CTE** — 단계적으로 빌드업
- **JOIN USING** — 깔끔한 조인 문법
- **TIMESTAMPDIFF** — 월차 계산
- **PIVOT 효과**: 결과를 Tableau에서 cohort heatmap으로 변환할 거예요 (나중)

### 기대 출력 (long format)

| cohort_month | cohort_size | months_since | active_customers | retention_rate |
|---|---|---|---|---|
| 2018-01-01 | 5,200 | 0 | 5,200 | 1.0000 |
| 2018-01-01 | 5,200 | 1 | 240 | 0.0462 |
| 2018-01-01 | 5,200 | 2 | 180 | 0.0346 |
| ... | ... | ... | ... | ... |

> Olist는 일회성 구매가 많아서 리텐션이 낮게 나오는 게 정상이에요 (e-commerce 특성).

### 스트레치

- 리텐션 매트릭스를 wide format으로 PIVOT (CASE WHEN으로 가능)
- **Conditional aggregation**: 늦은 배송 받은 고객 vs 정시 배송 받은 고객의 리텐션 차이

---

## 문제 4 — 재귀 CTE 활용

### 비즈니스 질문

> "2024년 1월 매일의 누적 trip 수와 누적 매출이 어떻게 변했나?"

### 사용할 테이블

```
mart_taxi_daily_zone
```

### 분석 접근

작성할 답안 파일: `sql/04_analysis/04_recursive_demos.sql`

#### 4-1. 재귀 CTE로 날짜 시퀀스 생성

```sql
USE portfolio;

WITH RECURSIVE date_series AS (
    SELECT DATE '2024-01-01' AS d
    UNION ALL
    SELECT d + INTERVAL 1 DAY
    FROM date_series
    WHERE d < '2024-01-31'
)
SELECT * FROM date_series;
```

→ 빈 날(0건 trip)도 dashboard에서 빼먹지 않고 표시 가능. 1월 31일치 row 31개 보장.

#### 4-2. 누적 합계 — 윈도우 함수로 더 간단

```sql
WITH daily_total AS (
    SELECT
        partition_date,
        SUM(trip_count)    AS daily_trips,
        SUM(total_revenue) AS daily_revenue
    FROM mart_taxi_daily_zone
    GROUP BY partition_date
)
SELECT
    partition_date,
    daily_trips,
    daily_revenue,
    -- TODO: 누적 trips (SUM ... OVER ORDER BY partition_date)
    -- TODO: 누적 revenue
    -- TODO: 7일 이동평균 trips (SUM OVER ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    -- TODO: 전일 대비 % 변화 (LAG)
FROM daily_total
ORDER BY partition_date;
```

### SQL 기법

- **재귀 CTE** — 날짜 시퀀스 / 계층 구조 / 트리 워크
- **윈도우 함수 ROWS BETWEEN** — 이동 평균
- **LAG / LEAD** — 전기 대비 비교

### 기대 출력

| partition_date | daily_trips | daily_revenue | cumulative_trips | cumulative_revenue | trips_7day_ma | dod_pct |
|---|---|---|---|---|---|---|
| 2024-01-01 | 75,000 | $1.5M | 75,000 | $1.5M | NULL | NULL |
| 2024-01-02 | 95,000 | $1.9M | 170,000 | $3.4M | NULL | +27% |
| ... | ... | ... | ... | ... | ... | ... |

### 스트레치

- 재귀 CTE로 zone별 **연쇄 trip 경로** 추적 (A→B→C→A 같은 driver 경로 시뮬레이션)
- 시간대별 cumulative (시간 0~23 시퀀스 만들고 매출 누적)

---

## 작성 순서 추천

1. **문제 1** (Taxi deadhead) — 결과가 시각적이라 동기 부여 큼
2. **문제 4** (재귀 CTE) — 가벼운 윈도우 함수 연습
3. **문제 3** (코호트) — 데이터 분석 면접 단골
4. **문제 2** (Seller leak) — 가장 복잡, 마지막에

## 막히면

각 문제마다:
- 막힌 단계 (CTE 1단계? 2단계?)
- 시도한 SQL
- 어떤 결과가 나왔는지

알려주시면 힌트 드릴게요. 정답을 알려주는 게 아니라 다음 한 발자국만 짚어주는 식으로요.

---

## 검증 방법

각 답안 작성 후 Workbench에서 ⚡ 실행. 결과가 비즈니스 상식과 맞는지 한번 stop & think:

- Taxi: 공항 zone이 상위에 있나?
- Olist seller: late_rate 1.0 (100% 늦음) 같은 비현실적 값 없나? (있으면 cleaning 다시)
- 코호트: months_since_first=0의 retention이 1.0인가?
- 재귀: 31개 row 다 나오나?
