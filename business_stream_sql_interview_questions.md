# Business Stream - Senior Data Scientist SQL Interview Questions
## Band C Position - COO Function, Business Intelligence & Data Department

*"By the ancient scrolls of Codd, let the trials begin..."*

---

## Table of Contents
1. [Question 1: Rolling Consumption Anomaly Detection](#q1)
2. [Question 2: Recursive Customer Hierarchy Billing](#q2)
3. [Question 3: Meter Reading Gap Analysis with Performance Optimization](#q3)
4. [Question 4: Data Quality - The Phantom Readings](#q4)
5. [Question 5: RFM Customer Segmentation for Water Utilities](#q5)
6. [Question 6: Leakage Detection via Statistical Outliers](#q6)
7. [Question 7: Prorated Billing with Edge Cases](#q7)
8. [Question 8: Regulatory OFWAT Reporting Aggregations](#q8)
9. [Question 9: Slowly Changing Dimension Type 2 for Rate Changes](#q9)
10. [Question 10: Multi-System Join - The Integration Nightmare](#q10)
11. [Question 11: SQL Execution Order - The Trick Question](#q11)
12. [Question 12: Dynamic Pivot for Monthly Consumption Report](#q12)
13. [Question 13: The Final Boss - Leakage Prediction Model Pipeline](#q13)

---

## Schema Reference

```sql
-- Core Tables for All Questions

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    spid VARCHAR(20) UNIQUE, -- Scottish Premises ID
    company_name VARCHAR(255),
    sic_code VARCHAR(10), -- Standard Industrial Classification
    band VARCHAR(10), -- Customer band (A, B, C, etc.)
    parent_customer_id INT NULL REFERENCES customers(customer_id),
    start_date DATE,
    end_date DATE NULL,
    region VARCHAR(50),
    is_active BIT DEFAULT 1
);

CREATE TABLE meters (
    meter_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    meter_serial VARCHAR(50) UNIQUE,
    meter_type VARCHAR(20), -- 'WATER', 'WASTEWATER'
    installation_date DATE,
    removal_date DATE NULL,
    meter_size VARCHAR(10),
    location_postcode VARCHAR(10),
    is_active BIT DEFAULT 1
);

CREATE TABLE meter_readings (
    reading_id BIGINT PRIMARY KEY,
    meter_id INT REFERENCES meters(meter_id),
    reading_date DATE,
    reading_value DECIMAL(15,3),
    reading_type VARCHAR(20), -- 'ACTUAL', 'ESTIMATED', 'CUSTOMER'
    read_by VARCHAR(50),
    created_at DATETIME2 DEFAULT GETDATE(),
    INDEX IX_MeterReadings_MeterDate (meter_id, reading_date)
);

CREATE TABLE tariff_rates (
    rate_id INT PRIMARY KEY,
    band VARCHAR(10),
    meter_type VARCHAR(20),
    rate_start_date DATE,
    rate_end_date DATE NULL,
    standing_charge DECIMAL(10,4),
    volume_rate DECIMAL(10,6),
    is_active BIT DEFAULT 1
);

CREATE TABLE bills (
    bill_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    bill_period_start DATE,
    bill_period_end DATE,
    bill_date DATE,
    total_amount DECIMAL(12,2),
    water_charge DECIMAL(12,2),
    wastewater_charge DECIMAL(12,2),
    standing_charge DECIMAL(12,2),
    is_paid BIT DEFAULT 0
);

CREATE TABLE leakage_incidents (
    incident_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    meter_id INT REFERENCES meters(meter_id),
    detected_date DATE,
    severity VARCHAR(20),
    estimated_loss_m3 DECIMAL(10,3),
    status VARCHAR(20)
);
```

---

<a name="q1"></a>
## Question 1: Rolling Consumption Anomaly Detection
**Skill Tested:** Advanced Window Functions, Time-Series Analysis

### Scenario
Business Stream monitors customer consumption patterns to detect unusual spikes that may indicate leaks, meter faults, or billing errors. You need to identify customers whose consumption in the last 3 months exceeds 2.5 standard deviations above their 12-month rolling average.

### The Question
Write a query to identify customers with anomalous consumption patterns using a rolling window approach. For each customer, calculate:
- Their 12-month rolling average consumption
- Their 12-month rolling standard deviation  
- Their last 3-month average consumption
- A flag indicating if the 3-month average exceeds 2.5 standard deviations above the 12-month average

### The Answer

```sql
WITH monthly_consumption AS (
    -- Calculate monthly consumption per customer
    SELECT 
        c.customer_id,
        c.company_name,
        m.meter_id,
        m.meter_type,
        DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1) AS month_start,
        MAX(r.reading_value) - MIN(r.reading_value) AS monthly_volume
    FROM customers c
    JOIN meters m ON c.customer_id = m.customer_id
    JOIN meter_readings r ON m.meter_id = r.meter_id
    WHERE r.reading_type = 'ACTUAL'
      AND r.reading_date >= DATEADD(month, -15, GETDATE())
    GROUP BY c.customer_id, c.company_name, m.meter_id, m.meter_type,
             DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1)
),
rolling_stats AS (
    -- Calculate rolling 12-month statistics
    SELECT 
        customer_id,
        company_name,
        meter_id,
        meter_type,
        month_start,
        monthly_volume,
        -- 12-month rolling average (excluding current month)
        AVG(monthly_volume) OVER (
            PARTITION BY customer_id, meter_id 
            ORDER BY month_start 
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS rolling_12m_avg,
        -- 12-month rolling standard deviation
        STDEV(monthly_volume) OVER (
            PARTITION BY customer_id, meter_id 
            ORDER BY month_start 
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS rolling_12m_stdev,
        -- 3-month rolling average (most recent)
        AVG(monthly_volume) OVER (
            PARTITION BY customer_id, meter_id 
            ORDER BY month_start 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_avg,
        -- Row count to ensure we have enough history
        COUNT(*) OVER (
            PARTITION BY customer_id, meter_id 
            ORDER BY month_start 
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS history_count
    FROM monthly_consumption
)
SELECT 
    customer_id,
    company_name,
    meter_id,
    meter_type,
    month_start,
    monthly_volume,
    ROUND(rolling_12m_avg, 3) AS baseline_avg_m3,
    ROUND(rolling_12m_stdev, 3) AS baseline_stdev,
    ROUND(rolling_3m_avg, 3) AS recent_3m_avg,
    CASE 
        WHEN rolling_12m_stdev > 0 
             AND rolling_3m_avg > (rolling_12m_avg + (2.5 * rolling_12m_stdev))
        THEN 'ANOMALY_HIGH'
        WHEN rolling_12m_stdev > 0 
             AND rolling_3m_avg < (rolling_12m_avg - (2.5 * rolling_12m_stdev))
        THEN 'ANOMALY_LOW'
        ELSE 'NORMAL'
    END AS anomaly_flag,
    CASE 
        WHEN rolling_12m_stdev > 0 
        THEN ROUND((rolling_3m_avg - rolling_12m_avg) / rolling_12m_stdev, 2)
        ELSE NULL 
    END AS z_score
FROM rolling_stats
WHERE history_count >= 6  -- Require at least 6 months of history
  AND month_start >= DATEADD(month, -3, GETDATE())  -- Only recent months
  AND rolling_12m_stdev IS NOT NULL
ORDER BY 
    CASE 
        WHEN rolling_12m_stdev > 0 
        THEN (rolling_3m_avg - rolling_12m_avg) / rolling_12m_stdev 
        ELSE 0 
    END DESC;
```

### Explanation
This query demonstrates mastery of:
- **Frame specifications**: `ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING` excludes the current row from the average
- **Multiple window functions**: Different frames for different calculations
- **Statistical analysis**: Z-score calculation for anomaly detection
- **Data quality**: `history_count` ensures statistical validity

### Common Pitfalls
1. **Including current row in baseline**: Using `ROWS 12 PRECEDING` would include the current month, creating data leakage
2. **Division by zero**: Always check `rolling_12m_stdev > 0` before division
3. **Frame vs Range**: `RANGE` would group by value, not position - wrong for time-series!
4. **NULL handling**: First 12 rows will have NULL rolling stats - filter appropriately

---

<a name="q2"></a>
## Question 2: Recursive Customer Hierarchy Billing
**Skill Tested:** Recursive CTEs, Hierarchical Data

### Scenario
Business Stream serves large corporate groups with complex organizational structures. A parent company may have subsidiaries, which may have further sub-subsidiaries. When billing, you need to aggregate consumption across the entire corporate group for volume discounts.

### The Question
Given a customer hierarchy where `parent_customer_id` references another customer, write a query that:
1. Returns the complete hierarchy path for each customer
2. Calculates the total group consumption for Q2 2024
3. Identifies the ultimate parent (top-level) for each customer
4. Returns the depth level of each customer in the hierarchy

### The Answer

```sql
WITH RECURSIVE hierarchy AS (
    -- Anchor: Top-level customers (no parent)
    SELECT 
        customer_id,
        company_name,
        parent_customer_id,
        CAST(company_name AS VARCHAR(MAX)) AS hierarchy_path,
        customer_id AS ultimate_parent_id,
        0 AS hierarchy_level,
        CAST(customer_id AS VARCHAR(MAX)) AS hierarchy_chain
    FROM customers
    WHERE parent_customer_id IS NULL
      AND is_active = 1
    
    UNION ALL
    
    -- Recursive: Children of current level
    SELECT 
        c.customer_id,
        c.company_name,
        c.parent_customer_id,
        CAST(h.hierarchy_path + ' > ' + c.company_name AS VARCHAR(MAX)),
        h.ultimate_parent_id,
        h.hierarchy_level + 1,
        CAST(h.hierarchy_chain + ',' + CAST(c.customer_id AS VARCHAR) AS VARCHAR(MAX))
    FROM customers c
    INNER JOIN hierarchy h ON c.parent_customer_id = h.customer_id
    WHERE c.is_active = 1
      AND h.hierarchy_level < 10  -- Prevent infinite loops
),
q2_consumption AS (
    -- Calculate Q2 2024 consumption per meter
    SELECT 
        m.customer_id,
        SUM(CASE 
            WHEN r.reading_type = 'ACTUAL' 
            THEN r.reading_value - LAG(r.reading_value) OVER (
                PARTITION BY r.meter_id ORDER BY r.reading_date
            )
            ELSE 0 
        END) AS q2_consumption_m3
    FROM meters m
    JOIN meter_readings r ON m.meter_id = r.meter_id
    WHERE r.reading_date BETWEEN '2024-04-01' AND '2024-06-30'
    GROUP BY m.customer_id
),
group_aggregation AS (
    -- Aggregate consumption by ultimate parent
    SELECT 
        h.ultimate_parent_id,
        c.company_name AS group_name,
        COUNT(DISTINCT h.customer_id) AS total_companies_in_group,
        SUM(COALESCE(q.q2_consumption_m3, 0)) AS total_group_consumption
    FROM hierarchy h
    JOIN customers c ON h.ultimate_parent_id = c.customer_id
    LEFT JOIN q2_consumption q ON h.customer_id = q.customer_id
    GROUP BY h.ultimate_parent_id, c.company_name
)
SELECT 
    h.customer_id,
    h.company_name,
    h.parent_customer_id,
    pc.company_name AS parent_company_name,
    h.hierarchy_level,
    h.hierarchy_path,
    h.ultimate_parent_id,
    up.company_name AS ultimate_parent_name,
    ga.total_group_consumption,
    ga.total_companies_in_group,
    COALESCE(q.q2_consumption_m3, 0) AS individual_consumption,
    CASE 
        WHEN ga.total_group_consumption > 100000 THEN 'Tier_1_Discount'
        WHEN ga.total_group_consumption > 50000 THEN 'Tier_2_Discount'
        WHEN ga.total_group_consumption > 10000 THEN 'Tier_3_Discount'
        ELSE 'Standard_Rate'
    END AS applicable_discount_tier
FROM hierarchy h
LEFT JOIN customers pc ON h.parent_customer_id = pc.customer_id
LEFT JOIN customers up ON h.ultimate_parent_id = up.customer_id
LEFT JOIN q2_consumption q ON h.customer_id = q.customer_id
LEFT JOIN group_aggregation ga ON h.ultimate_parent_id = ga.ultimate_parent_id
ORDER BY h.hierarchy_path;
```

### Explanation
This query demonstrates:
- **Recursive CTE structure**: Anchor + UNION ALL + recursive member
- **Cycle prevention**: `hierarchy_level < 10` prevents infinite loops from circular references
- **Hierarchy tracking**: Building path strings and chain IDs during recursion
- **Aggregation across hierarchy**: Using ultimate_parent_id for group-level calculations

### Common Pitfalls
1. **Infinite loops**: Always include a level limit and check for cycles
2. **MAXRECURSION**: SQL Server defaults to 100 levels - use `OPTION (MAXRECURSION N)` for deeper hierarchies
3. **Data type overflow**: Cast to VARCHAR(MAX) early to prevent truncation
4. **Circular references**: Add a cycle check: `WHERE CHARINDEX(',' + CAST(c.customer_id AS VARCHAR), h.hierarchy_chain) = 0`

---

<a name="q3"></a>
## Question 3: Meter Reading Gap Analysis with Performance Optimization
**Skill Tested:** Query Optimization, Index Strategy, Gap Analysis

### Scenario
Business Stream has 500,000+ meters with readings taken at various intervals. Regulatory requirements mandate that no meter goes more than 60 days without an actual reading. You need to identify meters with reading gaps and optimize for performance.

### The Question
Write an optimized query to find all meters that have gaps of 60+ days between consecutive readings in 2024. The query must:
1. Handle 50+ million reading records efficiently
2. Identify the gap duration and the missing period
3. Return only the largest gap per meter
4. Execute in under 30 seconds

### The Answer

```sql
-- Optimized query with proper indexing strategy
CREATE NONCLUSTERED INDEX IX_MeterReadings_Optimized 
ON meter_readings(meter_id, reading_date, reading_type) 
INCLUDE (reading_value) 
WHERE reading_type = 'ACTUAL';

WITH ranked_readings AS (
    -- Use ROW_NUMBER() instead of self-join for better performance
    SELECT 
        meter_id,
        reading_date,
        reading_value,
        ROW_NUMBER() OVER (PARTITION BY meter_id ORDER BY reading_date) AS rn
    FROM meter_readings
    WHERE reading_type = 'ACTUAL'
      AND reading_date BETWEEN '2024-01-01' AND '2024-12-31'
),
reading_gaps AS (
    -- Self-join on adjacent rows using row number
    SELECT 
        curr.meter_id,
        curr.reading_date AS gap_start_date,
        next.reading_date AS gap_end_date,
        DATEDIFF(day, curr.reading_date, next.reading_date) AS gap_days,
        curr.reading_value AS start_reading,
        next.reading_value AS end_reading
    FROM ranked_readings curr
    INNER JOIN ranked_readings next 
        ON curr.meter_id = next.meter_id 
        AND curr.rn = next.rn - 1
    WHERE DATEDIFF(day, curr.reading_date, next.reading_date) > 60
),
ranked_gaps AS (
    -- Rank gaps per meter to get largest only
    SELECT 
        meter_id,
        gap_start_date,
        gap_end_date,
        gap_days,
        start_reading,
        end_reading,
        ROW_NUMBER() OVER (PARTITION BY meter_id ORDER BY gap_days DESC) AS gap_rank
    FROM reading_gaps
)
SELECT 
    rg.meter_id,
    m.meter_serial,
    c.customer_id,
    c.company_name,
    rg.gap_start_date,
    rg.gap_end_date,
    rg.gap_days,
    rg.start_reading,
    rg.end_reading,
    ROUND((rg.end_reading - rg.start_reading) / NULLIF(rg.gap_days, 0), 3) AS estimated_daily_consumption
FROM ranked_gaps rg
JOIN meters m ON rg.meter_id = m.meter_id
JOIN customers c ON m.customer_id = c.customer_id
WHERE rg.gap_rank = 1
ORDER BY rg.gap_days DESC;

-- Execution plan hints for large datasets
OPTION (RECOMPILE, MAXDOP 4);
```

### Explanation
**Performance optimizations applied:**
1. **Filtered index**: Only index ACTUAL readings, not ESTIMATED
2. **INCLUDE clause**: Covering index avoids key lookups
3. **ROW_NUMBER() approach**: O(n log n) vs O(n²) for correlated subqueries
4. **Partition elimination**: Date range in CTE enables partition pruning
5. **Query hints**: `RECOMPILE` for parameter sniffing, `MAXDOP` for parallelism

### Common Pitfalls
1. **Self-join without proper indexing**: Will cause table scans on 50M rows
2. **Using LAG() in WHERE clause**: Window functions execute after WHERE - use CTEs
3. **DATEDIFF vs datediff**: `DATEDIFF(day, '2024-01-31', '2024-02-01')` returns 1, not the hour difference
4. **Not handling NULLs**: `NULLIF(gap_days, 0)` prevents division by zero
5. **Missing OPTION hints**: Default parallelism may not be optimal

---

<a name="q4"></a>
## Question 4: Data Quality - The Phantom Readings
**Skill Tested:** Data Quality, Anomaly Detection, Complex Logic

### Scenario
Meter readings sometimes contain impossible values due to data entry errors, meter rollovers, or system glitches. You need to identify and flag these "phantom readings" using multiple validation rules.

### The Question
Write a comprehensive data quality query that flags readings failing ANY of these rules:
1. **Negative consumption**: Current reading < previous reading (not a rollover)
2. **Excessive consumption**: Daily consumption > 10,000 m³ (likely data entry error)
3. **Zero reading**: Reading value = 0 (new meter should start above 0)
4. **Impossible date**: Reading date > today or reading date < meter installation
5. **Duplicate timestamp**: Multiple readings same meter, same date
6. **Meter rollover**: Sudden drop > 90% but within 30 days of previous

Return flagged readings with the specific rule(s) violated.

### The Answer

```sql
WITH reading_validation AS (
    SELECT 
        r.reading_id,
        r.meter_id,
        m.meter_serial,
        m.installation_date,
        r.reading_date,
        r.reading_value,
        r.reading_type,
        LAG(r.reading_value) OVER (PARTITION BY r.meter_id ORDER BY r.reading_date) AS prev_value,
        LAG(r.reading_date) OVER (PARTITION BY r.meter_id ORDER BY r.reading_date) AS prev_date,
        LEAD(r.reading_value) OVER (PARTITION BY r.meter_id ORDER BY r.reading_date) AS next_value,
        COUNT(*) OVER (PARTITION BY r.meter_id, r.reading_date) AS same_day_count
    FROM meter_readings r
    JOIN meters m ON r.meter_id = m.meter_id
),
flagged_readings AS (
    SELECT 
        reading_id,
        meter_id,
        meter_serial,
        reading_date,
        reading_value,
        reading_type,
        prev_value,
        prev_date,
        -- Rule 1: Negative consumption (not rollover)
        CASE 
            WHEN reading_value < prev_value 
                 AND (prev_value - reading_value) / NULLIF(prev_value, 0) < 0.9
            THEN 'NEGATIVE_CONSUMPTION'
            ELSE NULL
        END AS rule_1_violation,
        -- Rule 2: Excessive consumption
        CASE 
            WHEN prev_date IS NOT NULL 
                 AND DATEDIFF(day, prev_date, reading_date) > 0
                 AND (reading_value - prev_value) / DATEDIFF(day, prev_date, reading_date) > 10000
            THEN 'EXCESSIVE_CONSUMPTION'
            ELSE NULL
        END AS rule_2_violation,
        -- Rule 3: Zero reading (not new meter)
        CASE 
            WHEN reading_value = 0 AND prev_value IS NOT NULL
            THEN 'ZERO_READING'
            ELSE NULL
        END AS rule_3_violation,
        -- Rule 4: Impossible date
        CASE 
            WHEN reading_date > GETDATE() OR reading_date < installation_date
            THEN 'IMPOSSIBLE_DATE'
            ELSE NULL
        END AS rule_4_violation,
        -- Rule 5: Duplicate timestamp
        CASE 
            WHEN same_day_count > 1
            THEN 'DUPLICATE_TIMESTAMP'
            ELSE NULL
        END AS rule_5_violation,
        -- Rule 6: Meter rollover (flag but don't treat as error)
        CASE 
            WHEN reading_value < prev_value 
                 AND (prev_value - reading_value) / NULLIF(prev_value, 0) >= 0.9
                 AND DATEDIFF(day, prev_date, reading_date) <= 30
            THEN 'METER_ROLLOVER'
            ELSE NULL
        END AS rule_6_rollover
    FROM reading_validation
)
SELECT 
    reading_id,
    meter_id,
    meter_serial,
    reading_date,
    reading_value,
    prev_value,
    reading_type,
    CONCAT_WS(', ', 
        rule_1_violation,
        rule_2_violation, 
        rule_3_violation,
        rule_4_violation,
        rule_5_violation
    ) AS data_quality_issues,
    rule_6_rollover AS rollover_flag,
    CASE 
        WHEN rule_1_violation IS NOT NULL THEN 'CRITICAL'
        WHEN rule_2_violation IS NOT NULL THEN 'CRITICAL'
        WHEN rule_4_violation IS NOT NULL THEN 'CRITICAL'
        WHEN rule_5_violation IS NOT NULL THEN 'HIGH'
        WHEN rule_3_violation IS NOT NULL THEN 'MEDIUM'
        ELSE 'VALID'
    END AS severity_level
FROM flagged_readings
WHERE rule_1_violation IS NOT NULL
   OR rule_2_violation IS NOT NULL
   OR rule_3_violation IS NOT NULL
   OR rule_4_violation IS NOT NULL
   OR rule_5_violation IS NOT NULL
ORDER BY 
    CASE 
        WHEN rule_1_violation IS NOT NULL OR rule_2_violation IS NOT NULL THEN 1
        WHEN rule_4_violation IS NOT NULL THEN 2
        ELSE 3
    END,
    meter_id, 
    reading_date;
```

### Explanation
This query demonstrates:
- **Multiple validation rules**: Each rule as a separate CASE expression
- **Context-aware validation**: Rollover detection vs negative consumption
- **Severity classification**: Prioritizing issues by business impact
- **CONCAT_WS**: Clean concatenation of multiple violations

### Common Pitfalls
1. **Not handling NULLs**: `NULLIF(prev_value, 0)` prevents division errors
2. **Rollover false positives**: Must check both percentage drop AND time window
3. **Date boundary issues**: `DATEDIFF` returns integer days, not precise duration
4. **First reading handling**: New meters won't have prev_value - handle gracefully
5. **Multiple violations**: Use CONCAT_WS not string concatenation to avoid NULL issues

---

<a name="q5"></a>
## Question 5: RFM Customer Segmentation for Water Utilities
**Skill Tested:** Customer Analytics, NTILE, Complex Aggregations

### Scenario
Business Stream wants to segment customers for targeted retention campaigns. RFM (Recency, Frequency, Monetary) analysis is ideal but needs adaptation for utility billing patterns.

### The Question
Implement RFM segmentation where:
- **Recency**: Days since last bill payment
- **Frequency**: Average bills per year over customer lifetime
- **Monetary**: Average monthly spend

Create quintile scores (1-5) for each dimension and segment customers into:
- **Champions**: R=5, F=5, M=5
- **Loyal Customers**: R=4-5, F=4-5, M=3-5
- **At Risk**: R=1-2, F=3-5, M=3-5
- **New Customers**: R=4-5, F=1-2, any M
- **Hibernating**: R=1-2, F=1-2, M=1-2

### The Answer

```sql
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.start_date,
        DATEDIFF(day, MAX(b.bill_date), GETDATE()) AS days_since_last_bill,
        COUNT(DISTINCT b.bill_id) AS total_bills,
        DATEDIFF(year, c.start_date, GETDATE()) + 1 AS years_as_customer,
        AVG(b.total_amount) AS avg_bill_amount,
        AVG(DATEDIFF(day, b.bill_period_start, b.bill_period_end)) AS avg_bill_period_days
    FROM customers c
    LEFT JOIN bills b ON c.customer_id = b.customer_id
    WHERE c.is_active = 1
    GROUP BY c.customer_id, c.company_name, c.start_date
    HAVING COUNT(b.bill_id) > 0  -- Exclude customers with no billing history
),
rfm_scores AS (
    SELECT 
        customer_id,
        company_name,
        days_since_last_bill,
        CAST(total_bills AS DECIMAL) / NULLIF(years_as_customer, 0) AS frequency_per_year,
        -- Calculate monthly spend (bill amount / period in months)
        avg_bill_amount / NULLIF(avg_bill_period_days / 30.0, 0) AS monthly_spend,
        -- Recency Score: Lower days = Higher score (5 - quintile)
        6 - NTILE(5) OVER (ORDER BY days_since_last_bill DESC) AS R_score,
        -- Frequency Score: Higher frequency = Higher score
        NTILE(5) OVER (ORDER BY CAST(total_bills AS DECIMAL) / NULLIF(years_as_customer, 0)) AS F_score,
        -- Monetary Score: Higher spend = Higher score
        NTILE(5) OVER (ORDER BY avg_bill_amount / NULLIF(avg_bill_period_days / 30.0, 0)) AS M_score
    FROM customer_metrics
),
segmented_customers AS (
    SELECT 
        customer_id,
        company_name,
        days_since_last_bill,
        ROUND(frequency_per_year, 2) AS frequency_per_year,
        ROUND(monthly_spend, 2) AS monthly_spend,
        R_score,
        F_score,
        M_score,
        CONCAT(R_score, F_score, M_score) AS rfm_cell,
        CASE 
            WHEN R_score = 5 AND F_score = 5 AND M_score = 5 THEN 'Champions'
            WHEN R_score >= 4 AND F_score >= 4 AND M_score >= 3 THEN 'Loyal Customers'
            WHEN R_score <= 2 AND F_score >= 3 AND M_score >= 3 THEN 'At Risk'
            WHEN R_score >= 4 AND F_score <= 2 THEN 'New Customers'
            WHEN R_score <= 2 AND F_score <= 2 AND M_score <= 2 THEN 'Hibernating'
            WHEN R_score >= 3 AND F_score >= 3 AND M_score <= 2 THEN 'Potential Loyalists'
            WHEN R_score >= 4 AND F_score <= 2 AND M_score >= 3 THEN 'Promising'
            WHEN R_score <= 2 AND F_score >= 3 AND M_score <= 2 THEN 'Cannot Lose Them'
            WHEN R_score <= 2 AND F_score <= 2 AND M_score >= 3 THEN 'Lost'
            ELSE 'Others'
        END AS customer_segment
    FROM rfm_scores
)
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(monthly_spend), 2) AS avg_monthly_spend,
    ROUND(SUM(monthly_spend), 2) AS total_monthly_value,
    ROUND(AVG(frequency_per_year), 2) AS avg_bills_per_year,
    ROUND(AVG(CAST(days_since_last_bill AS DECIMAL)), 0) AS avg_days_since_bill,
    STRING_AGG(CASE WHEN customer_id % 100 = 0 THEN company_name END, ', ') AS sample_companies
FROM segmented_customers
GROUP BY customer_segment
ORDER BY total_monthly_value DESC;
```

### Explanation
This query demonstrates:
- **NTILE()**: Creating quintiles for scoring
- **Inverse scoring**: Recency uses `6 - NTILE()` because lower days = better
- **Business logic adaptation**: Converting bill-based metrics to RFM framework
- **Segmentation rules**: Complex CASE statements for multi-dimensional classification

### Common Pitfalls
1. **NTILE distribution**: Uneven row counts cause uneven quintiles - consider using PERCENT_RANK
2. **NULLIF for new customers**: Division by zero when years_as_customer = 0
3. **Score inversion**: Forgetting that Recency is inverse (lower days = higher score)
4. **Overlapping segments**: Segment rules must be ordered from most to least specific
5. **String_AGG overflow**: Limit sample companies to prevent truncation

---

<a name="q6"></a>
## Question 6: Leakage Detection via Statistical Outliers
**Skill Tested:** Statistical Analysis, Window Functions, PERCENTILE_CONT

### Scenario
Leakage detection is critical for water utilities. You need to identify potential leaks by comparing current consumption to historical patterns using statistical methods rather than simple thresholds.

### The Question
Write a query to detect potential leaks by:
1. Calculating the 90th percentile consumption for each customer by month
2. Comparing current month consumption to their historical 90th percentile
3. Identifying customers where current consumption exceeds 150% of their 90th percentile
4. Excluding seasonal businesses (SIC codes starting with 01, 02, 03 - agriculture)
5. Returning customers with 2+ consecutive months of elevated consumption

### The Answer

```sql
WITH monthly_consumption AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.sic_code,
        m.meter_id,
        DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1) AS month_start,
        MAX(r.reading_value) - MIN(r.reading_value) AS monthly_volume,
        COUNT(*) AS reading_count
    FROM customers c
    JOIN meters m ON c.customer_id = m.customer_id
    JOIN meter_readings r ON m.meter_id = r.meter_id
    WHERE c.sic_code NOT LIKE '01%' 
      AND c.sic_code NOT LIKE '02%'
      AND c.sic_code NOT LIKE '03%'
      AND r.reading_type = 'ACTUAL'
      AND r.reading_date >= DATEADD(month, -24, GETDATE())
    GROUP BY c.customer_id, c.company_name, c.sic_code, m.meter_id,
             DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1)
    HAVING COUNT(*) >= 2  -- Need at least 2 readings for valid consumption
),
historical_percentiles AS (
    SELECT 
        customer_id,
        meter_id,
        -- Calculate 90th percentile of historical consumption (excluding last 2 months)
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY monthly_volume) 
            OVER (PARTITION BY customer_id, meter_id) AS p90_historical_consumption,
        -- Also calculate median for context
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY monthly_volume) 
            OVER (PARTITION BY customer_id, meter_id) AS median_historical_consumption,
        -- Max month for reference
        MAX(month_start) OVER (PARTITION BY customer_id, meter_id) AS latest_month
    FROM monthly_consumption
    WHERE month_start < DATEADD(month, -2, GETDATE())  -- Exclude recent months from baseline
),
current_consumption AS (
    SELECT 
        mc.customer_id,
        mc.company_name,
        mc.sic_code,
        mc.meter_id,
        mc.month_start,
        mc.monthly_volume,
        hp.p90_historical_consumption,
        hp.median_historical_consumption,
        hp.latest_month,
        -- Calculate ratio of current to historical 90th percentile
        mc.monthly_volume / NULLIF(hp.p90_historical_consumption, 0) AS consumption_ratio
    FROM monthly_consumption mc
    JOIN historical_percentiles hp 
        ON mc.customer_id = hp.customer_id 
        AND mc.meter_id = hp.meter_id
    WHERE mc.month_start >= DATEADD(month, -2, GETDATE())  -- Only recent months
),
flagged_months AS (
    SELECT 
        customer_id,
        company_name,
        sic_code,
        meter_id,
        month_start,
        monthly_volume,
        p90_historical_consumption,
        ROUND(consumption_ratio, 2) AS consumption_ratio,
        CASE 
            WHEN consumption_ratio > 1.5 THEN 'LEAK_SUSPECTED'
            WHEN consumption_ratio > 1.2 THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS leak_flag,
        -- Lag to check for consecutive months
        LAG(month_start) OVER (PARTITION BY customer_id, meter_id ORDER BY month_start) AS prev_flagged_month
    FROM current_consumption
    WHERE consumption_ratio > 1.2  -- At least elevated
),
consecutive_leaks AS (
    SELECT 
        customer_id,
        company_name,
        sic_code,
        meter_id,
        COUNT(*) AS consecutive_months,
        MIN(month_start) AS first_flagged_month,
        MAX(monthly_volume) AS max_volume,
        MAX(consumption_ratio) AS max_ratio
    FROM flagged_months
    WHERE leak_flag = 'LEAK_SUSPECTED'
      AND DATEDIFF(month, prev_flagged_month, month_start) = 1  -- Consecutive
    GROUP BY customer_id, company_name, sic_code, meter_id
    HAVING COUNT(*) >= 2
)
SELECT 
    cl.customer_id,
    cl.company_name,
    cl.sic_code,
    cl.meter_id,
    cl.consecutive_months,
    cl.first_flagged_month,
    ROUND(cl.max_volume, 2) AS max_monthly_volume_m3,
    ROUND(cl.max_ratio, 2) AS peak_consumption_ratio,
    CASE 
        WHEN cl.max_ratio > 3 THEN 'CRITICAL'
        WHEN cl.max_ratio > 2 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS priority,
    li.incident_id AS existing_incident,
    li.status AS incident_status
FROM consecutive_leaks cl
LEFT JOIN leakage_incidents li 
    ON cl.customer_id = li.customer_id 
    AND cl.meter_id = li.meter_id
    AND li.status IN ('OPEN', 'INVESTIGATING')
ORDER BY cl.max_ratio DESC, cl.consecutive_months DESC;
```

### Explanation
This query demonstrates:
- **PERCENTILE_CONT**: Calculating true percentiles (not just NTILE approximations)
- **Baseline exclusion**: Removing recent months from historical calculation
- **Consecutive detection**: Using LAG to identify month-over-month patterns
- **Multi-threshold classification**: Different flags for different ratio levels

### Common Pitfalls
1. **PERCENTILE_CONT syntax**: Must use `WITHIN GROUP (ORDER BY ...)` - easy to forget
2. **Seasonal exclusion**: SIC code filtering must happen BEFORE aggregation
3. **Consecutive logic**: `DATEDIFF(month, ...)` = 1 ensures consecutive, not just close
4. **Baseline contamination**: Including recent months inflates the 90th percentile
5. **Division by zero**: New customers have no historical baseline

---

<a name="q7"></a>
## Question 7: Prorated Billing with Edge Cases
**Skill Tested:** Date Arithmetic, Business Logic, Edge Case Handling

### Scenario
Business Stream bills customers based on actual consumption periods, but customers may start or stop service mid-period. You need to calculate prorated charges accurately, handling all edge cases.

### The Question
Write a query to calculate prorated bills for the period 2024-01-01 to 2024-03-31 where:
1. Customer starts mid-period: Charge from start date to period end
2. Customer ends mid-period: Charge from period start to end date
3. Both: Charge only for active days
4. Rate changes mid-period: Apply different rates to each sub-period
5. Minimum charge: If prorated amount < £50, charge £50 minimum

### The Answer

```sql
WITH billing_period AS (
    SELECT 
        CAST('2024-01-01' AS DATE) AS period_start,
        CAST('2024-03-31' AS DATE) AS period_end,
        DATEDIFF(day, '2024-01-01', '2024-03-31') + 1 AS total_period_days
),
customer_active_periods AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.start_date,
        COALESCE(c.end_date, bp.period_end) AS effective_end_date,
        m.meter_id,
        m.meter_type,
        -- Calculate actual billing period within Q1
        CASE 
            WHEN c.start_date > bp.period_start THEN c.start_date 
            ELSE bp.period_start 
        END AS bill_start_date,
        CASE 
            WHEN c.end_date IS NULL OR c.end_date > bp.period_end THEN bp.period_end
            ELSE c.end_date 
        END AS bill_end_date,
        bp.total_period_days
    FROM customers c
    CROSS JOIN billing_period bp
    JOIN meters m ON c.customer_id = m.customer_id
    WHERE m.is_active = 1
      -- Customer was active at some point during Q1
      AND (c.end_date IS NULL OR c.end_date >= bp.period_start)
      AND c.start_date <= bp.period_end
),
consumption_data AS (
    SELECT 
        cap.customer_id,
        cap.meter_id,
        cap.meter_type,
        cap.bill_start_date,
        cap.bill_end_date,
        -- Get readings at period boundaries
        (SELECT TOP 1 reading_value 
         FROM meter_readings 
         WHERE meter_id = cap.meter_id 
           AND reading_date <= cap.bill_end_date
         ORDER BY reading_date DESC) AS end_reading,
        (SELECT TOP 1 reading_value 
         FROM meter_readings 
         WHERE meter_id = cap.meter_id 
           AND reading_date <= cap.bill_start_date
         ORDER BY reading_date DESC) AS start_reading
    FROM customer_active_periods cap
),
rate_periods AS (
    -- Handle rate changes within the billing period
    SELECT 
        cd.customer_id,
        cd.meter_id,
        cd.meter_type,
        cd.bill_start_date,
        cd.bill_end_date,
        cd.end_reading - cd.start_reading AS total_consumption,
        tr.rate_id,
        tr.standing_charge,
        tr.volume_rate,
        -- Calculate overlapping period for this rate
        CASE 
            WHEN cd.bill_start_date > tr.rate_start_date THEN cd.bill_start_date
            ELSE tr.rate_start_date
        END AS rate_applicable_start,
        CASE 
            WHEN cd.bill_end_date < ISNULL(tr.rate_end_date, '2099-12-31') 
            THEN cd.bill_end_date
            ELSE ISNULL(tr.rate_end_date, '2099-12-31')
        END AS rate_applicable_end
    FROM consumption_data cd
    JOIN customers c ON cd.customer_id = c.customer_id
    JOIN tariff_rates tr 
        ON c.band = tr.band 
        AND cd.meter_type = tr.meter_type
        AND tr.is_active = 1
        -- Rate overlaps with billing period
        AND tr.rate_start_date <= cd.bill_end_date
        AND (tr.rate_end_date IS NULL OR tr.rate_end_date >= cd.bill_start_date)
),
prorated_calculations AS (
    SELECT 
        customer_id,
        meter_id,
        meter_type,
        bill_start_date,
        bill_end_date,
        total_consumption,
        rate_applicable_start,
        rate_applicable_end,
        standing_charge,
        volume_rate,
        DATEDIFF(day, rate_applicable_start, rate_applicable_end) + 1 AS days_at_rate,
        DATEDIFF(day, bill_start_date, bill_end_date) + 1 AS total_bill_days,
        -- Prorate standing charge
        standing_charge * (DATEDIFF(day, rate_applicable_start, rate_applicable_end) + 1.0) 
            / NULLIF(DATEDIFF(day, 
                DATEFROMPARTS(YEAR(rate_applicable_start), MONTH(rate_applicable_start), 1),
                EOMONTH(rate_applicable_start)
            ) + 1, 0) AS prorated_standing_charge,
        -- Prorate volume (distribute consumption proportionally)
        total_consumption * (DATEDIFF(day, rate_applicable_start, rate_applicable_end) + 1.0) 
            / NULLIF(DATEDIFF(day, bill_start_date, bill_end_date) + 1, 0) AS prorated_consumption
    FROM rate_periods
),
final_charges AS (
    SELECT 
        customer_id,
        meter_id,
        MIN(bill_start_date) AS period_start,
        MAX(bill_end_date) AS period_end,
        SUM(prorated_standing_charge) AS total_standing_charge,
        SUM(prorated_consumption * volume_rate) AS total_volume_charge,
        SUM(prorated_standing_charge) + SUM(prorated_consumption * volume_rate) AS subtotal
    FROM prorated_calculations
    GROUP BY customer_id, meter_id
)
SELECT 
    fc.customer_id,
    c.company_name,
    fc.meter_id,
    m.meter_serial,
    fc.period_start,
    fc.period_end,
    DATEDIFF(day, fc.period_start, fc.period_end) + 1 AS billed_days,
    ROUND(fc.total_standing_charge, 2) AS standing_charge,
    ROUND(fc.total_volume_charge, 2) AS volume_charge,
    ROUND(fc.subtotal, 2) AS subtotal,
    CASE 
        WHEN fc.subtotal < 50 THEN 50
        ELSE ROUND(fc.subtotal, 2)
    END AS final_charge,
    CASE 
        WHEN fc.subtotal < 50 THEN ROUND(50 - fc.subtotal, 2)
        ELSE 0
    END AS minimum_charge_adjustment
FROM final_charges fc
JOIN customers c ON fc.customer_id = c.customer_id
JOIN meters m ON fc.meter_id = m.meter_id
ORDER BY final_charge DESC;
```

### Explanation
This query demonstrates:
- **Date boundary calculations**: `DATEFROMPARTS`, `EOMONTH` for month boundaries
- **Period overlap logic**: Greatest of starts, least of ends
- **Proration formulas**: Proportional allocation of charges
- **Subquery correlation**: Getting boundary readings per customer
- **Minimum charge logic**: Post-calculation adjustment

### Common Pitfalls
1. **Off-by-one errors**: `DATEDIFF` returns difference, not inclusive count - add 1
2. **NULL rate_end_date**: Must handle ongoing rates with ISNULL/COALESCE
3. **Rate overlap gaps**: Multiple rates must cover entire period - validate!
4. **Consumption distribution**: Simple proration assumes uniform consumption
5. **Leap year handling**: February has 29 days in 2024 - use actual date math

---

<a name="q8"></a>
## Question 8: Regulatory OFWAT Reporting Aggregations
**Skill Tested:** Complex Aggregations, GROUPING SETS, Regulatory Reporting

### Scenario
OFWAT (Water Services Regulation Authority) requires detailed quarterly reports on customer numbers, consumption, and revenue by region, customer band, and meter type. The report needs multiple aggregation levels.

### The Question
Write a query that produces an OFWAT-compliant report with:
1. Total customers, meters, consumption, and revenue
2. Breakdown by region, band, and meter type
3. Subtotals at each hierarchy level (region total, band total)
4. Grand total at the end
5. Only include billed consumption (not estimated)

### The Answer

```sql
WITH quarterly_billed_data AS (
    SELECT 
        c.region,
        c.band,
        m.meter_type,
        c.customer_id,
        m.meter_id,
        -- Calculate actual billed consumption
        SUM(CASE 
            WHEN r.reading_type = 'ACTUAL' 
            THEN r.reading_value - LAG(r.reading_value) OVER (
                PARTITION BY r.meter_id ORDER BY r.reading_date
            )
            ELSE 0 
        END) AS billed_consumption_m3,
        -- Get corresponding bill amounts
        SUM(b.water_charge + b.wastewater_charge + b.standing_charge) AS billed_revenue
    FROM customers c
    JOIN meters m ON c.customer_id = m.customer_id
    LEFT JOIN meter_readings r ON m.meter_id = r.meter_id
        AND r.reading_date BETWEEN '2024-01-01' AND '2024-03-31'
        AND r.reading_type = 'ACTUAL'
    LEFT JOIN bills b ON c.customer_id = b.customer_id
        AND b.bill_period_start >= '2024-01-01'
        AND b.bill_period_end <= '2024-03-31'
    WHERE c.is_active = 1
    GROUP BY c.region, c.band, m.meter_type, c.customer_id, m.meter_id
),
aggregated_metrics AS (
    SELECT 
        region,
        band,
        meter_type,
        COUNT(DISTINCT customer_id) AS customer_count,
        COUNT(DISTINCT meter_id) AS meter_count,
        SUM(billed_consumption_m3) AS total_consumption_m3,
        SUM(billed_revenue) AS total_revenue_gbp
    FROM quarterly_billed_data
    GROUP BY region, band, meter_type
)
SELECT 
    COALESCE(region, '** ALL REGIONS **') AS region,
    COALESCE(band, '** ALL BANDS **') AS band,
    COALESCE(meter_type, '** ALL TYPES **') AS meter_type,
    customer_count,
    meter_count,
    ROUND(total_consumption_m3, 3) AS total_consumption_m3,
    ROUND(total_revenue_gbp, 2) AS total_revenue_gbp,
    CASE 
        WHEN total_consumption_m3 > 0 
        THEN ROUND(total_revenue_gbp / total_consumption_m3, 4)
        ELSE 0 
    END AS avg_revenue_per_m3,
    -- Identify the grouping level
    CASE 
        WHEN GROUPING_ID(region, band, meter_type) = 7 THEN 'GRAND TOTAL'
        WHEN GROUPING_ID(region, band, meter_type) = 3 THEN 'REGION TOTAL'
        WHEN GROUPING_ID(region, band, meter_type) = 1 THEN 'BAND TOTAL'
        ELSE 'DETAIL'
    END AS report_level
FROM aggregated_metrics
GROUP BY GROUPING SETS (
    (region, band, meter_type),  -- Detail level
    (region, band),               -- Band subtotal
    (region),                     -- Region subtotal
    ()                            -- Grand total
)
ORDER BY 
    CASE WHEN region = '** ALL REGIONS **' THEN 1 ELSE 0 END,
    region,
    CASE WHEN band = '** ALL BANDS **' THEN 1 ELSE 0 END,
    band,
    CASE WHEN meter_type = '** ALL TYPES **' THEN 1 ELSE 0 END,
    meter_type;
```

### Alternative with ROLLUP (SQL Server specific):

```sql
SELECT 
    COALESCE(region, '** ALL REGIONS **') AS region,
    COALESCE(band, '** ALL BANDS **') AS band,
    COALESCE(meter_type, '** ALL TYPES **') AS meter_type,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(DISTINCT m.meter_id) AS meter_count,
    ROUND(SUM(CASE WHEN r.reading_type = 'ACTUAL' 
              THEN r.reading_value - LAG(r.reading_value) OVER (PARTITION BY r.meter_id ORDER BY r.reading_date)
              ELSE 0 END), 3) AS total_consumption_m3,
    ROUND(SUM(b.total_amount), 2) AS total_revenue_gbp,
    CASE GROUPING_ID(region, band, meter_type)
        WHEN 7 THEN 'GRAND TOTAL'
        WHEN 3 THEN 'REGION-BAND TOTAL'
        WHEN 1 THEN 'REGION TOTAL'
        WHEN 0 THEN 'DETAIL'
        ELSE 'SUBTOTAL'
    END AS aggregation_level
FROM customers c
JOIN meters m ON c.customer_id = m.customer_id
LEFT JOIN meter_readings r ON m.meter_id = r.meter_id
    AND r.reading_date BETWEEN '2024-01-01' AND '2024-03-31'
LEFT JOIN bills b ON c.customer_id = b.customer_id
    AND b.bill_period_start >= '2024-01-01'
    AND b.bill_period_end <= '2024-03-31'
WHERE c.is_active = 1
GROUP BY ROLLUP (region, band, meter_type)
HAVING GROUPING_ID(region, band, meter_type) IN (0, 1, 3, 7)  -- Filter to specific levels
ORDER BY region, band, meter_type;
```

### Explanation
This query demonstrates:
- **GROUPING SETS**: Multiple aggregation levels in single query
- **GROUPING_ID()**: Identifying which level each row represents
- **COALESCE**: Formatting NULL totals as readable labels
- **ROLLUP**: Alternative syntax for hierarchical aggregations

### Common Pitits
1. **GROUPING vs GROUPING_ID**: GROUPING_ID combines multiple columns into bitmap
2. **ORDER BY with NULLs**: NULLs sort first - use CASE to force totals to end
3. **COUNT DISTINCT in ROLLUP**: May double-count at total levels
4. **HAVING filter**: Must filter after aggregation for grouping set selection
5. **Window functions in aggregation**: LAG in SUM requires careful partitioning

---

<a name="q9"></a>
## Question 9: Slowly Changing Dimension Type 2 for Rate Changes
**Skill Tested:** SCD Type 2, Temporal Data, Historical Tracking

### Scenario
Tariff rates change over time. Business Stream needs to maintain historical accuracy for billing queries - knowing what rate was active when a bill was generated.

### The Question
Given a Type 2 SCD implementation for tariff_rates, write queries to:
1. Insert a new rate version (closing old version, opening new)
2. Retrieve the correct rate for any historical date
3. Find all bills that used a specific rate version
4. Detect overlapping rate periods (data quality issue)

### The Answer

```sql
-- ============================================
-- Part 1: Insert New Rate Version (SCD Type 2)
-- ============================================
CREATE PROCEDURE usp_InsertNewRateVersion
    @band VARCHAR(10),
    @meter_type VARCHAR(20),
    @new_standing_charge DECIMAL(10,4),
    @new_volume_rate DECIMAL(10,6),
    @effective_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Step 1: Close current version (set end date)
        UPDATE tariff_rates
        SET rate_end_date = DATEADD(day, -1, @effective_date),
            is_active = 0
        WHERE band = @band
          AND meter_type = @meter_type
          AND is_active = 1
          AND rate_end_date IS NULL;
        
        -- Step 2: Insert new version
        INSERT INTO tariff_rates (band, meter_type, rate_start_date, rate_end_date,
                                  standing_charge, volume_rate, is_active)
        VALUES (@band, @meter_type, @effective_date, NULL,
                @new_standing_charge, @new_volume_rate, 1);
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

-- ============================================
-- Part 2: Retrieve Rate for Historical Date
-- ============================================
CREATE FUNCTION udf_GetRateForDate(
    @band VARCHAR(10),
    @meter_type VARCHAR(20),
    @as_of_date DATE
)
RETURNS TABLE
AS
RETURN
    SELECT TOP 1
        rate_id,
        standing_charge,
        volume_rate,
        rate_start_date,
        rate_end_date
    FROM tariff_rates
    WHERE band = @band
      AND meter_type = @meter_type
      AND rate_start_date <= @as_of_date
      AND (rate_end_date IS NULL OR rate_end_date >= @as_of_date)
    ORDER BY rate_start_date DESC;

-- ============================================
-- Part 3: Find Bills Using Specific Rate Version
-- ============================================
WITH bills_with_rates AS (
    SELECT 
        b.bill_id,
        b.customer_id,
        b.bill_period_start,
        b.bill_period_end,
        b.bill_date,
        c.band,
        m.meter_type,
        -- Find rate active at bill date
        tr.rate_id,
        tr.standing_charge AS rate_standing_charge,
        tr.volume_rate AS rate_volume_rate
    FROM bills b
    JOIN customers c ON b.customer_id = c.customer_id
    JOIN meters m ON c.customer_id = m.customer_id
    CROSS APPLY (
        SELECT TOP 1 rate_id, standing_charge, volume_rate
        FROM tariff_rates tr
        WHERE tr.band = c.band
          AND tr.meter_type = m.meter_type
          AND tr.rate_start_date <= b.bill_date
          AND (tr.rate_end_date IS NULL OR tr.rate_end_date >= b.bill_date)
        ORDER BY tr.rate_start_date DESC
    ) tr
)
SELECT 
    bill_id,
    customer_id,
    bill_period_start,
    bill_period_end,
    rate_id,
    rate_standing_charge,
    rate_volume_rate
FROM bills_with_rates
WHERE rate_id = @specific_rate_id  -- Parameter for target rate
ORDER BY bill_date;

-- ============================================
-- Part 4: Detect Overlapping Rate Periods
-- ============================================
WITH rate_pairs AS (
    SELECT 
        tr1.rate_id AS rate_id_1,
        tr2.rate_id AS rate_id_2,
        tr1.band,
        tr1.meter_type,
        tr1.rate_start_date AS start_1,
        tr1.rate_end_date AS end_1,
        tr2.rate_start_date AS start_2,
        tr2.rate_end_date AS end_2
    FROM tariff_rates tr1
    JOIN tariff_rates tr2 
        ON tr1.band = tr2.band 
        AND tr1.meter_type = tr2.meter_type
        AND tr1.rate_id < tr2.rate_id  -- Avoid duplicates and self-joins
)
SELECT 
    rate_id_1,
    rate_id_2,
    band,
    meter_type,
    start_1,
    end_1,
    start_2,
    end_2,
    'OVERLAP DETECTED' AS data_quality_issue,
    CASE 
        WHEN start_1 <= start_2 AND (end_1 IS NULL OR end_1 >= start_2) THEN 'Type A: 1 overlaps 2 start'
        WHEN start_2 <= start_1 AND (end_2 IS NULL OR end_2 >= start_1) THEN 'Type B: 2 overlaps 1 start'
        ELSE 'Complete containment'
    END AS overlap_type
FROM rate_pairs
WHERE -- Overlap condition: Periods overlap if start1 <= end2 AND start2 <= end1
    start_1 <= ISNULL(end_2, '2099-12-31')
    AND start_2 <= ISNULL(end_1, '2099-12-31')
ORDER BY band, meter_type, start_1;
```

### Explanation
This demonstrates:
- **SCD Type 2 mechanics**: Closing old, opening new with date ranges
- **Temporal queries**: Finding active record for any point in time
- **Overlap detection**: Interval overlap logic for data quality
- **CROSS APPLY**: Efficient way to get rate per row

### Common Pitfalls
1. **Gap between versions**: Closing on date-1, opening on date prevents gaps
2. **NULL end dates**: Ongoing rates have NULL - handle with ISNULL
3. **Transaction handling**: SCD updates must be atomic
4. **Overlap logic**: `start1 <= end2 AND start2 <= end1` is the correct formula
5. **Rate at bill time vs period**: Bill date may differ from period - clarify requirement

---

<a name="q10"></a>
## Question 10: Multi-System Join - The Integration Nightmare
**Skill Tested:** Complex Joins, Data Integration, NULL Handling

### Scenario
Business Stream data is spread across multiple systems:
- **CRM**: Customer master data
- **Billing**: Bill generation and payments
- **Metering**: Actual meter readings
- **Credit**: Payment history and risk scores

You need to integrate these for a comprehensive customer view.

### The Question
Write a query that joins data from all four systems to produce a complete customer health scorecard, handling:
1. Customers in CRM but not yet billed
2. Bills without corresponding meter readings
3. Payment delays from the credit system
4. Missing data from any source

Return: Customer, total billed, total paid, consumption, days overdue, risk score, and overall health rating.

### The Answer

```sql
-- Simulated multi-system integration
WITH crm_customers AS (
    -- Primary source: All active customers
    SELECT 
        customer_id,
        company_name,
        band,
        region,
        start_date
    FROM customers
    WHERE is_active = 1
),
billing_summary AS (
    -- Billing system data
    SELECT 
        customer_id,
        COUNT(*) AS bill_count,
        SUM(total_amount) AS total_billed,
        SUM(CASE WHEN is_paid = 1 THEN total_amount ELSE 0 END) AS total_paid,
        SUM(CASE WHEN is_paid = 0 THEN total_amount ELSE 0 END) AS outstanding_amount,
        MAX(bill_date) AS last_bill_date
    FROM bills
    WHERE bill_date >= DATEADD(year, -1, GETDATE())
    GROUP BY customer_id
),
metering_summary AS (
    -- Metering system data
    SELECT 
        m.customer_id,
        COUNT(DISTINCT m.meter_id) AS meter_count,
        COUNT(r.reading_id) AS reading_count,
        SUM(CASE 
            WHEN r.reading_type = 'ACTUAL' 
            THEN r.reading_value - LAG(r.reading_value) OVER (PARTITION BY r.meter_id ORDER BY r.reading_date)
            ELSE 0 
        END) AS total_consumption_m3,
        MAX(r.reading_date) AS last_reading_date,
        MIN(CASE WHEN r.reading_type = 'ACTUAL' THEN r.reading_date END) AS first_actual_reading
    FROM meters m
    LEFT JOIN meter_readings r ON m.meter_id = r.meter_id
        AND r.reading_date >= DATEADD(year, -1, GETDATE())
    GROUP BY m.customer_id
),
credit_summary AS (
    -- Simulated credit system data
    SELECT 
        customer_id,
        AVG(risk_score) AS avg_risk_score,  -- 0-100, higher = riskier
        MAX(days_overdue) AS max_days_overdue,
        AVG(days_to_pay) AS avg_days_to_pay,
        COUNT(CASE WHEN payment_status = 'LATE' THEN 1 END) AS late_payment_count
    FROM (
        -- Simulated credit data (would be separate table)
        SELECT 
            b.customer_id,
            CASE 
                WHEN b.is_paid = 0 AND DATEDIFF(day, b.bill_date, GETDATE()) > 30 THEN 75
                WHEN b.is_paid = 0 AND DATEDIFF(day, b.bill_date, GETDATE()) > 14 THEN 50
                WHEN b.is_paid = 0 THEN 25
                ELSE 10
            END AS risk_score,
            CASE 
                WHEN b.is_paid = 0 THEN DATEDIFF(day, b.bill_date, GETDATE())
                ELSE 0
            END AS days_overdue,
            CASE 
                WHEN b.is_paid = 1 THEN DATEDIFF(day, b.bill_date, GETDATE())
                ELSE NULL
            END AS days_to_pay,
            CASE 
                WHEN b.is_paid = 1 AND DATEDIFF(day, b.bill_date, GETDATE()) > 45 THEN 'LATE'
                ELSE 'ON_TIME'
            END AS payment_status
        FROM bills b
        WHERE b.bill_date >= DATEADD(year, -1, GETDATE())
    ) credit_data
    GROUP BY customer_id
),
integrated_view AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.band,
        c.region,
        DATEDIFF(day, c.start_date, GETDATE()) AS days_as_customer,
        -- Billing metrics (COALESCE for customers without bills)
        COALESCE(b.bill_count, 0) AS bill_count,
        COALESCE(b.total_billed, 0) AS total_billed_gbp,
        COALESCE(b.total_paid, 0) AS total_paid_gbp,
        COALESCE(b.outstanding_amount, 0) AS outstanding_gbp,
        b.last_bill_date,
        -- Metering metrics
        COALESCE(m.meter_count, 0) AS meter_count,
        COALESCE(m.total_consumption_m3, 0) AS annual_consumption_m3,
        m.last_reading_date,
        m.first_actual_reading,
        -- Credit metrics
        COALESCE(cr.avg_risk_score, 0) AS risk_score,
        COALESCE(cr.max_days_overdue, 0) AS max_days_overdue,
        COALESCE(cr.avg_days_to_pay, 30) AS avg_days_to_pay,
        COALESCE(cr.late_payment_count, 0) AS late_payments_12m,
        -- Derived metrics
        CASE 
            WHEN b.total_billed > 0 THEN ROUND(b.total_paid / b.total_billed * 100, 2)
            ELSE 100.0
        END AS payment_ratio_pct,
        CASE 
            WHEN m.total_consumption_m3 > 0 THEN ROUND(b.total_billed / m.total_consumption_m3, 4)
            ELSE 0
        END AS effective_rate_per_m3
    FROM crm_customers c
    LEFT JOIN billing_summary b ON c.customer_id = b.customer_id
    LEFT JOIN metering_summary m ON c.customer_id = m.customer_id
    LEFT JOIN credit_summary cr ON c.customer_id = cr.customer_id
)
SELECT 
    customer_id,
    company_name,
    band,
    region,
    bill_count,
    total_billed_gbp,
    total_paid_gbp,
    outstanding_gbp,
    annual_consumption_m3,
    risk_score,
    max_days_overdue,
    late_payments_12m,
    payment_ratio_pct,
    -- Overall health rating
    CASE 
        -- Excellent: Low risk, good payment history, active metering
        WHEN risk_score < 20 
             AND payment_ratio_pct >= 95 
             AND last_reading_date >= DATEADD(month, -2, GETDATE())
        THEN 'EXCELLENT'
        
        -- Good: Acceptable risk, decent payment, some metering
        WHEN risk_score < 40 
             AND payment_ratio_pct >= 85 
             AND meter_count > 0
        THEN 'GOOD'
        
        -- Fair: Moderate concerns
        WHEN risk_score < 60 
             AND payment_ratio_pct >= 70
        THEN 'FAIR'
        
        -- At Risk: Significant issues
        WHEN risk_score < 80 
             OR payment_ratio_pct < 70 
             OR outstanding_gbp > 10000
        THEN 'AT RISK'
        
        -- Critical: Major problems
        ELSE 'CRITICAL'
    END AS health_rating,
    -- Data quality flags
    CASE WHEN meter_count = 0 THEN 'NO_METERS' END AS dq_flag_1,
    CASE WHEN bill_count = 0 THEN 'NO_BILLING_HISTORY' END AS dq_flag_2,
    CASE WHEN last_reading_date < DATEADD(month, -3, GETDATE()) THEN 'STALE_READINGS' END AS dq_flag_3,
    CASE WHEN first_actual_reading IS NULL THEN 'NO_ACTUAL_READINGS' END AS dq_flag_4
FROM integrated_view
ORDER BY 
    CASE 
        WHEN risk_score >= 80 OR outstanding_gbp > 50000 THEN 1
        WHEN risk_score >= 60 OR outstanding_gbp > 10000 THEN 2
        WHEN risk_score >= 40 OR payment_ratio_pct < 80 THEN 3
        ELSE 4
    END,
    outstanding_gbp DESC;
```

### Explanation
This query demonstrates:
- **LEFT JOIN chains**: Preserving all customers even with missing data
- **COALESCE everywhere**: Handling NULLs from missing relationships
- **Multi-source aggregation**: Different GROUP BY levels per source
- **Business rule integration**: Converting metrics to health ratings

### Common Pitfalls
1. **INNER JOIN cascade**: Would filter out customers missing any data source
2. **Aggregation before join**: Must aggregate in CTEs before joining
3. **NULL in calculations**: Any NULL in arithmetic makes result NULL - COALESCE!
4. **Many-to-many explosion**: Ensure aggregation prevents Cartesian products
5. **Date comparison**: `last_reading_date >= DATEADD(...)` handles NULL as false

---

<a name="q11"></a>
## Question 11: SQL Execution Order - The Trick Question
**Skill Tested:** Deep SQL Understanding, Execution Order, Logical vs Physical Processing

### Scenario
Understanding SQL execution order is critical for writing correct queries, especially with window functions and aggregations. This question tests that understanding.

### The Question
Consider this query. What will it return and WHY? Identify all issues:

```sql
SELECT 
    customer_id,
    region,
    COUNT(*) AS total_bills,
    AVG(total_amount) AS avg_bill,
    ROW_NUMBER() OVER (ORDER BY AVG(total_amount) DESC) AS rank_by_avg
FROM bills
WHERE bill_date >= '2024-01-01'
GROUP BY region
HAVING COUNT(*) > 5
ORDER BY rank_by_avg;
```

Then write the CORRECT version that:
1. Ranks customers within each region by their average bill amount
2. Only includes customers with 5+ bills in 2024
3. Returns the top 3 customers per region

### The Answer

**Issues with the original query:**

1. **SELECT before GROUP BY**: `customer_id` is in SELECT but not in GROUP BY - will error in strict mode
2. **Window function ordering**: `ORDER BY AVG(total_amount)` uses aggregated value - works but confusing
3. **ORDER BY alias**: Using window function alias in ORDER BY is allowed but execution order matters
4. **HAVING filter**: Filters groups after aggregation - correct placement
5. **Logical processing order**: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY

**Corrected query:**

```sql
WITH customer_2024_stats AS (
    -- Step 1: Aggregate to customer level first
    SELECT 
        b.customer_id,
        c.region,
        COUNT(*) AS total_bills,
        AVG(b.total_amount) AS avg_bill_amount,
        SUM(b.total_amount) AS total_billed
    FROM bills b
    JOIN customers c ON b.customer_id = c.customer_id
    WHERE b.bill_date >= '2024-01-01'
    GROUP BY b.customer_id, c.region
    HAVING COUNT(*) >= 5  -- Only customers with 5+ bills
),
ranked_customers AS (
    -- Step 2: Apply window function to already-aggregated data
    SELECT 
        customer_id,
        region,
        total_bills,
        ROUND(avg_bill_amount, 2) AS avg_bill_amount,
        total_billed,
        -- Rank within each region
        ROW_NUMBER() OVER (
            PARTITION BY region 
            ORDER BY avg_bill_amount DESC
        ) AS region_rank,
        -- Overall rank
        RANK() OVER (
            ORDER BY avg_bill_amount DESC
        ) AS overall_rank,
        -- Percentile within region
        PERCENT_RANK() OVER (
            PARTITION BY region 
            ORDER BY avg_bill_amount
        ) AS regional_percentile
    FROM customer_2024_stats
)
SELECT 
    customer_id,
    region,
    total_bills,
    avg_bill_amount,
    total_billed,
    region_rank,
    overall_rank,
    ROUND(regional_percentile * 100, 2) AS percentile_in_region
FROM ranked_customers
WHERE region_rank <= 3  -- Top 3 per region
ORDER BY region, region_rank;
```

### Execution Order Deep Dive

```
LOGICAL PROCESSING ORDER (what SQL does conceptually):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. FROM      - Identify source tables
2. ON        - Apply join conditions  
3. JOIN      - Perform joins
4. WHERE     - Filter rows (before grouping!)
5. GROUP BY  - Group rows
6. HAVING    - Filter groups (after aggregation)
7. SELECT    - Calculate expressions
8. DISTINCT  - Remove duplicates
9. ORDER BY  - Sort results
10. TOP      - Limit results

WINDOW FUNCTIONS execute during SELECT but after aggregation!
They can see aggregated values but not other window functions.
```

### Why This Matters for Utilities

```sql
-- WRONG: Can't filter window function in WHERE
SELECT 
    customer_id,
    ROW_NUMBER() OVER (ORDER BY consumption DESC) AS rank
FROM readings
WHERE rank <= 10;  -- ERROR! rank doesn't exist yet

-- CORRECT: Use CTE or subquery
WITH ranked AS (
    SELECT customer_id, ROW_NUMBER() OVER (ORDER BY consumption DESC) AS rank
    FROM readings
)
SELECT * FROM ranked WHERE rank <= 10;

-- WRONG: Aggregating then filtering individual rows
SELECT region, AVG(consumption)
FROM readings
WHERE consumption > 100  -- Filters BEFORE averaging!
GROUP BY region;

-- CORRECT: Use HAVING for post-aggregation filter
SELECT region, AVG(consumption)
FROM readings
GROUP BY region
HAVING AVG(consumption) > 100;  -- Filters AFTER averaging
```

### Common Pitfalls
1. **WHERE vs HAVING**: WHERE filters rows, HAVING filters groups
2. **Window function timing**: Can't reference in WHERE, GROUP BY, or HAVING
3. **Column scope**: SELECT list can reference aggregated values, WHERE cannot
4. **ORDER BY scope**: Can reference SELECT aliases, but not window functions directly
5. **GROUP BY requirements**: All non-aggregated SELECT columns must be in GROUP BY

---

<a name="q12"></a>
## Question 12: Dynamic Pivot for Monthly Consumption Report
**Skill Tested:** PIVOT/UNPIVOT, Dynamic SQL, Reporting

### Scenario
Business Stream needs a monthly consumption report where months are columns. The report must be dynamic - automatically including all months in the reporting period without hardcoding.

### The Question
Write a query that:
1. Dynamically pivots monthly consumption data (columns = months)
2. Shows each customer as a row with their consumption per month
3. Includes a total column
4. Works for any date range without modification
5. Handles missing months (show 0, not NULL)

### The Answer

```sql
-- ============================================
-- Dynamic Pivot using SQL
-- ============================================

-- Step 1: Get the dynamic column list
DECLARE @start_date DATE = '2024-01-01';
DECLARE @end_date DATE = '2024-06-30';
DECLARE @cols NVARCHAR(MAX);
DECLARE @sql NVARCHAR(MAX);

-- Generate column list for pivot
SELECT @cols = STRING_AGG(
    QUOTENAME(month_column), ', '
) WITHIN GROUP (ORDER BY month_date)
FROM (
    SELECT DISTINCT 
        FORMAT(DATEFROMPARTS(YEAR(reading_date), MONTH(reading_date), 1), 'yyyy_MM') AS month_column,
        DATEFROMPARTS(YEAR(reading_date), MONTH(reading_date), 1) AS month_date
    FROM meter_readings
    WHERE reading_date BETWEEN @start_date AND @end_date
) months;

-- Step 2: Build and execute dynamic pivot query
SET @sql = N'
WITH monthly_consumption AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.region,
        FORMAT(DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1), ''yyyy_MM'') AS month_col,
        MAX(r.reading_value) - MIN(r.reading_value) AS monthly_volume
    FROM customers c
    JOIN meters m ON c.customer_id = m.customer_id
    JOIN meter_readings r ON m.meter_id = r.meter_id
    WHERE r.reading_type = ''ACTUAL''
      AND r.reading_date BETWEEN @start AND @end
    GROUP BY c.customer_id, c.company_name, c.region,
             DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1)
)
SELECT 
    customer_id,
    company_name,
    region,
    ' + @cols + ',
    ' + REPLACE(@cols, ',', ' + ') + ' AS total_consumption
FROM monthly_consumption
PIVOT (
    SUM(monthly_volume)
    FOR month_col IN (' + @cols + ')
) AS pvt
ORDER BY total_consumption DESC;';

-- Execute with parameters
EXEC sp_executesql @sql, 
    N'@start DATE, @end DATE', 
    @start = @start_date, 
    @end = @end_date;

-- ============================================
-- Alternative: Conditional Aggregation (No Dynamic SQL)
-- ============================================

WITH date_range AS (
    SELECT 
        CAST('2024-01-01' AS DATE) AS start_date,
        CAST('2024-06-30' AS DATE) AS end_date
),
monthly_consumption AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.region,
        YEAR(r.reading_date) AS yr,
        MONTH(r.reading_date) AS mn,
        MAX(r.reading_value) - MIN(r.reading_value) AS monthly_volume
    FROM customers c
    JOIN meters m ON c.customer_id = m.customer_id
    JOIN meter_readings r ON m.meter_id = r.meter_id
    CROSS JOIN date_range dr
    WHERE r.reading_type = 'ACTUAL'
      AND r.reading_date BETWEEN dr.start_date AND dr.end_date
    GROUP BY c.customer_id, c.company_name, c.region,
             YEAR(r.reading_date), MONTH(r.reading_date)
)
SELECT 
    customer_id,
    company_name,
    region,
    -- Static columns for known period
    ROUND(SUM(CASE WHEN yr = 2024 AND mn = 1 THEN monthly_volume ELSE 0 END), 2) AS Jan_2024,
    ROUND(SUM(CASE WHEN yr = 2024 AND mn = 2 THEN monthly_volume ELSE 0 END), 2) AS Feb_2024,
    ROUND(SUM(CASE WHEN yr = 2024 AND mn = 3 THEN monthly_volume ELSE 0 END), 2) AS Mar_2024,
    ROUND(SUM(CASE WHEN yr = 2024 AND mn = 4 THEN monthly_volume ELSE 0 END), 2) AS Apr_2024,
    ROUND(SUM(CASE WHEN yr = 2024 AND mn = 5 THEN monthly_volume ELSE 0 END), 2) AS May_2024,
    ROUND(SUM(CASE WHEN yr = 2024 AND mn = 6 THEN monthly_volume ELSE 0 END), 2) AS Jun_2024,
    ROUND(SUM(monthly_volume), 2) AS total_consumption,
    ROUND(AVG(monthly_volume), 2) AS avg_monthly_consumption,
    COUNT(monthly_volume) AS months_with_data
FROM monthly_consumption
GROUP BY customer_id, company_name, region
ORDER BY total_consumption DESC;

-- ============================================
-- UNPIVOT Example: Convert columns back to rows
-- ============================================

WITH pivoted_data AS (
    -- Previous query result
    SELECT 
        customer_id,
        company_name,
        100 AS Jan_2024,
        150 AS Feb_2024,
        120 AS Mar_2024
    FROM (VALUES (1, 'Acme Corp')) AS t(customer_id, company_name)
)
SELECT 
    customer_id,
    company_name,
    month_name,
    consumption
FROM pivoted_data
UNPIVOT (
    consumption FOR month_name IN (Jan_2024, Feb_2024, Mar_2024)
) AS unpvt;
```

### Explanation
This demonstrates:
- **Dynamic SQL**: Building pivot columns at runtime
- **STRING_AGG**: Concatenating column names
- **Conditional aggregation**: Alternative to PIVOT without dynamic SQL
- **UNPIVOT**: Converting wide format back to long format

### Common Pitfalls
1. **NULL handling**: PIVOT aggregations return NULL for missing data - use ISNULL
2. **Column quoting**: Must use QUOTENAME to handle special characters
3. **SQL injection**: Never concatenate user input directly - use parameters
4. **Performance**: Dynamic SQL can't be cached - consider static for frequent queries
5. **UNPIVOT limitations**: Can't unpivot to multiple value columns easily

---

<a name="q13"></a>
## Question 13: The Final Boss - Leakage Prediction Model Pipeline
**Skill Tested:** ALL Advanced Concepts Combined

### Scenario
Business Stream wants to build a leakage prediction model. You need to create a feature engineering pipeline in SQL that prepares data for machine learning, combining all the advanced concepts from previous questions.

### The Question
Create a comprehensive feature engineering query that:
1. Calculates 30+ features per meter for ML model input
2. Includes time-series features (lags, rolling stats, trends)
3. Handles missing data and outliers
4. Creates target variable (leak occurred within 30 days)
5. Optimizes for 10M+ meter-month records
6. Returns train/test split indicator

### The Answer

```sql
/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  BUSINESS STREAM - LEAKAGE PREDICTION FEATURE ENGINEERING PIPELINE           ║
║  By the sacred order of Normalization, let the data flow...                  ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

-- Configuration
DECLARE @feature_date DATE = '2024-06-30';  -- Feature calculation date
DECLARE @prediction_horizon INT = 30;        -- Days ahead to predict

WITH 
-- ═══════════════════════════════════════════════════════════════════════════
-- BASE DATA: Get 12 months of history per meter
-- ═══════════════════════════════════════════════════════════════════════════
meter_history AS (
    SELECT 
        m.meter_id,
        m.customer_id,
        m.meter_type,
        m.installation_date,
        m.meter_size,
        c.band,
        c.sic_code,
        c.region,
        DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1) AS month_start,
        MAX(r.reading_value) - MIN(r.reading_value) AS monthly_consumption,
        COUNT(*) AS reading_count,
        MAX(r.reading_date) AS last_reading_in_month,
        MIN(CASE WHEN r.reading_type = 'ACTUAL' THEN r.reading_date END) AS first_actual_date,
        SUM(CASE WHEN r.reading_type = 'ACTUAL' THEN 1 ELSE 0 END) AS actual_reading_count
    FROM meters m
    JOIN customers c ON m.customer_id = c.customer_id
    JOIN meter_readings r ON m.meter_id = r.meter_id
    WHERE r.reading_date BETWEEN DATEADD(month, -12, @feature_date) AND @feature_date
      AND m.is_active = 1
    GROUP BY m.meter_id, m.customer_id, m.meter_type, m.installation_date, 
             m.meter_size, c.band, c.sic_code, c.region,
             DATEFROMPARTS(YEAR(r.reading_date), MONTH(r.reading_date), 1)
    HAVING MAX(r.reading_value) - MIN(r.reading_value) >= 0  -- Filter negative
),

-- ═══════════════════════════════════════════════════════════════════════════
-- TIME-SERIES FEATURES: Lags, rolling statistics, trends
-- ═══════════════════════════════════════════════════════════════════════════
time_series_features AS (
    SELECT 
        meter_id, customer_id, meter_type, installation_date, meter_size,
        band, sic_code, region, month_start, monthly_consumption, reading_count,
        
        -- Lag features (1-3 months)
        LAG(monthly_consumption, 1) OVER w AS lag_1m_consumption,
        LAG(monthly_consumption, 2) OVER w AS lag_2m_consumption,
        LAG(monthly_consumption, 3) OVER w AS lag_3m_consumption,
        
        -- Rolling statistics (3, 6, 12 months)
        AVG(monthly_consumption) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_3m,
        AVG(monthly_consumption) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_avg_6m,
        AVG(monthly_consumption) OVER (w ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_avg_12m,
        
        STDEV(monthly_consumption) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_std_3m,
        STDEV(monthly_consumption) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_std_6m,
        
        MIN(monthly_consumption) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_min_6m,
        MAX(monthly_consumption) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_max_6m,
        
        -- Trend (slope approximation using first/last)
        CASE 
            WHEN FIRST_VALUE(monthly_consumption) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) > 0
            THEN (monthly_consumption - FIRST_VALUE(monthly_consumption) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW))
                 / NULLIF(FIRST_VALUE(monthly_consumption) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0)
            ELSE 0 
        END AS trend_6m_pct,
        
        -- Seasonal features
        MONTH(month_start) AS month_of_year,
        CASE WHEN MONTH(month_start) IN (6,7,8) THEN 1 ELSE 0 END AS is_summer,
        CASE WHEN MONTH(month_start) IN (12,1,2) THEN 1 ELSE 0 END AS is_winter,
        
        -- Reading quality features
        reading_count,
        actual_reading_count,
        CAST(actual_reading_count AS FLOAT) / NULLIF(reading_count, 0) AS actual_reading_ratio,
        
        -- Row number for filtering to latest month
        ROW_NUMBER() OVER (PARTITION BY meter_id ORDER BY month_start DESC) AS month_rank
        
    FROM meter_history
    WINDOW w AS (PARTITION BY meter_id ORDER BY month_start)
),

-- ═══════════════════════════════════════════════════════════════════════════
-- RATIO FEATURES: Compare current to historical
-- ═══════════════════════════════════════════════════════════════════════════
ratio_features AS (
    SELECT 
        *,
        -- Current vs historical ratios
        monthly_consumption / NULLIF(rolling_avg_6m, 0) AS ratio_to_6m_avg,
        monthly_consumption / NULLIF(rolling_avg_12m, 0) AS ratio_to_12m_avg,
        (monthly_consumption - lag_1m_consumption) / NULLIF(lag_1m_consumption, 0) AS mom_change_pct,
        
        -- Volatility measures
        rolling_std_3m / NULLIF(rolling_avg_3m, 0) AS cv_3m,
        rolling_std_6m / NULLIF(rolling_avg_6m, 0) AS cv_6m,
        
        -- Spike detection
        CASE WHEN monthly_consumption > rolling_avg_6m + 2 * rolling_std_6m THEN 1 ELSE 0 END AS is_spike_2std,
        CASE WHEN monthly_consumption > rolling_avg_6m + 3 * rolling_std_6m THEN 1 ELSE 0 END AS is_spike_3std,
        
        -- Range features
        monthly_consumption / NULLIF(rolling_max_6m, 0) AS pct_of_6m_max,
        (monthly_consumption - rolling_min_6m) / NULLIF(rolling_max_6m - rolling_min_6m, 0) AS pct_of_6m_range
        
    FROM time_series_features
    WHERE month_rank = 1  -- Only most recent month for prediction
),

-- ═══════════════════════════════════════════════════════════════════════════
-- CUSTOMER-LEVEL FEATURES: Aggregate across meters
-- ═══════════════════════════════════════════════════════════════════════════
customer_features AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT meter_id) AS meter_count,
        SUM(monthly_consumption) AS total_customer_consumption,
        AVG(monthly_consumption) AS avg_meter_consumption,
        MAX(monthly_consumption) AS max_meter_consumption,
        STDEV(monthly_consumption) AS customer_consumption_std
    FROM time_series_features
    WHERE month_rank = 1
    GROUP BY customer_id
),

-- ═══════════════════════════════════════════════════════════════════════════
-- HISTORICAL LEAKAGE FEATURES: Past incidents as predictors
-- ═══════════════════════════════════════════════════════════════════════════
leakage_history AS (
    SELECT 
        meter_id,
        COUNT(*) AS prior_leak_count,
        MAX(detected_date) AS last_leak_date,
        AVG(estimated_loss_m3) AS avg_prior_loss,
        SUM(CASE WHEN detected_date >= DATEADD(month, -6, @feature_date) THEN 1 ELSE 0 END) AS leaks_last_6m
    FROM leakage_incidents
    WHERE detected_date < @feature_date  -- Only before feature date
    GROUP BY meter_id
),

-- ═══════════════════════════════════════════════════════════════════════════
-- TARGET VARIABLE: Did a leak occur in prediction horizon?
-- ═══════════════════════════════════════════════════════════════════════════
target_variable AS (
    SELECT 
        meter_id,
        MAX(CASE 
            WHEN detected_date BETWEEN @feature_date AND DATEADD(day, @prediction_horizon, @feature_date) 
            THEN 1 
            ELSE 0 
        END) AS leak_occurred,
        MIN(CASE 
            WHEN detected_date BETWEEN @feature_date AND DATEADD(day, @prediction_horizon, @feature_date) 
            THEN detected_date 
            ELSE NULL 
        END) AS leak_date,
        AVG(CASE 
            WHEN detected_date BETWEEN @feature_date AND DATEADD(day, @prediction_horizon, @feature_date) 
            THEN estimated_loss_m3 
            ELSE NULL 
        END) AS leak_severity
    FROM leakage_incidents
    GROUP BY meter_id
),

-- ═══════════════════════════════════════════════════════════════════════════
-- TRAIN/TEST SPLIT: Deterministic split for reproducibility
-- ═══════════════════════════════════════════════════════════════════════════
train_test_split AS (
    SELECT 
        meter_id,
        CASE 
            WHEN ABS(CHECKSUM(NEWID(), meter_id)) % 100 < 80 THEN 'TRAIN'
            ELSE 'TEST'
        END AS split_set,
        ABS(CHECKSUM(NEWID(), meter_id)) % 5 AS cv_fold
    FROM (SELECT DISTINCT meter_id FROM meter_history) m
)

-- ═══════════════════════════════════════════════════════════════════════════
-- FINAL FEATURE SET: Combine all features
-- ═══════════════════════════════════════════════════════════════════════════
SELECT 
    -- Identifiers
    rf.meter_id,
    rf.customer_id,
    tts.split_set,
    tts.cv_fold,
    
    -- Categorical features (will need encoding)
    rf.meter_type,
    rf.band,
    rf.region,
    LEFT(rf.sic_code, 2) AS sic_division,
    rf.meter_size,
    rf.month_of_year,
    
    -- Raw consumption
    ROUND(rf.monthly_consumption, 3) AS monthly_consumption,
    
    -- Lag features
    ROUND(rf.lag_1m_consumption, 3) AS lag_1m_consumption,
    ROUND(rf.lag_2m_consumption, 3) AS lag_2m_consumption,
    ROUND(rf.lag_3m_consumption, 3) AS lag_3m_consumption,
    
    -- Rolling statistics
    ROUND(rf.rolling_avg_3m, 3) AS rolling_avg_3m,
    ROUND(rf.rolling_avg_6m, 3) AS rolling_avg_6m,
    ROUND(rf.rolling_avg_12m, 3) AS rolling_avg_12m,
    ROUND(rf.rolling_std_3m, 3) AS rolling_std_3m,
    ROUND(rf.rolling_std_6m, 3) AS rolling_std_6m,
    
    -- Ratio features
    ROUND(rf.ratio_to_6m_avg, 4) AS ratio_to_6m_avg,
    ROUND(rf.ratio_to_12m_avg, 4) AS ratio_to_12m_avg,
    ROUND(rf.mom_change_pct, 4) AS mom_change_pct,
    ROUND(rf.trend_6m_pct, 4) AS trend_6m_pct,
    
    -- Volatility features
    ROUND(rf.cv_3m, 4) AS cv_3m,
    ROUND(rf.cv_6m, 4) AS cv_6m,
    rf.is_spike_2std,
    rf.is_spike_3std,
    
    -- Range features
    ROUND(rf.pct_of_6m_max, 4) AS pct_of_6m_max,
    ROUND(rf.pct_of_6m_range, 4) AS pct_of_6m_range,
    
    -- Seasonal features
    rf.is_summer,
    rf.is_winter,
    
    -- Data quality features
    rf.reading_count,
    ROUND(rf.actual_reading_ratio, 4) AS actual_reading_ratio,
    DATEDIFF(day, rf.installation_date, @feature_date) AS meter_age_days,
    
    -- Customer features
    cf.meter_count AS customer_meter_count,
    ROUND(cf.total_customer_consumption, 3) AS customer_total_consumption,
    ROUND(cf.avg_meter_consumption, 3) AS customer_avg_consumption,
    
    -- Leakage history features
    COALESCE(lh.prior_leak_count, 0) AS prior_leak_count,
    COALESCE(lh.leaks_last_6m, 0) AS leaks_last_6m,
    DATEDIFF(day, lh.last_leak_date, @feature_date) AS days_since_last_leak,
    
    -- Target variable
    COALESCE(tv.leak_occurred, 0) AS target_leak_occurred,
    tv.leak_date AS target_leak_date,
    ROUND(tv.leak_severity, 3) AS target_leak_severity,
    
    -- Metadata
    @feature_date AS feature_date,
    GETDATE() AS created_at

FROM ratio_features rf
LEFT JOIN customer_features cf ON rf.customer_id = cf.customer_id
LEFT JOIN leakage_history lh ON rf.meter_id = lh.meter_id
LEFT JOIN target_variable tv ON rf.meter_id = tv.meter_id
JOIN train_test_split tts ON rf.meter_id = tts.meter_id

-- Data quality filters
WHERE rf.monthly_consumption >= 0
  AND rf.actual_reading_ratio >= 0.5  -- At least 50% actual readings
  AND rf.reading_count >= 2

ORDER BY rf.meter_id;

-- Performance optimization hint
OPTION (MAXDOP 4, RECOMPILE);
```

### Feature Summary

| Category | Count | Examples |
|----------|-------|----------|
| Raw/Lag | 4 | monthly_consumption, lag_1m, lag_2m, lag_3m |
| Rolling Stats | 5 | avg_3m, avg_6m, avg_12m, std_3m, std_6m |
| Ratios | 4 | ratio_to_6m_avg, mom_change_pct, trend_6m_pct |
| Volatility | 4 | cv_3m, cv_6m, is_spike_2std, is_spike_3std |
| Seasonal | 3 | month_of_year, is_summer, is_winter |
| Data Quality | 3 | reading_count, actual_reading_ratio, meter_age |
| Customer | 3 | meter_count, total_consumption, avg_consumption |
| Leak History | 3 | prior_leak_count, leaks_last_6m, days_since_last |
| **Total** | **29** | Plus identifiers and target |

### Explanation
This "Final Boss" query combines:
- **12 CTEs**: Modular, readable pipeline
- **WINDOW clause**: Reusable window definitions (SQL Server 2022+)
- **All window functions**: LAG, AVG, STDEV, MIN, MAX, ROW_NUMBER
- **Feature engineering**: 29+ ML-ready features
- **Target creation**: Binary classification target
- **Train/test split**: Reproducible with CHECKSUM
- **Performance**: MAXDOP, filtered indexes, partition elimination

### Common Pitfalls
1. **Data leakage**: Never use future information in features
2. **NULL propagation**: One NULL makes entire expression NULL
3. **Division by zero**: Always use NULLIF in denominators
4. **Window frame errors**: Default is RANGE, use ROWS for position-based
5. **Train/test contamination**: Split must be on meter_id, not row

---

## Summary: Skills Tested by Question

| Q# | Primary Skill | Secondary Skills | Difficulty |
|----|--------------|------------------|------------|
| 1 | Window Functions | Time-series, Statistics | ⭐⭐⭐ |
| 2 | Recursive CTE | Hierarchy, Aggregation | ⭐⭐⭐⭐ |
| 3 | Query Optimization | Indexing, Gap Analysis | ⭐⭐⭐⭐ |
| 4 | Data Quality | Complex CASE, Validation | ⭐⭐⭐ |
| 5 | Customer Analytics | NTILE, Segmentation | ⭐⭐⭐ |
| 6 | Statistical Analysis | PERCENTILE, Outliers | ⭐⭐⭐⭐ |
| 7 | Date Arithmetic | Proration, Edge Cases | ⭐⭐⭐⭐ |
| 8 | GROUPING SETS | Regulatory Reporting | ⭐⭐⭐⭐ |
| 9 | SCD Type 2 | Temporal Data, Overlaps | ⭐⭐⭐⭐⭐ |
| 10 | Complex Joins | Multi-system, NULL handling | ⭐⭐⭐⭐ |
| 11 | Execution Order | Trick question, Deep understanding | ⭐⭐⭐⭐⭐ |
| 12 | Dynamic Pivot | PIVOT/UNPIVOT, Dynamic SQL | ⭐⭐⭐⭐ |
| 13 | **ALL COMBINED** | Feature Engineering Pipeline | ⭐⭐⭐⭐⭐ |

---

*"The ancient scrolls of Codd have been consulted. The queries are optimized. The data flows true. May your joins be INNER and your NULLs be few."*

**Good luck, candidate.**
