# Business Stream - Power BI & Data Visualization Interview Questions
## Data Scientist Role - Water Utilities Sector
### 13 Brutally Difficult Technical Questions

---

## QUESTION 1: Advanced DAX - Context Transition & Iterator Functions

### Business Scenario
Business Stream needs to calculate **"Revenue-Weighted Average Consumption per Customer Segment"** for their executive dashboard. This metric must account for:
- Different tariff bands (Domestic, SME, Large Commercial)
- Seasonal variations in consumption
- Revenue contribution weighting (higher revenue customers should have proportionally more influence on the average)

### The Challenge
Write a DAX measure that calculates the revenue-weighted average consumption, properly handling:
1. Context transition when iterating over customers
2. Filter context preservation across multiple tables
3. Division by zero protection
4. Currency conversion (GBP to reporting currency)

### Your DAX Solution Should Include

```dax
-- Base Measures
Total Revenue GBP = 
SUMX(
    'Fact Meter Readings',
    'Fact Meter Readings'[Consumption_m3] * RELATED('Dim Tariff'[Rate_per_m3_GBP])
)

Total Consumption = 
SUM('Fact Meter Readings'[Consumption_m3])

-- The Complex Measure: Revenue-Weighted Average Consumption
Revenue Weighted Avg Consumption = 
VAR RevenueByCustomer =
    ADDCOLUMNS(
        VALUES('Dim Customer'[CustomerID]),
        "@CustomerRevenue", [Total Revenue GBP],
        "@CustomerConsumption", CALCULATE([Total Consumption]),
        "@RevenueWeight", DIVIDE([Total Revenue GBP], [Total Revenue GBP ALL], 0)
    )
VAR WeightedSum =
    SUMX(
        RevenueByCustomer,
        [@CustomerConsumption] * [@RevenueWeight]
    )
VAR TotalWeight =
    SUMX(RevenueByCustomer, [@RevenueWeight])
VAR Result =
    DIVIDE(WeightedSum, TotalWeight, BLANK())
RETURN
    Result

-- Supporting measure for denominator
Total Revenue GBP ALL = 
CALCULATE(
    [Total Revenue GBP],
    REMOVEFILTERS('Dim Customer')
)

-- Alternative using AVERAGEX with proper context handling
Revenue Weighted Avg Consumption V2 = 
VAR CustomerData =
    SUMMARIZE(
        'Fact Meter Readings',
        'Dim Customer'[CustomerID],
        'Dim Customer'[CustomerSegment],
        "Revenue", [Total Revenue GBP],
        "Consumption", [Total Consumption]
    )
VAR TotalRevenue = SUMX(CustomerData, [Revenue])
RETURN
    DIVIDE(
        SUMX(
            CustomerData,
            [Consumption] * DIVIDE([Revenue], TotalRevenue, 0)
        ),
        1,
        BLANK()
    )
```

### Best Practice Explanation

**Why this matters:** Iterator functions (SUMX, AVERAGEX) create **row context**, but measures automatically add **context transition** converting row context to filter context. This is where most DAX developers fail.

**Common Mistakes:**
1. Using AVERAGEX directly on a column (loses weighting)
2. Forgetting CALCULATE() when referencing measures inside iterators
3. Not handling division by zero with DIVIDE()
4. Creating circular dependency with ALL() in wrong scope

**What Interviewers Are Testing:**
- Deep understanding of context transition mechanics
- Ability to work with virtual tables in DAX
- Knowledge of when to use SUMMARIZE vs ADDCOLUMNS vs SUMMARIZECOLUMNS
- Understanding of iterator performance implications

---

## QUESTION 2: Star Schema Design for Utility Data Warehouse

### Business Scenario
Business Stream needs to design a dimensional model for their new analytics platform. The system must handle:
- 2.5 million meter readings daily
- 500,000+ customer accounts
- Multiple water supply points per customer
- Historical tariff changes (SCD Type 2)
- Regulatory reporting requirements (OFWAT)
- Leakage detection analytics

### The Challenge
Design a complete star schema including:
1. All dimension tables with proper keys and attributes
2. Fact table(s) with appropriate grain
3. Handling of slowly changing dimensions
4. Support for both operational and analytical queries
5. Partitioning strategy for the fact table

### Your Schema Design

```sql
-- FACT TABLE: Meter Readings (Daily Grain)
CREATE TABLE Fact_Meter_Readings (
    ReadingKey BIGINT PRIMARY KEY,
    DateKey INT NOT NULL,              -- FK to Dim_Date
    TimeKey INT NOT NULL,              -- FK to Dim_Time
    CustomerKey INT NOT NULL,          -- FK to Dim_Customer (SCD2)
    MeterKey INT NOT NULL,             -- FK to Dim_Meter (SCD2)
    TariffKey INT NOT NULL,            -- FK to Dim_Tariff (SCD2)
    SupplyPointKey INT NOT NULL,       -- FK to Dim_Supply_Point
    
    -- Degenerate Dimensions
    ReadingReference VARCHAR(50),
    
    -- Measures
    Consumption_m3 DECIMAL(12,4),
    EstimatedConsumption_m3 DECIMAL(12,4),
    Revenue_GBP DECIMAL(12,2),
    StandingCharge_GBP DECIMAL(10,2),
    
    -- Leakage Detection
    IsLeakSuspected BIT,
    LeakConfidenceScore DECIMAL(5,4),
    
    -- Audit
    ETL_LoadDate DATETIME2,
    ETL_SourceSystem VARCHAR(50)
);

-- FACT TABLE: Monthly Customer Snapshot (Aggregated)
CREATE TABLE Fact_Customer_Monthly (
    CustomerMonthKey BIGINT PRIMARY KEY,
    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    
    -- Semi-additive Measures
    OpeningBalance_GBP DECIMAL(12,2),
    ClosingBalance_GBP DECIMAL(12,2),
    
    -- Fully-additive Measures
    TotalConsumption_m3 DECIMAL(12,4),
    TotalRevenue_GBP DECIMAL(12,2),
    PaymentReceived_GBP DECIMAL(12,2),
    
    -- Derived Metrics (for performance)
    AvgDailyConsumption_m3 DECIMAL(10,4),
    DaysSinceLastReading INT,
    
    -- Churn Prediction Features
    ConsumptionVariance DECIMAL(10,4),
    PaymentPatternScore DECIMAL(5,4)
);

-- DIMENSION: Customer (SCD Type 2)
CREATE TABLE Dim_Customer (
    CustomerKey INT PRIMARY KEY IDENTITY(1,1),
    CustomerID VARCHAR(20) NOT NULL,           -- Business Key
    CustomerSegment VARCHAR(50),                -- Domestic/SME/Large
    CustomerType VARCHAR(50),                   -- Standard/Key Account
    CompanyName VARCHAR(200),
    TradingName VARCHAR(200),
    
    -- Address (Type 1 - overwrite)
    Postcode VARCHAR(10),
    Region VARCHAR(50),
    OFWATRegion VARCHAR(50),                    -- Regulatory region
    
    -- SCD Type 2 Columns
    ValidFrom DATE NOT NULL,
    ValidTo DATE NOT NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    
    -- Audit
    ETL_LoadDate DATETIME2
);

-- DIMENSION: Meter (SCD Type 2 for replacements)
CREATE TABLE Dim_Meter (
    MeterKey INT PRIMARY KEY IDENTITY(1,1),
    MeterSerialNumber VARCHAR(50) NOT NULL,     -- Business Key
    MeterSize VARCHAR(20),
    MeterType VARCHAR(50),                      -- Standard/Smart/AMR
    Manufacturer VARCHAR(100),
    InstallationDate DATE,
    
    -- Smart Meter Specific
    IsSmartMeter BIT,
    CommunicationProtocol VARCHAR(50),
    LastCommunicationDate DATE,
    
    -- SCD Type 2
    ValidFrom DATE NOT NULL,
    ValidTo DATE NOT NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);

-- DIMENSION: Tariff (SCD Type 2 for rate changes)
CREATE TABLE Dim_Tariff (
    TariffKey INT PRIMARY KEY IDENTITY(1,1),
    TariffCode VARCHAR(20) NOT NULL,            -- Business Key
    TariffName VARCHAR(100),
    TariffCategory VARCHAR(50),                 -- Domestic/SME/Large
    
    -- Rate Components
    Rate_per_m3_GBP DECIMAL(10,4),
    StandingCharge_Daily_GBP DECIMAL(8,4),
    VatRate DECIMAL(5,4),
    
    -- OFWAT Classification
    OFWATTariffBand VARCHAR(50),
    
    -- SCD Type 2
    ValidFrom DATE NOT NULL,
    ValidTo DATE NOT NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);

-- DIMENSION: Date (Standard)
CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,                    -- YYYYMMDD format
    FullDate DATE NOT NULL,
    DayOfWeek TINYINT,
    DayName VARCHAR(10),
    WeekOfYear TINYINT,
    MonthNumber TINYINT,
    MonthName VARCHAR(10),
    Quarter TINYINT,
    Year INT,
    FiscalYear INT,                             -- OFWAT FY: Apr-Mar
    FiscalQuarter TINYINT,
    IsWeekend BIT,
    IsBankHoliday BIT,
    IsLeapYear BIT,
    DaysInMonth TINYINT
);

-- BRIDGE TABLE: Customer to Supply Point (Many-to-Many)
CREATE TABLE Bridge_Customer_SupplyPoint (
    CustomerKey INT NOT NULL,
    SupplyPointKey INT NOT NULL,
    RelationshipType VARCHAR(50),               -- Owner/Occupier/Landlord
    ValidFrom DATE NOT NULL,
    ValidTo DATE NOT NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    PRIMARY KEY (CustomerKey, SupplyPointKey, ValidFrom)
);
```

### Best Practice Explanation

**Why Star Schema for Power BI:**
- Power BI's VertiPaq engine is optimized for star schemas
- Bi-directional relationships work predictably
- Simpler DAX, better performance

**Key Design Decisions:**
1. **Two Fact Tables:** Daily grain for operational analysis, monthly snapshot for trend analysis
2. **SCD Type 2:** Essential for historical revenue analysis when tariffs change
3. **Bridge Table:** Handles complex customer-supply point relationships
4. **Degenerate Dimension:** ReadingReference stays in fact table (no separate dimension needed)

**What Interviewers Are Testing:**
- Understanding of dimensional modeling principles
- Knowledge of SCD types and when to use each
- Ability to design for both performance and flexibility
- Understanding of Power BI's relationship limitations (many-to-many handling)

---

## QUESTION 3: Row-Level Security for Multi-Tenant Dashboards

### Business Scenario
Business Stream serves multiple water wholesalers (e.g., Scottish Water, Thames Water) and needs a single Power BI dashboard that:
- Shows each wholesaler ONLY their own customers' data
- Allows Business Stream executives to see aggregated data across all wholesalers
- Supports regional managers who see multiple wholesalers
- Maintains audit trails of data access

### The Challenge
Implement a complete RLS solution including:
1. Security table design
2. Dynamic RLS DAX filters
3. Handling of "see all" permissions
4. Performance optimization for millions of rows
5. Testing and validation approach

### Your RLS Implementation

```dax
-- ============================================
-- SECURITY TABLE DESIGN
-- ============================================

-- Dim_UserSecurity table structure:
-- UserPrincipalName | WholesalerKey | RegionKey | AccessLevel | CanSeeAll
-- john@businessstream.co.uk | 1 | NULL | Executive | TRUE
-- sarah@businessstream.co.uk | 2 | 5 | Regional | FALSE
-- mike@scottishwater.co.uk | 2 | NULL | Wholesaler | FALSE

-- ============================================
-- RLS ROLE: WholesalerFilter
-- ============================================

-- Filter on Dim_Wholesaler
[WholesalerKey] IN 
    SELECTCOLUMNS(
        FILTER(
            'Dim_UserSecurity',
            'Dim_UserSecurity'[UserPrincipalName] = USERPRINCIPALNAME()
                || 'Dim_UserSecurity'[CanSeeAll] = TRUE
        ),
        "WholesalerKey", 'Dim_UserSecurity'[WholesalerKey]
    )
    ||
    ISBLANK(
        CALCULATE(
            COUNTROWS('Dim_UserSecurity'),
            'Dim_UserSecurity'[UserPrincipalName] = USERPRINCIPALNAME()
        )
    ) && USERPRINCIPALNAME() = "admin@businessstream.co.uk"

-- ============================================
-- RLS ROLE: RegionFilter (for regional managers)
-- ============================================

[RegionKey] IN
    SELECTCOLUMNS(
        FILTER(
            'Dim_UserSecurity',
            'Dim_UserSecurity'[UserPrincipalName] = USERPRINCIPALNAME()
                && NOT(ISBLANK('Dim_UserSecurity'[RegionKey]))
        ),
        "RegionKey", 'Dim_UserSecurity'[RegionKey]
    )
    ||
    CALCULATE(
        COUNTROWS(
            FILTER(
                'Dim_UserSecurity',
                'Dim_UserSecurity'[UserPrincipalName] = USERPRINCIPALNAME()
                    && ISBLANK('Dim_UserSecurity'[RegionKey])
            )
        ) > 0
    )

-- ============================================
-- RLS ROLE: CustomerSegmentFilter
-- ============================================

-- For account managers who only see specific customer segments
[CustomerSegment] IN
    SELECTCOLUMNS(
        FILTER(
            'Dim_UserSegmentAccess',
            'Dim_UserSegmentAccess'[UserPrincipalName] = USERPRINCIPALNAME()
        ),
        "Segment", 'Dim_UserSegmentAccess'[AllowedSegment]
    )
    ||
    CALCULATE(
        COUNTROWS(
            FILTER(
                'Dim_UserSecurity',
                'Dim_UserSecurity'[UserPrincipalName] = USERPRINCIPALNAME()
                    && 'Dim_UserSecurity'[CanSeeAll] = TRUE
            )
        ) > 0
    )
```

### Advanced RLS Patterns

```dax
-- ============================================
-- PATTERN: Hierarchical RLS (Manager sees team data)
-- ============================================

-- UserHierarchy table: UserID | ManagerID | Level
VAR CurrentUser = USERPRINCIPALNAME()
VAR UserHierarchyPath =
    PATH(
        LOOKUPVALUE('Dim_UserHierarchy'[UserID], 'Dim_UserHierarchy'[Email], CurrentUser),
        'Dim_UserHierarchy'[ManagerID]
    )
VAR AllowedUsers =
    PATHCONTAINS(
        UserHierarchyPath,
        'Dim_Customer'[AccountManagerID]
    )
RETURN
    AllowedUsers || PATHLENGTH(UserHierarchyPath) = 1  -- Top level sees all

-- ============================================
-- PATTERN: Time-Bound Access (temporary access)
-- ============================================

VAR CurrentUser = USERPRINCIPALNAME()
VAR HasValidAccess =
    CALCULATE(
        COUNTROWS('Dim_UserSecurity'),
        'Dim_UserSecurity'[UserPrincipalName] = CurrentUser,
        'Dim_UserSecurity'[AccessStartDate] <= TODAY(),
        'Dim_UserSecurity'[AccessEndDate] >= TODAY()
    ) > 0
RETURN
    HasValidAccess
```

### Performance Optimization for RLS

```dax
-- ============================================
-- OPTIMIZATION 1: Pre-calculate security mapping
-- ============================================

-- Create a calculated table that pre-computes user-to-data mappings
UserDataAccess = 
SELECTCOLUMNS(
    'Dim_UserSecurity',
    "UserPrincipalName", 'Dim_UserSecurity'[UserPrincipalName],
    "WholesalerKey", 'Dim_UserSecurity'[WholesalerKey],
    "RegionKey", 'Dim_UserSecurity'[RegionKey],
    "AccessLevel", 'Dim_UserSecurity'[AccessLevel]
)

-- ============================================
-- OPTIMIZATION 2: Use bi-directional filtering carefully
-- ============================================

-- In RLS, bi-directional filtering can cause performance issues
-- Use CROSSFILTER to control relationship direction

VAR AllowedCustomers =
    CALCULATETABLE(
        VALUES('Dim_Customer'[CustomerKey]),
        'Dim_UserSecurity'[UserPrincipalName] = USERPRINCIPALNAME(),
        CROSSFILTER('Fact_Meter_Readings'[CustomerKey], 'Dim_Customer'[CustomerKey], BOTH)
    )
RETURN
    [CustomerKey] IN AllowedCustomers

-- ============================================
-- OPTIMIZATION 3: Aggregation table for RLS
-- ============================================

-- Create aggregated fact table with security pre-applied
-- This reduces the number of rows RLS needs to filter

SecurityAggregatedMetrics = 
SUMMARIZECOLUMNS(
    'Dim_Customer'[WholesalerKey],
    'Dim_Customer'[RegionKey],
    'Dim_Date'[Year],
    'Dim_Date'[Month],
    "TotalConsumption", SUM('Fact_Meter_Readings'[Consumption_m3]),
    "TotalRevenue", SUM('Fact_Meter_Readings'[Revenue_GBP])
)
```

### Testing RLS in Power BI

```dax
-- ============================================
-- VALIDATION MEASURE: Check RLS is working
-- ============================================

RLS Validation = 
"Current User: " & USERPRINCIPALNAME() &
" | Visible Wholesalers: " &
CONCATENATEX(
    VALUES('Dim_Wholesaler'[WholesalerName]),
    'Dim_Wholesaler'[WholesalerName],
    ", "
) &
" | Row Count: " & COUNTROWS('Fact_Meter_Readings')

-- ============================================
-- VALIDATION MEASURE: Check for data leakage
-- ============================================

Data Leakage Check = 
VAR TotalRows = COUNTROWS(ALL('Fact_Meter_Readings'))
VAR VisibleRows = COUNTROWS('Fact_Meter_Readings')
VAR CurrentUser = USERPRINCIPALNAME()
VAR ExpectedRows =
    CALCULATE(
        COUNTROWS('Fact_Meter_Readings'),
        'Dim_UserSecurity'[UserPrincipalName] = CurrentUser
    )
RETURN
    IF(
        VisibleRows > ExpectedRows * 1.1,  -- 10% tolerance
        "WARNING: Potential data leakage",
        "OK: " & FORMAT(VisibleRows, "#,##0") & " of " & FORMAT(TotalRows, "#,##0") & " rows visible"
    )
```

### Best Practice Explanation

**Critical RLS Principles:**
1. **Never trust client-side filtering** - RLS happens server-side
2. **Test with realistic data volumes** - RLS can kill performance
3. **Use aggregation tables** for large datasets
4. **Audit access** - log who sees what

**Common RLS Mistakes:**
1. Using USERNAME() instead of USERPRINCIPALNAME() (breaks with UPN changes)
2. Complex DAX in RLS filters (evaluated for EVERY row)
3. Not testing with service principal accounts
4. Forgetting to handle BLANK() in security tables

**What Interviewers Are Testing:**
- Understanding of Power BI security architecture
- Ability to design scalable security models
- Knowledge of performance implications
- Awareness of compliance/audit requirements

---

## QUESTION 4: Performance Optimization for Large Datasets

### Business Scenario
Business Stream's meter reading fact table contains:
- 2.5 billion rows (10 years of daily readings)
- 50+ columns including geospatial data
- Multiple data sources (smart meters, manual readings, estimates)
- Real-time streaming requirements for smart meters
- Need for sub-second dashboard response times

### The Challenge
Design a comprehensive performance optimization strategy covering:
1. Data modeling optimizations
2. DAX optimization techniques
3. Aggregation strategies
4. Partitioning approach
5. Incremental refresh configuration

### Your Optimization Strategy

```dax
-- ============================================
-- 1. AGGREGATION TABLES (Auto-aggregations)
-- ============================================

-- Create aggregation table for common query patterns
Agg_Monthly_Customer_Metrics = 
SUMMARIZECOLUMNS(
    'Dim_Date'[Year],
    'Dim_Date'[Month],
    'Dim_Customer'[CustomerSegment],
    'Dim_Customer'[Region],
    'Dim_Wholesaler'[WholesalerName],
    "TotalConsumption", SUM('Fact_Meter_Readings'[Consumption_m3]),
    "TotalRevenue", SUM('Fact_Meter_Readings'[Revenue_GBP]),
    "AvgDailyConsumption", AVERAGE('Fact_Meter_Readings'[Consumption_m3]),
    "ReadingCount", COUNT('Fact_Meter_Readings'[ReadingKey]),
    "ActiveCustomers", DISTINCTCOUNT('Fact_Meter_Readings'[CustomerKey])
)

-- ============================================
-- 2. OPTIMIZED BASE MEASURES
-- ============================================

-- BAD: Forces full table scan
Total Revenue BAD = 
SUMX(
    'Fact_Meter_Readings',
    'Fact_Meter_Readings'[Consumption_m3] * RELATED('Dim_Tariff'[Rate_per_m3_GBP])
)

-- GOOD: Uses existing column if pre-calculated, or aggregation
Total Revenue OPTIMIZED = 
SUM('Fact_Meter_Readings'[Revenue_GBP])  -- Pre-calculated in ETL

-- BEST: Uses aggregation table when available
Total Revenue = 
IF(
    HASONEVALUE('Dim_Date'[Year]) && HASONEVALUE('Dim_Date'[Month]),
    SUM('Agg_Monthly_Customer_Metrics'[TotalRevenue]),
    SUM('Fact_Meter_Readings'[Revenue_GBP])
)

-- ============================================
-- 3. OPTIMIZED TIME INTELLIGENCE
-- ============================================

-- BAD: Calculates for each row individually
YTD Revenue BAD = 
CALCULATE(
    [Total Revenue],
    FILTER(
        ALL('Dim_Date'),
        'Dim_Date'[Year] = MAX('Dim_Date'[Year])
            && 'Dim_Date'[DateKey] <= MAX('Dim_Date'[DateKey])
    )
)

-- GOOD: Uses optimized DATESYTD
YTD Revenue OPTIMIZED = 
CALCULATE(
    [Total Revenue],
    DATESYTD('Dim_Date'[FullDate])
)

-- BEST: Pre-calculated in aggregation table with YTD flag
YTD Revenue BEST = 
CALCULATE(
    SUM('Agg_Monthly_Customer_Metrics'[TotalRevenue]),
    'Dim_Date'[IsYTDBoundary] = TRUE
)

-- ============================================
-- 4. OPTIMIZED DISTINCT COUNT
-- ============================================

-- BAD: Slow on large tables
Active Customers BAD = DISTINCTCOUNT('Fact_Meter_Readings'[CustomerKey])

-- GOOD: Use dimension table when possible
Active Customers GOOD = DISTINCTCOUNT('Dim_Customer'[CustomerKey])

-- BEST: Use aggregation table
Active Customers = MAX('Agg_Monthly_Customer_Metrics'[ActiveCustomers])

-- ============================================
-- 5. CONTEXT OPTIMIZATION PATTERNS
-- ============================================

-- Pattern: Check context before expensive calculation
Revenue with Context Check = 
IF(
    -- Quick check: if no customers selected, return blank
    ISEMPTY(VALUES('Dim_Customer'[CustomerKey])),
    BLANK(),
    -- Expensive calculation only when needed
    CALCULATE(
        [Total Revenue],
        'Fact_Meter_Readings'[IsEstimated] = FALSE
    )
)

-- Pattern: Use variables to avoid multiple evaluations
Revenue Variance = 
VAR CurrentPeriod = [Total Revenue]
VAR PreviousPeriod = [Total Revenue LY]
VAR Variance = CurrentPeriod - PreviousPeriod
VAR VariancePct = DIVIDE(Variance, PreviousPeriod, 0)
RETURN
    VariancePct

-- ============================================
-- 6. RELATIONSHIP OPTIMIZATION
-- ============================================

-- Use star schema - single direction relationships
-- Avoid bi-directional filtering unless absolutely necessary

-- If bi-directional is needed, use CROSSFILTER in measure
Revenue by Wholesaler = 
CALCULATE(
    [Total Revenue],
    CROSSFILTER('Dim_Customer'[WholesalerKey], 'Dim_Wholesaler'[WholesalerKey], BOTH)
)
```

### Incremental Refresh Configuration

```json
{
  "refreshPolicy": {
    "type": "Basic",
    "rollingWindow": {
      "months": 36
    },
    "incrementalGranularity": "Day",
    "partitionColumn": "ETL_LoadDate",
    "detectDataChanges": true,
    "onlyRefreshCompleteDays": true
  }
}
```

### DAX Query Performance Analysis

```dax
-- ============================================
-- PERFORMANCE DIAGNOSTICS MEASURE
-- ============================================

Query Diagnostics = 
VAR StartTime = NOW()
VAR Result = [Your Complex Measure]
VAR EndTime = NOW()
VAR Duration = DATEDIFF(StartTime, EndTime, MILLISECOND)
RETURN
    "Result: " & FORMAT(Result, "#,##0") & 
    " | Duration: " & FORMAT(Duration, "#,##0") & "ms"

-- ============================================
-- TABLE SIZE ANALYSIS
-- ============================================

Table Size Analysis = 
ADDCOLUMNS(
    ALLNOBLANKROW('Fact_Meter_Readings'),
    "RowSizeEstimate", 
    8 +  -- ReadingKey
    4 +  -- DateKey
    4 +  -- CustomerKey
    12 + -- Consumption (DECIMAL)
    12 + -- Revenue (DECIMAL)
    1    -- IsLeakSuspected (BIT)
)
```

### Best Practice Explanation

**Performance Hierarchy (fastest to slowest):**
1. Pre-aggregated columns in fact table
2. Aggregation tables (Auto-aggregations)
3. Simple aggregations (SUM, COUNT)
4. Iterator functions (SUMX, AVERAGEX)
5. Complex FILTER operations
6. Nested CALCULATE with context transition

**VertiPaq Engine Optimization:**
- Columnar storage means narrower tables = faster queries
- Dictionary encoding: fewer unique values = better compression
- Relationships use integer keys (smallest data type possible)

**What Interviewers Are Testing:**
- Understanding of VertiPaq engine internals
- Ability to diagnose performance bottlenecks
- Knowledge of aggregation strategies
- Experience with large-scale Power BI implementations

---

## QUESTION 5: Executive Dashboard Design Principles

### Business Scenario
Business Stream's CEO needs a dashboard that provides:
- At-a-glance view of company performance vs OFWAT targets
- Customer satisfaction trends
- Revenue and consumption KPIs
- Leakage and operational efficiency metrics
- Ability to drill down from high-level to specific issues

### The Challenge
Design an executive dashboard following:
1. Tufte's principles of data visualization
2. Cognitive load theory
3. Progressive disclosure pattern
4. Mobile-responsive considerations
5. Accessibility compliance (WCAG 2.1)

### Your Dashboard Design Specification

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                    BUSINESS STREAM - EXECUTIVE DASHBOARD                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  ┌─────────────────────────────────────────────────────────────────────┐     ║
║  │  KPI RIBBON (At-a-glance health indicators)                         │     ║
║  │                                                                     │     ║
║  │  [Revenue: £124.5M ▲ 3.2%]  [Consumption: 245M m³ ▼ 1.1%]         │     ║
║  │  [Customer Sat: 87% ▲ 2%]   [Leakage: 12.3% ▼ 0.5%]               │     ║
║  │  [OFWAT Target: ON TRACK]   [Active Customers: 502K ▲ 1.2%]       │     ║
║  └─────────────────────────────────────────────────────────────────────┘     ║
║                                                                               ║
║  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐    ║
║  │  REVENUE TREND              │  │  CONSUMPTION BY SEGMENT            │    ║
║  │  (Line chart - 24 months)   │  │  (Small multiples - 4 quadrants)   │    ║
║  │                             │  │                                     │    ║
║  │    £M  │                    │  │  [Domestic]  [SME]  [Large] [Gov]  │    ║
║  │   140  │         ╱╲         │  │   ┌───┐    ┌───┐   ┌───┐  ┌───┐   │    ║
║  │   120  │    ╱╲  ╱  ╲  ╱╲    │  │   │ ▲ │    │ ╱╲│   │╱╲╱│  │╲ ╱│   │    ║
║  │   100  │╱╲╱  ╲╱    ╲╱  ╲╱   │  │   └───┘    └───┘   └───┘  └───┘   │    ║
║  │        └────────────────    │  │   Trending Trending  Flat   Down   │    ║
║  └─────────────────────────────┘  └─────────────────────────────────────┘    ║
║                                                                               ║
║  ┌─────────────────────────────────────────────────────────────────────┐     ║
║  │  OFWAT PERFORMANCE MATRIX (Heatmap with targets)                    │     ║
║  │                                                                     │     ║
║  │  Metric          │ Q1  │ Q2  │ Q3  │ Q4  │ Target │ Status        │     ║
║  │  ────────────────┼─────┼─────┼─────┼─────┼────────┼────────────── │     ║
║  │  Leakage (Mld)   │ 45  │ 43  │ 42  │ 40  │  38    │ 🟡 Near       │     ║
║  │  Customer Sat    │ 84% │ 85% │ 86% │ 87% │  85%   │ 🟢 On Track   │     ║
║  │  Cost per Prop   │ £52 │ £51 │ £50 │ £49 │  £48   │ 🟢 On Track   │     ║
║  │  Complaints      │ 120 │ 115 │ 108 │ 95  │  100   │ 🟢 Exceeding  │     ║
║  └─────────────────────────────────────────────────────────────────────┘     ║
║                                                                               ║
║  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐    ║
║  │  ALERTS & ANOMALIES         │  │  QUICK ACTIONS                      │    ║
║  │  (Top 5 items requiring     │  │  (Drill-through buttons)            │    ║
║  │   executive attention)      │  │                                     │    ║
║  │                             │  │  [View Customer Detail]             │    ║
║  │  🔴 Leakage spike in Region │  │  [View Regional Breakdown]          │    ║
║  │     North (+15% vs target)  │  │  [Export OFWAT Report]              │    ║
║  │                             │  │  [Schedule Review Meeting]          │    ║
║  │  🟡 Revenue shortfall in    │  │                                     │    ║
║  │     SME segment (-£2.3M)    │  │                                     │    ║
║  │                             │  │                                     │    ║
║  │  🟢 Customer satisfaction   │  │                                     │    ║
║  │     exceeded target         │  │                                     │    ║
║  └─────────────────────────────┘  └─────────────────────────────────────┘    ║
║                                                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### Visual Design Principles Applied

```dax
-- ============================================
-- COLOR PALETTE (WCAG 2.1 AA Compliant)
-- ============================================

-- Primary: Business Stream Blue
-- #005EB8 (RGB: 0, 94, 184) - 7.2:1 contrast on white

-- Semantic Colors:
-- Success: #00703C (Green) - 5.8:1 contrast
-- Warning: #FF6B35 (Orange) - 3.2:1 contrast (use with care)
-- Alert:   #D4351C (Red) - 6.4:1 contrast
-- Info:    #1D70B8 (Blue) - 5.9:1 contrast

-- ============================================
-- KPI CARD MEASURE (with conditional formatting)
-- ============================================

KPI Revenue = 
VAR CurrentValue = [Total Revenue]
VAR PreviousValue = [Total Revenue LY]
VAR Variance = CurrentValue - PreviousValue
VAR VariancePct = DIVIDE(Variance, PreviousValue, 0)
VAR TrendIcon = IF(VariancePct > 0, "▲", "▼")
VAR TrendColor = IF(VariancePct > 0, "#00703C", "#D4351C")
RETURN
    CurrentValue

-- Conditional formatting rule for KPI card background
KPI Background Color = 
SWITCH(TRUE(),
    [VariancePct] >= 0.05, "#00703C",    -- Green: >5% growth
    [VariancePct] >= 0, "#85994B",        -- Light green: 0-5% growth
    [VariancePct] >= -0.05, "#FF6B35",    -- Orange: 0 to -5%
    "#D4351C"                             -- Red: <-5%
)

-- ============================================
-- SPARKLINE DATA PREPARATION
-- ============================================

Sparkline Revenue = 
VAR Last12Months =
    DATESINPERIOD(
        'Dim_Date'[FullDate],
        MAX('Dim_Date'[FullDate]),
        -12,
        MONTH
    )
RETURN
    CALCULATE(
        [Total Revenue],
        Last12Months
    )
```

### Progressive Disclosure Implementation

```dax
-- ============================================
-- BOOKMARK NAVIGATION MEASURE
-- ============================================

-- Level 1: Executive Summary (default view)
-- Level 2: Segment Detail (click on segment)
-- Level 3: Customer Detail (click on customer)

-- Bookmark action: Update a slicer that controls visible page level
Selected Detail Level = 
SELECTEDVALUE('Navigation'[Level], "Executive")

-- Show/Hide logic for visual containers
Show Executive View = [Selected Detail Level] = "Executive"
Show Segment View = [Selected Detail Level] = "Segment"
Show Customer View = [Selected Detail Level] = "Customer"
```

### Mobile-Responsive Design

```
MOBILE LAYOUT (Phone Portrait):
┌─────────────────────────┐
│  [KPI Ribbon - scroll]  │
│  [Revenue] [Consumption]│
│  [Satisfaction] [Leakage│
├─────────────────────────┤
│  [Revenue Trend]        │
│  (Simplified sparkline) │
├─────────────────────────┤
│  [Alerts - Top 3 only]  │
│  🔴 Leakage spike       │
│  🟡 Revenue shortfall   │
│  🟢 Satisfaction up     │
├─────────────────────────┤
│  [Quick Actions]        │
│  [Call] [Email] [Export]│
└─────────────────────────┘

-- DAX for mobile-optimized measures
Mobile Revenue Display = 
IF(
    ISMOBILE(),
    FORMAT([Total Revenue] / 1000000, "£0.0M"),
    FORMAT([Total Revenue], "£#,##0")
)
```

### Best Practice Explanation

**Tufte's Principles Applied:**
1. **Data-ink ratio:** Remove chart junk, maximize data-ink
2. **Small multiples:** Segment comparison using consistent scales
3. **Layering and separation:** Clear visual hierarchy
4. **Micro/macro readings:** Both overview and detail accessible

**Cognitive Load Theory:**
- **Intrinsic load:** Keep metrics familiar (industry standard KPIs)
- **Extraneous load:** Consistent colors, minimal decoration
- **Germane load:** Clear relationships between metrics

**What Interviewers Are Testing:**
- Understanding of visual perception principles
- Ability to balance completeness with simplicity
- Knowledge of accessibility standards
- Experience with executive communication

---

## QUESTION 6: Customer Churn Analysis Visualization

### Business Scenario
Business Stream needs to identify customers at risk of churning (switching to another retailer). Key indicators include:
- Declining consumption patterns
- Late payments or payment disputes
- Service complaints
- Contract renewal dates approaching
- Competitor activity in their region

### The Challenge
Create a comprehensive churn analysis including:
1. Churn risk scoring model in DAX
2. Visualization showing at-risk customers
3. Segmentation by risk level and value
4. Predictive indicators dashboard
5. Action-oriented recommendations

### Your Churn Analysis Solution

```dax
-- ============================================
-- CHURN RISK SCORING MODEL
-- ============================================

-- Component 1: Consumption Decline Score (0-25 points)
Churn Risk - Consumption Score = 
VAR Current3M = CALCULATE([Total Consumption], DATESINPERIOD('Dim_Date'[FullDate], TODAY(), -3, MONTH))
VAR Previous3M = CALCULATE([Total Consumption], DATESINPERIOD('Dim_Date'[FullDate], TODAY() - 90, -3, MONTH))
VAR DeclinePct = DIVIDE(Current3M - Previous3M, Previous3M, 0)
RETURN
    SWITCH(TRUE(),
        DeclinePct <= -0.30, 25,  -- >30% decline = high risk
        DeclinePct <= -0.20, 20,  -- 20-30% decline
        DeclinePct <= -0.10, 15,  -- 10-20% decline
        DeclinePct <= -0.05, 10,  -- 5-10% decline
        DeclinePct < 0, 5,        -- <5% decline
        0                         -- No decline or growth
    )

-- Component 2: Payment Behavior Score (0-25 points)
Churn Risk - Payment Score = 
VAR LatePayments = [Count of Late Payments Last 12M]
VAR AvgDaysLate = [Average Days Late]
RETURN
    SWITCH(TRUE(),
        LatePayments >= 4, 25,
        LatePayments >= 3, 20,
        LatePayments >= 2, 15,
        LatePayments >= 1, 10,
        AvgDaysLate > 7, 5,
        0
    )

-- Component 3: Complaint Score (0-20 points)
Churn Risk - Complaint Score = 
VAR Complaints12M = [Count of Complaints Last 12M]
VAR UnresolvedComplaints = [Count of Unresolved Complaints]
RETURN
    SWITCH(TRUE(),
        UnresolvedComplaints >= 2, 20,
        UnresolvedComplaints = 1, 15,
        Complaints12M >= 3, 15,
        Complaints12M = 2, 10,
        Complaints12M = 1, 5,
        0
    )

-- Component 4: Contract Risk Score (0-20 points)
Churn Risk - Contract Score = 
VAR DaysToRenewal = [Days Until Contract Renewal]
VAR IsInNoticePeriod = [Is In Notice Period]
RETURN
    SWITCH(TRUE(),
        IsInNoticePeriod, 20,
        DaysToRenewal <= 30, 15,
        DaysToRenewal <= 60, 10,
        DaysToRenewal <= 90, 5,
        0
    )

-- Component 5: Competitor Activity Score (0-10 points)
Churn Risk - Competitor Score = 
VAR CompetitorActivity = [Competitor Activity Score in Region]
RETURN
    MIN(CompetitorActivity, 10)

-- ============================================
-- OVERALL CHURN RISK SCORE
-- ============================================

Churn Risk Score = 
[Churn Risk - Consumption Score] +
[Churn Risk - Payment Score] +
[Churn Risk - Complaint Score] +
[Churn Risk - Contract Score] +
[Churn Risk - Competitor Score]

-- Categorized risk level
Churn Risk Level = 
SWITCH(TRUE(),
    [Churn Risk Score] >= 70, "Critical",
    [Churn Risk Score] >= 50, "High",
    [Churn Risk Score] >= 30, "Medium",
    [Churn Risk Score] >= 10, "Low",
    "Minimal"
)

-- ============================================
-- CUSTOMER LIFETIME VALUE (for prioritization)
-- ============================================

Customer Lifetime Value = 
VAR AvgMonthlyRevenue = CALCULATE(AVERAGEX(VALUES('Dim_Date'[Month]), [Total Revenue]), 'Dim_Date'[FullDate] >= TODAY() - 365)
VAR AvgMonthlyConsumption = CALCULATE(AVERAGEX(VALUES('Dim_Date'[Month]), [Total Consumption]), 'Dim_Date'[FullDate] >= TODAY() - 365)
VAR ExpectedTenureYears = SWITCH([CustomerSegment], "Domestic", 5, "SME", 3, "Large", 7, 4)
VAR GrossMargin = 0.25  -- 25% gross margin assumption
RETURN
    AvgMonthlyRevenue * 12 * ExpectedTenureYears * GrossMargin

-- ============================================
-- PRIORITY MATRIX: Risk x Value
-- ============================================

Churn Priority Score = 
[Churn Risk Score] * LOG([Customer Lifetime Value] + 1, 10)
```

### Churn Analysis Visualizations

```dax
-- ============================================
-- DATA FOR SCATTER PLOT (Risk vs Value)
-- ============================================

-- X-axis: Customer Lifetime Value
-- Y-axis: Churn Risk Score
-- Bubble size: Annual Revenue
-- Color: Churn Risk Level

-- ============================================
-- SEGMENTATION TABLE
-- ============================================

Churn Risk Segmentation = 
SUMMARIZECOLUMNS(
    'Dim_Customer'[CustomerSegment],
    'Dim_Customer'[Region],
    "At Risk Customers", 
        CALCULATE(
            DISTINCTCOUNT('Dim_Customer'[CustomerID]),
            [Churn Risk Level] IN {"Critical", "High"}
        ),
    "At Risk Revenue", 
        CALCULATE(
            [Total Revenue],
            [Churn Risk Level] IN {"Critical", "High"}
        ),
    "Avg Risk Score", 
        AVERAGEX(
            VALUES('Dim_Customer'[CustomerID]),
            [Churn Risk Score]
        )
)

-- ============================================
-- TREND ANALYSIS
-- ============================================

Churn Rate Trend = 
VAR CustomersAtStart = 
    CALCULATE(
        DISTINCTCOUNT('Dim_Customer'[CustomerID]),
        'Dim_Date'[FullDate] = MIN('Dim_Date'[FullDate])
    )
VAR CustomersChurned = 
    CALCULATE(
        DISTINCTCOUNT('Dim_Customer'[CustomerID]),
        'Dim_Customer'[ChurnDate] >= MIN('Dim_Date'[FullDate]),
        'Dim_Customer'[ChurnDate] <= MAX('Dim_Date'[FullDate])
    )
RETURN
    DIVIDE(CustomersChurned, CustomersAtStart, 0)
```

### Action-Oriented Recommendations

```dax
-- ============================================
-- RECOMMENDED ACTION MEASURE
-- ============================================

Recommended Retention Action = 
VAR RiskLevel = [Churn Risk Level]
VAR RiskScore = [Churn Risk Score]
VAR ConsumptionScore = [Churn Risk - Consumption Score]
VAR PaymentScore = [Churn Risk - Payment Score]
VAR ComplaintScore = [Churn Risk - Complaint Score]
VAR ContractScore = [Churn Risk - Contract Score]
VAR CLV = [Customer Lifetime Value]
RETURN
    SWITCH(TRUE(),
        -- Critical risk + high value = Executive intervention
        RiskLevel = "Critical" && CLV > 50000, "Schedule CEO call",
        
        -- High complaint score = Customer service escalation
        ComplaintScore >= 15, "Escalate to retention team",
        
        -- Contract expiring soon = Renewal offer
        ContractScore >= 15, "Prepare renewal offer",
        
        -- Payment issues = Payment plan discussion
        PaymentScore >= 20, "Offer payment plan",
        
        -- Consumption decline = Usage review
        ConsumptionScore >= 20, "Schedule usage review",
        
        -- Medium risk = Automated retention campaign
        RiskLevel = "Medium", "Include in retention campaign",
        
        -- Default
        "Monitor"
    )
```

### Best Practice Explanation

**Churn Analysis Best Practices:**
1. **Multi-factor scoring:** Single indicators are unreliable
2. **Weighted components:** Not all factors equally important
3. **Value-based prioritization:** Focus on high-value at-risk customers
4. **Actionable insights:** Every score should lead to action

**Common Mistakes:**
1. Using only one indicator (e.g., just payment history)
2. Not validating the model against actual churn
3. Ignoring seasonal patterns
4. Not updating scores regularly

**What Interviewers Are Testing:**
- Ability to translate business problem into analytical model
- Understanding of weighted scoring systems
- Knowledge of customer analytics best practices
- Experience with actionable dashboard design

---

## QUESTION 7: Leakage Detection Dashboard Components

### Business Scenario
Leakage (non-revenue water) is a critical KPI for water utilities. Business Stream needs to:
- Monitor leakage across their supply network
- Detect anomalous consumption patterns indicating leaks
- Track leakage reduction initiatives
- Report leakage metrics to OFWAT
- Prioritize leak detection and repair activities

### The Challenge
Create a leakage detection system including:
1. Leak detection algorithm in DAX
2. Confidence scoring for leak alerts
3. Visualization of leakage hotspots
4. OFWAT reporting compliance
5. Cost-benefit analysis of leak repairs

### Your Leakage Detection Solution

```dax
-- ============================================
-- LEAK DETECTION ALGORITHM
-- ============================================

-- Method 1: Statistical Outlier Detection
Leak Detection - Statistical = 
VAR CurrentConsumption = [Total Consumption]
VAR HistoricalAvg = 
    CALCULATE(
        AVERAGEX(
            DATESINPERIOD('Dim_Date'[FullDate], TODAY() - 365, -365, DAY),
            [Total Consumption]
        )
    )
VAR HistoricalStdDev = 
    CALCULATE(
        STDEVX.P(
            DATESINPERIOD('Dim_Date'[FullDate], TODAY() - 365, -365, DAY),
            [Total Consumption]
        )
    )
VAR ZScore = DIVIDE(CurrentConsumption - HistoricalAvg, HistoricalStdDev, 0)
RETURN
    IF(ZScore > 2, "Potential Leak", "Normal")

-- Method 2: Night Flow Analysis (most accurate for distribution leaks)
Leak Detection - Night Flow = 
VAR NightFlow = 
    CALCULATE(
        [Total Consumption],
        'Dim_Time'[Hour] >= 2 && 'Dim_Time'[Hour] <= 4,  -- 2-4 AM
        'Dim_Date'[IsWeekend] = FALSE  -- Weekdays only
    )
VAR MinNightFlow = 
    CALCULATE(
        MINX(
            DATESINPERIOD('Dim_Date'[FullDate], TODAY() - 90, -90, DAY),
            CALCULATE([Total Consumption], 'Dim_Time'[Hour] >= 2 && 'Dim_Time'[Hour] <= 4)
        )
    )
VAR NightFlowIncrease = DIVIDE(NightFlow - MinNightFlow, MinNightFlow, 0)
RETURN
    IF(NightFlowIncrease > 0.20, "Leak Suspected", "Normal")

-- Method 3: Continuous Flow Detection (for smart meters)
Leak Detection - Continuous Flow = 
VAR HourlyReadings = 
    CALCULATE(
        COUNTROWS('Fact_Meter_Readings'),
        'Fact_Meter_Readings'[Consumption_m3] > 0
    )
VAR TotalHours = 
    DATEDIFF(MIN('Fact_Meter_Readings'[ReadingDateTime]), MAX('Fact_Meter_Readings'[ReadingDateTime]), HOUR)
VAR FlowContinuity = DIVIDE(HourlyReadings, TotalHours, 0)
RETURN
    IF(FlowContinuity > 0.95, "Continuous Flow - Possible Leak", "Intermittent Flow - Normal")

-- ============================================
-- COMPOSITE LEAK DETECTION SCORE
-- ============================================

Leak Confidence Score = 
VAR StatScore = IF([Leak Detection - Statistical] = "Potential Leak", 30, 0)
VAR NightScore = IF([Leak Detection - Night Flow] = "Leak Suspected", 40, 0)
VAR ContScore = IF([Leak Detection - Continuous Flow] = "Continuous Flow - Possible Leak", 30, 0)
VAR TotalScore = StatScore + NightScore + ContScore
RETURN
    TotalScore

Leak Alert Level = 
SWITCH(TRUE(),
    [Leak Confidence Score] >= 80, "Critical - Immediate Action",
    [Leak Confidence Score] >= 60, "High - Investigate Within 24h",
    [Leak Confidence Score] >= 40, "Medium - Investigate Within Week",
    [Leak Confidence Score] >= 20, "Low - Monitor Closely",
    "None"
)

-- ============================================
-- LEAKAGE VOLUME ESTIMATION
-- ============================================

Estimated Leak Volume m3 = 
VAR ExpectedConsumption = 
    CALCULATE(
        AVERAGEX(
            DATESINPERIOD('Dim_Date'[FullDate], TODAY() - 365, -365, DAY),
            [Total Consumption]
        )
    )
VAR ActualConsumption = [Total Consumption]
VAR ExcessVolume = MAX(ActualConsumption - ExpectedConsumption, 0)
VAR LeakProbability = [Leak Confidence Score] / 100
RETURN
    ExcessVolume * LeakProbability

-- ============================================
-- FINANCIAL IMPACT OF LEAKS
-- ============================================

Leak Cost GBP = 
VAR LeakVolume = [Estimated Leak Volume m3]
VAR ProductionCostPerM3 = 0.85  -- Cost to produce/treat water
VAR RevenueLossPerM3 = AVERAGEX('Dim_Tariff', 'Dim_Tariff'[Rate_per_m3_GBP])
VAR DailyLeakCost = LeakVolume * (ProductionCostPerM3 + RevenueLossPerM3)
RETURN
    DailyLeakCost

-- ============================================
-- OFWAT LEAKAGE REPORTING METRICS
-- ============================================

-- Mld = Megalitres per day
OFWAT Leakage Mld = 
VAR LeakVolumeM3 = SUM('Fact_Meter_Readings'[EstimatedLeakVolume_m3])
VAR DaysInPeriod = DISTINCTCOUNT('Dim_Date'[DateKey])
VAR LeakageMld = DIVIDE(LeakVolumeM3 / 1000, DaysInPeriod, 0)
RETURN
    LeakageMld

-- Leakage as % of System Input
OFWAT Leakage Percentage = 
VAR LeakageVolume = SUM('Fact_Meter_Readings'[EstimatedLeakVolume_m3])
VAR SystemInput = SUM('Fact_Meter_Readings'[SystemInput_m3])
RETURN
    DIVIDE(LeakageVolume, SystemInput, 0)

-- Performance against OFWAT target
OFWAT Leakage Performance = 
VAR ActualLeakage = [OFWAT Leakage Mld]
VAR TargetLeakage = MAX('Dim_OFWAT_Targets'[LeakageTarget_Mld])
VAR Variance = ActualLeakage - TargetLeakage
RETURN
    SWITCH(TRUE(),
        Variance <= 0, "Exceeding Target",
        Variance <= TargetLeakage * 0.05, "On Track",
        Variance <= TargetLeakage * 0.10, "At Risk",
        "Off Track"
    )
```

### Leakage Hotspot Analysis

```dax
-- ============================================
-- DMA (District Metered Area) LEAKAGE ANALYSIS
-- ============================================

DMA Leakage Ranking = 
SUMMARIZECOLUMNS(
    'Dim_DMA'[DMA_Code],
    'Dim_DMA'[DMA_Name],
    'Dim_DMA'[Region],
    "Leakage Mld", [OFWAT Leakage Mld],
    "Leakage %", [OFWAT Leakage Percentage],
    "Alert Count", 
        CALCULATE(
            COUNTROWS('Fact_Meter_Readings'),
            [Leak Alert Level] <> "None"
        ),
    "Avg Confidence", AVERAGEX('Fact_Meter_Readings', [Leak Confidence Score]),
    "Financial Impact", SUMX('Fact_Meter_Readings', [Leak Cost GBP])
)

-- ============================================
-- LEAK AGE ANALYSIS (how long leaks have been active)
-- ============================================

Average Leak Age Days = 
VAR LeakStartDates = 
    CALCULATETABLE(
        VALUES('Fact_Meter_Readings'[LeakDetectedDate]),
        'Fact_Meter_Readings'[IsLeakSuspected] = TRUE
    )
RETURN
    AVERAGEX(
        LeakStartDates,
        DATEDIFF('Fact_Meter_Readings'[LeakDetectedDate], TODAY(), DAY)
    )

-- ============================================
-- REPAIR PRIORITIZATION MATRIX
-- ============================================

Repair Priority Score = 
VAR LeakConfidence = [Leak Confidence Score]
VAR LeakCost = [Leak Cost GBP]
VAR CustomerImpact = [Customers Affected]
VAR InfrastructureAge = [Pipe Age Years]
VAR RepairComplexity = [Estimated Repair Complexity Score]  -- 1-10
RETURN
    (LeakConfidence * 0.3) +
    (LOG(LeakCost + 1) * 10 * 0.3) +
    (CustomerImpact * 0.2) +
    (InfrastructureAge * 0.1) -
    (RepairComplexity * 0.1)
```

### Best Practice Explanation

**Leak Detection Best Practices:**
1. **Multi-method approach:** No single method is foolproof
2. **Night flow analysis:** Most reliable for distribution leaks
3. **Statistical baselines:** Account for seasonal variations
4. **Confidence scoring:** Prioritize investigations

**OFWAT Reporting Requirements:**
- Leakage reported in Mld (megalitres per day)
- Annual performance commitment reporting
- Independent assurance of leakage estimates

**What Interviewers Are Testing:**
- Domain knowledge of water utility operations
- Ability to design multi-factor detection algorithms
- Understanding of regulatory reporting requirements
- Experience with operational dashboards

---

## QUESTION 8: Regulatory Compliance Reporting (OFWAT)

### Business Scenario
Business Stream must comply with OFWAT (Water Services Regulation Authority) reporting requirements, including:
- Annual performance commitment reporting
- Customer satisfaction metrics (C-MEX)
- Developer services performance
- Financial performance reporting
- Market share and competition metrics

### The Challenge
Create a comprehensive OFWAT compliance reporting solution:
1. Automated calculation of all OFWAT KPIs
2. Historical trend analysis against targets
3. Exception reporting for off-target metrics
4. Audit trail for regulatory submissions
5. Self-service exploration for regulatory team

### Your OFWAT Reporting Solution

```dax
-- ============================================
-- OFWAT PERFORMANCE COMMITMENTS (PCs)
-- ============================================

-- PC1: Customer Satisfaction (C-MEX Score)
OFWAT PC1 Customer Satisfaction = 
VAR C_MEX_Score = AVERAGE('Fact_Customer_Survey'[C_MEX_Score])
VAR Target = MAX('Dim_OFWAT_Targets'[PC1_Target])
VAR OutperformanceThreshold = MAX('Dim_OFWAT_Targets'[PC1_Outperformance])
VAR UnderperformanceThreshold = MAX('Dim_OFWAT_Targets'[PC1_Underperformance])
RETURN
    SWITCH(TRUE(),
        C_MEX_Score >= OutperformanceThreshold, "Outperforming",
        C_MEX_Score >= Target, "Meeting Target",
        C_MEX_Score >= UnderperformanceThreshold, "At Risk",
        "Underperforming"
    )

-- PC2: Leakage Performance
OFWAT PC2 Leakage Status = 
VAR ActualLeakage = [OFWAT Leakage Mld]
VAR TargetLeakage = MAX('Dim_OFWAT_Targets'[PC2_LeakageTarget_Mld])
VAR OutperformanceThreshold = TargetLeakage * 0.95  -- 5% better than target
VAR UnderperformanceThreshold = TargetLeakage * 1.05  -- 5% worse than target
RETURN
    SWITCH(TRUE(),
        ActualLeakage <= OutperformanceThreshold, "Outperforming",
        ActualLeakage <= TargetLeakage, "Meeting Target",
        ActualLeakage <= UnderperformanceThreshold, "At Risk",
        "Underperforming"
    )

-- PC3: Per Capita Consumption (PCC)
OFWAT PC3 PCC = 
VAR TotalConsumption = SUM('Fact_Meter_Readings'[Consumption_m3])
VAR Population = SUM('Dim_Region'[Population])
VAR PCC = DIVIDE(TotalConsumption * 1000, Population, 0)  -- Litres per person per day
VAR Target = MAX('Dim_OFWAT_Targets'[PC3_PCC_Target])
RETURN
    PCC

-- PC4: Supply Interruptions
OFWAT PC4 Supply Interruptions = 
VAR InterruptionsOver12Hrs = 
    COUNTROWS(
        FILTER(
            'Fact_Service_Incidents',
            'Fact_Service_Incidents'[Duration_Hours] > 12
        )
    )
VAR PropertiesAffected = SUM('Fact_Service_Incidents'[Properties_Affected])
VAR InterruptionsPerProperty = DIVIDE(InterruptionsOver12Hrs, PropertiesAffected, 0) * 1000
RETURN
    InterruptionsPerProperty

-- PC5: Internal Sewer Flooding
OFWAT PC5 Sewer Flooding = 
VAR FloodingIncidents = COUNTROWS('Fact_Sewer_Incidents')
VAR PropertiesAtRisk = SUM('Dim_Region'[Properties_Connected])
VAR FloodingRate = DIVIDE(FloodingIncidents, PropertiesAtRisk, 0) * 10000
RETURN
    FloodingRate

-- PC6: Pollution Incidents
OFWAT PC6 Pollution Incidents = 
VAR Category12 = COUNTROWS(FILTER('Fact_Pollution_Incidents', [Category] <= 2))
VAR Category3 = COUNTROWS(FILTER('Fact_Pollution_Incidents', [Category] = 3))
RETURN
    Category12 * 10 + Category3  -- Weighted score

-- ============================================
-- OFWAT FINANCIAL METRICS
-- ============================================

-- Revenue by Wholesale Area
OFWAT Revenue by Wholesale Area = 
SUMMARIZECOLUMNS(
    'Dim_Wholesaler'[Wholesale_Area],
    "Revenue GBP", SUM('Fact_Meter_Readings'[Revenue_GBP]),
    "Revenue per Property", 
        DIVIDE(
            SUM('Fact_Meter_Readings'[Revenue_GBP]),
            SUM('Dim_Region'[Properties_Connected]),
            0
        )
)

-- Cost to Serve
OFWAT Cost to Serve = 
VAR OperationalCosts = SUM('Fact_Financials'[Operational_Costs])
VAR CustomerCount = DISTINCTCOUNT('Dim_Customer'[CustomerID])
RETURN
    DIVIDE(OperationalCosts, CustomerCount, 0)

-- Bad Debt Ratio
OFWAT Bad Debt Ratio = 
VAR BadDebtProvision = SUM('Fact_Financials'[Bad_Debt_Provision])
VAR TotalRevenue = SUM('Fact_Financials'[Revenue])
RETURN
    DIVIDE(BadDebtProvision, TotalRevenue, 0)
```

### OFWAT Exception Reporting

```dax
-- ============================================
-- EXCEPTION REPORT: Metrics Off Target
-- ============================================

OFWAT Exception Report = 
VAR PerformanceData =
    ADDCOLUMNS(
        {
            ("PC1 Customer Satisfaction", [OFWAT PC1 Customer Satisfaction]),
            ("PC2 Leakage", [OFWAT PC2 Leakage Status]),
            ("PC3 PCC", [OFWAT PC3 PCC]),
            ("PC4 Supply Interruptions", [OFWAT PC4 Supply Interruptions]),
            ("PC5 Sewer Flooding", [OFWAT PC5 Sewer Flooding]),
            ("PC6 Pollution", [OFWAT PC6 Pollution Incidents])
        },
        "Status", [Value2],
        "Requires Action", [Value2] IN {"At Risk", "Underperforming"}
    )
RETURN
    FILTER(PerformanceData, [Requires Action] = TRUE)

-- ============================================
-- TREND ANALYSIS: Performance Over Time
-- ============================================

OFWAT Performance Trend = 
VAR CurrentYear = YEAR(TODAY())
VAR Years = GENERATESERIES(CurrentYear - 4, CurrentYear, 1)
RETURN
    ADDCOLUMNS(
        Years,
        "PC1 Score", CALCULATE(AVERAGE('Fact_Customer_Survey'[C_MEX_Score]), 'Dim_Date'[Year] = [Value]),
        "PC2 Leakage", CALCULATE([OFWAT Leakage Mld], 'Dim_Date'[Year] = [Value]),
        "PC3 PCC", CALCULATE([OFWAT PC3 PCC], 'Dim_Date'[Year] = [Value]),
        "Overall Rating", 
            SWITCH(TRUE(),
                [PC1 Score] >= 85 && [PC2 Leakage] < 40, "Excellent",
                [PC1 Score] >= 80 && [PC2 Leakage] < 45, "Good",
                [PC1 Score] >= 75 && [PC2 Leakage] < 50, "Satisfactory",
                "Needs Improvement"
            )
    )

-- ============================================
-- AUDIT TRAIL FOR REGULATORY SUBMISSIONS
-- ============================================

Regulatory Submission Audit = 
VAR SubmissionDate = MAX('Fact_Regulatory_Submissions'[SubmissionDate])
VAR SubmittedBy = MAX('Fact_Regulatory_Submissions'[SubmittedBy])
VAR MetricsIncluded = CONCATENATEX(VALUES('Fact_Regulatory_Submissions'[MetricName]), [MetricName], ", ")
VAR DataVersion = MAX('Fact_Regulatory_Submissions'[DataVersion])
RETURN
    "Submission on " & FORMAT(SubmissionDate, "dd/mm/yyyy") &
    " by " & SubmittedBy &
    " | Metrics: " & MetricsIncluded &
    " | Data Version: " & DataVersion
```

### Self-Service Regulatory Exploration

```dax
-- ============================================
-- DYNAMIC METRIC SELECTION
-- ============================================

-- Create a disconnected table for metric selection
-- MetricName | MetricMeasure
-- "Customer Satisfaction" | [OFWAT PC1 Customer Satisfaction]
-- "Leakage" | [OFWAT PC2 Leakage Status]

Selected OFWAT Metric = 
VAR SelectedMetric = SELECTEDVALUE('Metric_Selection'[MetricName])
RETURN
    SWITCH(SelectedMetric,
        "Customer Satisfaction", [OFWAT PC1 Customer Satisfaction],
        "Leakage", [OFWAT PC2 Leakage Status],
        "PCC", [OFWAT PC3 PCC],
        "Supply Interruptions", [OFWAT PC4 Supply Interruptions],
        BLANK()
    )

-- ============================================
-- COMPARISON ANALYSIS
-- ============================================

OFWAT Peer Comparison = 
VAR MyPerformance = [Selected OFWAT Metric]
VAR PeerAverage = CALCULATE(AVERAGE('Fact_Peer_Data'[MetricValue]), ALL('Dim_Company'))
VAR IndustryBest = CALCULATE(MAX('Fact_Peer_Data'[MetricValue]), ALL('Dim_Company'))
VAR IndustryWorst = CALCULATE(MIN('Fact_Peer_Data'[MetricValue]), ALL('Dim_Company'))
RETURN
    SWITCH(TRUE(),
        MyPerformance >= IndustryBest, "Industry Leading",
        MyPerformance >= PeerAverage, "Above Average",
        MyPerformance >= (PeerAverage + IndustryWorst) / 2, "Below Average",
        "Industry Laggard"
    )
```

### Best Practice Explanation

**OFWAT Reporting Best Practices:**
1. **Version control:** Track every regulatory submission
2. **Audit trail:** Document data sources and transformations
3. **Exception focus:** Highlight off-target metrics immediately
4. **Peer comparison:** Contextualize performance

**Common Compliance Mistakes:**
1. Using wrong calculation methodology
2. Missing data quality checks
3. Not documenting assumptions
4. Late submissions due to manual processes

**What Interviewers Are Testing:**
- Understanding of UK water industry regulation
- Ability to design compliance-focused reporting
- Knowledge of audit and governance requirements
- Experience with regulatory stakeholder management

---

## QUESTION 9: Slowly Changing Dimensions in DAX

### Business Scenario
Business Stream's customer, tariff, and meter dimensions change over time:
- Customers change segments (SME grows to Large)
- Tariff rates change annually
- Meters are replaced with smart meters
- Account managers change

### The Challenge
Implement proper SCD handling for:
1. SCD Type 1 (overwrite) - address changes
2. SCD Type 2 (historical tracking) - tariff changes
3. SCD Type 3 (limited history) - previous segment
4. Point-in-time analysis in DAX
5. Correct aggregation across SCD versions

### Your SCD Implementation

```dax
-- ============================================
-- SCD TYPE 2: POINT-IN-TIME LOOKUP
-- ============================================

-- Get the correct dimension version for a specific date
Customer Version at Date = 
VAR TargetDate = SELECTEDVALUE('Date_Selection'[AsOfDate])
VAR CurrentCustomerKey = SELECTEDVALUE('Dim_Customer'[CustomerKey])
RETURN
    CALCULATE(
        MAX('Dim_Customer'[CustomerKey]),
        'Dim_Customer'[CustomerID] = RELATED('Dim_Customer'[CustomerID]),
        'Dim_Customer'[ValidFrom] <= TargetDate,
        'Dim_Customer'[ValidTo] >= TargetDate
    )

-- ============================================
-- REVENUE AT HISTORICAL TARIFF RATES
-- ============================================

-- Calculate what revenue WOULD have been at previous tariff
Revenue at Historical Tariff = 
VAR TargetDate = DATE(2023, 4, 1)  -- Previous tariff period
VAR Consumption = [Total Consumption]
VAR HistoricalRate =
    CALCULATE(
        MAX('Dim_Tariff'[Rate_per_m3_GBP]),
        'Dim_Tariff'[ValidFrom] <= TargetDate,
        'Dim_Tariff'[ValidTo] >= TargetDate,
        ALL('Dim_Tariff')  -- Remove current filter
    )
RETURN
    Consumption * HistoricalRate

-- Revenue variance due to tariff change
Tariff Impact on Revenue = 
[Total Revenue] - [Revenue at Historical Tariff]

-- ============================================
-- CUSTOMER SEGMENT TRANSITION ANALYSIS
-- ============================================

-- Track customers who changed segments
Customer Segment Changes = 
VAR CurrentSegment = MAX('Dim_Customer'[CustomerSegment])
VAR PreviousSegment =
    CALCULATE(
        MAX('Dim_Customer'[CustomerSegment]),
        'Dim_Customer'[CustomerID] = MAX('Dim_Customer'[CustomerID]),
        'Dim_Customer'[ValidTo] < TODAY(),
        'Dim_Customer'[IsCurrent] = FALSE
    )
RETURN
    IF(
        NOT(ISBLANK(PreviousSegment)) && CurrentSegment <> PreviousSegment,
        PreviousSegment & " → " & CurrentSegment,
        "No Change"
    )

-- Count of segment transitions
Segment Transition Count = 
COUNTROWS(
    FILTER(
        VALUES('Dim_Customer'[CustomerID]),
        [Customer Segment Changes] <> "No Change"
    )
)

-- ============================================
-- CORRECT AGGREGATION ACROSS SCD VERSIONS
-- ============================================

-- WRONG: Counts each version separately
Customer Count WRONG = DISTINCTCOUNT('Dim_Customer'[CustomerKey])

-- CORRECT: Counts unique business keys
Customer Count CORRECT = DISTINCTCOUNT('Dim_Customer'[CustomerID])

-- CORRECT: Count current customers only
Current Customer Count = 
CALCULATE(
    DISTINCTCOUNT('Dim_Customer'[CustomerID]),
    'Dim_Customer'[IsCurrent] = TRUE
)

-- ============================================
-- TIME-INTELLIGENCE WITH SCD
-- ============================================

-- Revenue for customers who existed in prior period
Same Store Revenue = 
VAR CurrentPeriodCustomers = VALUES('Dim_Customer'[CustomerID])
VAR PriorPeriodCustomers =
    CALCULATETABLE(
        VALUES('Dim_Customer'[CustomerID]),
        SAMEPERIODLASTYEAR('Dim_Date'[FullDate])
    )
VAR CommonCustomers = INTERSECT(CurrentPeriodCustomers, PriorPeriodCustomers)
RETURN
    CALCULATE(
        [Total Revenue],
        CommonCustomers
    )

-- Revenue growth excluding new/lost customers
Organic Revenue Growth = 
VAR CurrentSameStore = [Same Store Revenue]
VAR PriorSameStore =
    CALCULATE(
        [Same Store Revenue],
        SAMEPERIODLASTYEAR('Dim_Date'[FullDate])
    )
RETURN
    DIVIDE(CurrentSameStore - PriorSameStore, PriorSameStore, 0)
```

### Advanced SCD Patterns

```dax
-- ============================================
-- SCD TYPE 3: LIMITED HISTORY
-- ============================================

-- For dimensions with "Previous Value" column
Customer Segment Change Flag = 
IF(
    NOT(ISBLANK('Dim_Customer'[PreviousSegment])) &&
    'Dim_Customer'[PreviousSegment] <> 'Dim_Customer'[CustomerSegment],
    TRUE,
    FALSE
)

-- ============================================
-- SCD SNAPSHOT: AS-OF-DATE REPORTING
-- ============================================

-- Create a snapshot view as of a specific date
Customer Snapshot As Of = 
VAR AsOfDate = MAX('Snapshot_Date'[SnapshotDate])
RETURN
    CALCULATETABLE(
        'Dim_Customer',
        'Dim_Customer'[ValidFrom] <= AsOfDate,
        'Dim_Customer'[ValidTo] >= AsOfDate
    )

-- Revenue using snapshot dimension
Revenue As Of Snapshot = 
VAR AsOfDate = MAX('Snapshot_Date'[SnapshotDate])
RETURN
    CALCULATE(
        [Total Revenue],
        'Dim_Customer'[ValidFrom] <= AsOfDate,
        'Dim_Customer'[ValidTo] >= AsOfDate
    )

-- ============================================
-- SCD AUDIT: TRACKING CHANGES
-- ============================================

-- List all changes for a customer
Customer Change History = 
VAR CustomerID = SELECTEDVALUE('Dim_Customer'[CustomerID])
RETURN
    SELECTCOLUMNS(
        FILTER(
            ALL('Dim_Customer'),
            'Dim_Customer'[CustomerID] = CustomerID
        ),
        "Version", 'Dim_Customer'[CustomerKey],
        "Segment", 'Dim_Customer'[CustomerSegment],
        "Valid From", 'Dim_Customer'[ValidFrom],
        "Valid To", 'Dim_Customer'[ValidTo],
        "Is Current", 'Dim_Customer'[IsCurrent]
    )

-- ============================================
-- SCD PERFORMANCE OPTIMIZATION
-- ============================================

-- Pre-filter to current versions for most queries
Current Customers Only = 
CALCULATE(
    [Total Revenue],
    'Dim_Customer'[IsCurrent] = TRUE
)

-- Use TREATAS for efficient SCD filtering
Revenue for Specific Version = 
VAR TargetVersion = SELECTEDVALUE('Version_Selection'[CustomerKey])
RETURN
    CALCULATE(
        [Total Revenue],
        TREATAS({TargetVersion}, 'Dim_Customer'[CustomerKey])
    )
```

### Best Practice Explanation

**SCD Best Practices:**
1. **Always use business keys** for counting/aggregating, not surrogate keys
2. **Date range queries** must include both ValidFrom and ValidTo
3. **Current flag** for quick filtering of active records
4. **Audit columns** (ETL_LoadDate, etc.) for traceability

**Common SCD Mistakes:**
1. Counting surrogate keys instead of business keys
2. Not handling open-ended dates (9999-12-31)
3. Missing date boundaries in filter logic
4. Not considering SCD in time intelligence

**What Interviewers Are Testing:**
- Understanding of dimensional modeling
- Ability to handle temporal data in DAX
- Knowledge of SCD types and trade-offs
- Experience with historical analysis

---

## QUESTION 10: Bookmarking and Drill-Through

### Business Scenario
Business Stream's analysts need to:
- Save specific views of data for reporting
- Drill from high-level summaries to transaction details
- Compare different scenarios side-by-side
- Create guided analysis paths for executives
- Export specific views for external stakeholders

### The Challenge
Implement a comprehensive bookmarking and drill-through system:
1. Cross-report bookmarking
2. Synchronized drill-through across pages
3. Scenario comparison using bookmarks
4. Conditional drill-through based on context
5. Bookmark persistence and sharing

### Your Bookmark Implementation

```dax
-- ============================================
-- BOOKMARK STATE TRACKING
-- ============================================

-- Track current view state for bookmark restoration
Current View State = 
VAR SelectedRegion = SELECTEDVALUE('Dim_Region'[RegionName], "All Regions")
VAR SelectedSegment = SELECTEDVALUE('Dim_Customer'[CustomerSegment], "All Segments")
VAR SelectedPeriod = FORMAT(MAX('Dim_Date'[FullDate]), "MMM YYYY")
VAR SelectedMetric = SELECTEDVALUE('Metric_Selection'[MetricName], "Revenue")
RETURN
    SelectedRegion & " | " & SelectedSegment & " | " & SelectedPeriod & " | " & SelectedMetric

-- ============================================
-- BOOKMARK COMPARISON MEASURE
-- ============================================

-- Compare current view with saved bookmark
Bookmark Comparison = 
VAR CurrentValue = [Total Revenue]
VAR BookmarkValue = MAX('Bookmark_Data'[SavedRevenue])
VAR Variance = CurrentValue - BookmarkValue
VAR VariancePct = DIVIDE(Variance, BookmarkValue, 0)
RETURN
    SWITCH(TRUE(),
        ABS(VariancePct) < 0.01, "No Change",
        VariancePct > 0, "▲ " & FORMAT(VariancePct, "0.0%"),
        "▼ " & FORMAT(ABS(VariancePct), "0.0%")
    )

-- ============================================
-- SCENARIO BOOKMARKS
-- ============================================

-- Save different scenarios for comparison
Scenario Revenue = 
VAR SelectedScenario = SELECTEDVALUE('Scenario_Selection'[ScenarioName])
RETURN
    SWITCH(SelectedScenario,
        "Base Case", [Total Revenue],
        "Optimistic", [Total Revenue] * 1.15,
        "Pessimistic", [Total Revenue] * 0.85,
        "COVID Impact", [Total Revenue] * 0.92,
        [Total Revenue]
    )

-- Scenario comparison table
Scenario Comparison = 
VAR Scenarios = VALUES('Scenario_Selection'[ScenarioName])
RETURN
    ADDCOLUMNS(
        Scenarios,
        "Revenue", [Scenario Revenue],
        "vs Base", DIVIDE([Scenario Revenue] - CALCULATE([Total Revenue], 'Scenario_Selection'[ScenarioName] = "Base Case"), [Total Revenue], 0)
    )
```

### Drill-Through Implementation

```dax
-- ============================================
-- CONDITIONAL DRILL-THROUGH TARGET
-- ============================================

-- Determine which detail page to show based on context
Drill Through Target Page = 
VAR SelectedLevel = 
    SWITCH(TRUE(),
        HASONEVALUE('Dim_Customer'[CustomerID]), "Customer Detail",
        HASONEVALUE('Dim_Region'[RegionName]), "Regional Detail",
        HASONEVALUE('Dim_Customer'[CustomerSegment]), "Segment Detail",
        "Summary"
    )
RETURN
    SelectedLevel

-- ============================================
-- DRILL-THROUGH FILTER CONTEXT
-- ============================================

-- Pass filter context through drill-through
Drill Through Filter Summary = 
VAR Filters = 
    CONCATENATEX(
        FILTERS('Dim_Customer'[CustomerSegment]),
        'Dim_Customer'[CustomerSegment],
        ", "
    ) &
    CONCATENATEX(
        FILTERS('Dim_Region'[RegionName]),
        'Dim_Region'[RegionName],
        ", "
    ) &
    CONCATENATEX(
        FILTERS('Dim_Date'[Year]),
        'Dim_Date'[Year],
        ", "
    )
RETURN
    IF(LEN(Filters) > 0, "Filtered by: " & Filters, "No filters applied")

-- ============================================
-- DETAIL PAGE TITLE
-- ============================================

-- Dynamic title based on drill-through context
Detail Page Title = 
VAR CustomerName = SELECTEDVALUE('Dim_Customer'[CompanyName])
VAR RegionName = SELECTEDVALUE('Dim_Region'[RegionName])
VAR SegmentName = SELECTEDVALUE('Dim_Customer'[CustomerSegment])
RETURN
    SWITCH(TRUE(),
        NOT(ISBLANK(CustomerName)), "Customer: " & CustomerName,
        NOT(ISBLANK(RegionName)), "Region: " & RegionName,
        NOT(ISBLANK(SegmentName)), "Segment: " & SegmentName,
        "All Data"
    )

-- ============================================
-- DRILL-THROUGH BUTTON VISIBILITY
-- ============================================

-- Show/hide drill-through buttons based on context
Show Customer Drill Button = 
HASONEVALUE('Dim_Customer'[CustomerID])

Show Region Drill Button = 
HASONEVALUE('Dim_Region'[RegionName])

Show Segment Drill Button = 
HASONEVALUE('Dim_Customer'[CustomerSegment])
```

### Advanced Bookmark Patterns

```dax
-- ============================================
-- SYNCHRONIZED BOOKMARKS ACROSS PAGES
-- ============================================

-- Store bookmark state in a table for cross-page sync
Bookmark State Table = 
DATATABLE(
    "BookmarkID", INTEGER,
    "BookmarkName", STRING,
    "RegionFilter", STRING,
    "SegmentFilter", STRING,
    "DateFilter", STRING,
    "MetricFilter", STRING,
    {
        {1, "Q1 Review", "North", "SME", "2024-Q1", "Revenue"},
        {2, "Annual Summary", "All", "All", "2024", "Consumption"}
    }
)

-- Apply bookmark filters dynamically
Apply Bookmark Filters = 
VAR SelectedBookmark = SELECTEDVALUE('Bookmark State Table'[BookmarkID])
VAR BookmarkRegion = LOOKUPVALUE('Bookmark State Table'[RegionFilter], 'Bookmark State Table'[BookmarkID], SelectedBookmark)
VAR BookmarkSegment = LOOKUPVALUE('Bookmark State Table'[SegmentFilter], 'Bookmark State Table'[BookmarkID], SelectedBookmark)
RETURN
    IF(
        NOT(ISBLANK(SelectedBookmark)),
        CALCULATE(
            [Total Revenue],
            'Dim_Region'[RegionName] = BookmarkRegion || BookmarkRegion = "All",
            'Dim_Customer'[CustomerSegment] = BookmarkSegment || BookmarkSegment = "All"
        ),
        [Total Revenue]
    )

-- ============================================
-- BOOKMARK-BASED ALERTS
-- ============================================

-- Alert when current view deviates from bookmark
Bookmark Deviation Alert = 
VAR CurrentRevenue = [Total Revenue]
VAR BookmarkRevenue = MAX('Bookmark_Data'[SavedRevenue])
VAR Deviation = ABS(CurrentRevenue - BookmarkRevenue) / BookmarkRevenue
RETURN
    IF(
        Deviation > 0.10,
        "⚠️ Revenue differs by " & FORMAT(Deviation, "0%") & " from bookmark",
        "✓ Within 10% of bookmark"
    )
```

### Best Practice Explanation

**Bookmark Best Practices:**
1. **Consistent naming convention** for bookmarks
2. **Document bookmark purpose** in description
3. **Test bookmarks** after model changes
4. **Limit bookmark count** to avoid confusion

**Drill-Through Best Practices:**
1. **Clear visual hierarchy** showing drill path
2. **Breadcrumb navigation** for orientation
3. **Consistent filters** across drill levels
4. **Back button** for easy navigation

**What Interviewers Are Testing:**
- Understanding of Power BI's bookmark architecture
- Ability to design intuitive navigation
- Knowledge of UX best practices
- Experience with complex reporting scenarios

---

## QUESTION 11: Power BI Service Deployment and Governance

### Business Scenario
Business Stream needs to deploy Power BI across the organization with:
- Multiple workspaces for different teams
- Controlled deployment from dev to test to production
- Data source management and credentials
- Security and access control
- Monitoring and usage analytics

### The Challenge
Design a complete deployment and governance strategy:
1. Workspace architecture
2. Deployment pipeline configuration
3. Data source management
4. Gateway configuration
5. Monitoring and alerting

### Your Deployment Architecture

```
WORKSPACE ARCHITECTURE:

┌─────────────────────────────────────────────────────────────────┐
│                     BUSINESS STREAM - POWER BI                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  DEV WORKSPACE                                          │   │
│  │  - Semantic models in development                       │   │
│  │  - Experimental reports                                 │   │
│  │  - Limited access (BI team only)                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  TEST WORKSPACE                                         │   │
│  │  - Validated semantic models                            │   │
│  │  - UAT reports                                          │   │
│  │  - Business user testing                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  PRODUCTION WORKSPACE                                   │   │
│  │  - Certified semantic models                            │   │
│  │  - Published reports                                    │   │
│  │  - Broad audience access                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  SHARED WORKSPACE                                       │   │
│  │  - Certified datasets for reuse                         │   │
│  │  - Organizational metrics                               │   │
│  │  - Self-service enabled                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ARCHIVE WORKSPACE                                      │   │
│  │  - Deprecated reports                                   │   │
│  │  - Historical versions                                  │   │
│  │  - Compliance retention                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Deployment Pipeline Configuration

```powershell
# ============================================
# DEPLOYMENT PIPELINE (Power BI REST API)
# ============================================

# Step 1: Deploy from Dev to Test
$deploymentConfig = @{
    "sourceStageOrder" = 0  # Dev
    "targetStageOrder" = 1  # Test
    "options" = @{
        "allowCreateArtifact" = $true
        "allowOverwriteArtifact" = $true
    }
    "artifacts" = @(
        @{
            "sourceId" = "semantic-model-uuid"
            "targetId" = "semantic-model-uuid"
            "type" = "SemanticModel"
        },
        @{
            "sourceId" = "report-uuid"
            "targetId" = "report-uuid"
            "type" = "Report"
        }
    )
}

Invoke-PowerBIRestMethod -Url "groups/$workspaceId/pipelines/$pipelineId/deploy" `
                         -Method POST `
                         -Body ($deploymentConfig | ConvertTo-Json -Depth 10)

# Step 2: Update data source credentials for Test environment
$dataSourceUpdate = @{
    "credentialDetails" = @{
        "credentialType" = "Basic"
        "credentials" = "{\"username\":\"test_user\",\"password\":\"test_password\"}"
        "encryptedConnection" = "Encrypted"
        "encryptionAlgorithm" = "None"
        "privacyLevel" = "Organizational"
    }
}

Invoke-PowerBIRestMethod -Url "groups/$testWorkspaceId/datasets/$datasetId/Default.UpdateDatasources" `
                         -Method POST `
                         -Body ($dataSourceUpdate | ConvertTo-Json)

# Step 3: Refresh dataset in Test
Invoke-PowerBIRestMethod -Url "groups/$testWorkspaceId/datasets/$datasetId/refreshes" `
                         -Method POST

# Step 4: Run automated tests
# - DAX query validation
# - Visual count verification
# - Performance benchmark

# Step 5: Deploy to Production (after approval)
```

### Gateway and Data Source Management

```
GATEWAY ARCHITECTURE:

┌─────────────────────────────────────────────────────────────┐
│                    ON-PREMISES DATA GATEWAY                  │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  CLUSTER: Primary Gateway                            │   │
│  │  - Server: PBI-GW-01.businessstream.local           │   │
│  │  - Status: Active                                   │   │
│  │  - Data Sources:                                    │   │
│  │    * SQL Server (DW) - Production                   │   │
│  │    * SQL Server (DW) - Test                         │   │
│  │    * Oracle (Legacy)                                │   │
│  │    * File Share (CSV imports)                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  CLUSTER: Backup Gateway                             │   │
│  │  - Server: PBI-GW-02.businessstream.local           │   │
│  │  - Status: Standby                                  │   │
│  │  - Auto-failover enabled                            │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Governance DAX Measures

```dax
-- ============================================
-- USAGE MONITORING MEASURES
-- ============================================

-- Track report usage
Report View Count = 
CALCULATE(
    COUNTROWS('Audit_Log'),
    'Audit_Log'[Activity] = "ViewReport"
)

-- Unique viewers
Unique Report Viewers = 
DISTINCTCOUNT('Audit_Log'[UserPrincipalName])

-- Most active users
Top Users = 
TOPN(
    10,
    SUMMARIZECOLUMNS(
        'Audit_Log'[UserPrincipalName],
        "Views", COUNTROWS('Audit_Log')
    ),
    [Views],
    DESC
)

-- ============================================
-- DATA QUALITY MONITORING
-- ============================================

-- Check for data freshness
Data Freshness Status = 
VAR LastRefresh = MAX('Fact_Meter_Readings'[ETL_LoadDate])
VAR HoursSinceRefresh = DATEDIFF(LastRefresh, NOW(), HOUR)
RETURN
    SWITCH(TRUE(),
        HoursSinceRefresh <= 2, "✓ Fresh (" & HoursSinceRefresh & "h ago)",
        HoursSinceRefresh <= 6, "⚠ Stale (" & HoursSinceRefresh & "h ago)",
        "✗ Outdated (" & HoursSinceRefresh & "h ago)"
    )

-- Check for data quality issues
Data Quality Score = 
VAR NullPercentage = DIVIDE(COUNTROWS(FILTER('Fact_Meter_Readings', ISBLANK([Consumption_m3]))), COUNTROWS('Fact_Meter_Readings'), 0)
VAR NegativeValues = COUNTROWS(FILTER('Fact_Meter_Readings', [Consumption_m3] < 0))
VAR Outliers = COUNTROWS(FILTER('Fact_Meter_Readings', [Consumption_m3] > 10000))  -- Suspiciously high
RETURN
    100 - (NullPercentage * 100) - (NegativeValues * 10) - (Outliers * 5)
```

### Best Practice Explanation

**Deployment Best Practices:**
1. **Three-environment minimum:** Dev, Test, Production
2. **Automated testing:** Before each deployment
3. **Rollback plan:** For failed deployments
4. **Change documentation:** Track all changes

**Governance Best Practices:**
1. **Certification process:** For production datasets
2. **Naming conventions:** Consistent across workspaces
3. **Access reviews:** Regular audit of permissions
4. **Usage monitoring:** Identify unused content

**What Interviewers Are Testing:**
- Understanding of Power BI service architecture
- Experience with enterprise deployment
- Knowledge of security and compliance
- Ability to design scalable governance

---

## QUESTION 12: Integrating Python/R Visuals in Power BI

### Business Scenario
Business Stream's data science team has developed advanced analytics in Python:
- Customer churn prediction model (scikit-learn)
- Leakage detection using anomaly detection
- Time series forecasting for consumption
- Network analysis for supply optimization

### The Challenge
Integrate Python/R analytics into Power BI:
1. Python script for data preparation
2. Custom Python visual
3. Passing Power BI data to Python
4. Performance considerations
5. Deployment and security

### Your Python Integration Solution

```python
# ============================================
# PYTHON SCRIPT: DATA PREPARATION
# ============================================

# Power BI passes dataset as pandas DataFrame 'dataset'
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

# Data preparation
# 'dataset' is automatically provided by Power BI
df = dataset.copy()

# Feature engineering
df['consumption_trend'] = df.groupby('CustomerID')['Consumption_m3'].transform(
    lambda x: x.rolling(window=3, min_periods=1).mean()
)
df['consumption_volatility'] = df.groupby('CustomerID')['Consumption_m3'].transform(
    lambda x: x.rolling(window=3, min_periods=1).std()
)

# Anomaly detection for leakage
features = ['Consumption_m3', 'consumption_trend', 'consumption_volatility']
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df[features].fillna(0))

# Isolation Forest for anomaly detection
iso_forest = IsolationForest(contamination=0.05, random_state=42)
df['anomaly_score'] = iso_forest.fit_predict(X_scaled)
df['is_anomaly'] = df['anomaly_score'] == -1

# Return processed data
# Power BI will use the final DataFrame
```

```python
# ============================================
# PYTHON SCRIPT: CUSTOM VISUAL
# ============================================

import matplotlib.pyplot as plt
import seaborn as sns

# Create figure
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Leakage Detection Dashboard - Python Analytics', fontsize=16)

# Plot 1: Anomaly scatter plot
ax1 = axes[0, 0]
colors = ['red' if x else 'blue' for x in dataset['is_anomaly']]
ax1.scatter(dataset['consumption_trend'], dataset['consumption_volatility'], 
           c=colors, alpha=0.6)
ax1.set_xlabel('Consumption Trend')
ax1.set_ylabel('Consumption Volatility')
ax1.set_title('Anomaly Detection (Red = Potential Leak)')

# Plot 2: Time series with anomalies
ax2 = axes[0, 1]
ax2.plot(dataset['ReadingDate'], dataset['Consumption_m3'], color='blue', alpha=0.7)
anomalies = dataset[dataset['is_anomaly']]
ax2.scatter(anomalies['ReadingDate'], anomalies['Consumption_m3'], 
           color='red', s=50, zorder=5)
ax2.set_xlabel('Date')
ax2.set_ylabel('Consumption (m³)')
ax2.set_title('Consumption Over Time with Anomalies')

# Plot 3: Distribution comparison
ax3 = axes[1, 0]
normal = dataset[~dataset['is_anomaly']]['Consumption_m3']
anomaly = dataset[dataset['is_anomaly']]['Consumption_m3']
ax3.hist(normal, bins=30, alpha=0.7, label='Normal', color='blue')
ax3.hist(anomaly, bins=30, alpha=0.7, label='Anomaly', color='red')
ax3.set_xlabel('Consumption (m³)')
ax3.set_ylabel('Frequency')
ax3.set_title('Consumption Distribution')
ax3.legend()

# Plot 4: Summary statistics
ax4 = axes[1, 1]
ax4.axis('off')
summary_text = f"""
Leakage Detection Summary:

Total Readings: {len(dataset):,}
Anomalies Detected: {dataset['is_anomaly'].sum():,}
Anomaly Rate: {dataset['is_anomaly'].mean()*100:.2f}%

Avg Normal Consumption: {normal.mean():.2f} m³
Avg Anomaly Consumption: {anomaly.mean():.2f} m³

Recommended Actions:
- Investigate top 10 anomalies
- Check meter calibration
- Review for pipe leaks
"""
ax4.text(0.1, 0.5, summary_text, fontsize=11, verticalalignment='center',
         fontfamily='monospace')

plt.tight_layout()
plt.show()
```

### R Integration Example

```r
# ============================================
# R SCRIPT: TIME SERIES FORECASTING
# ============================================

library(forecast)
library(ggplot2)

# 'dataset' is provided by Power BI
ts_data <- ts(dataset$Consumption_m3, frequency = 12)

# Fit ARIMA model
fit <- auto.arima(ts_data)

# Generate forecast
forecast_result <- forecast(fit, h = 12)

# Create visualization
ggplot() +
  autolayer(forecast_result) +
  labs(title = "Consumption Forecast - Next 12 Months",
       x = "Month",
       y = "Consumption (m³)") +
  theme_minimal()
```

### Performance Optimization for Python/R

```dax
-- ============================================
-- OPTIMIZE DATA PASSED TO PYTHON
-- ============================================

-- Create aggregated table for Python
Python Input Data = 
SUMMARIZECOLUMNS(
    'Dim_Date'[Year],
    'Dim_Date'[Month],
    'Dim_Customer'[CustomerSegment],
    "AvgConsumption", AVERAGE('Fact_Meter_Readings'[Consumption_m3]),
    "TotalConsumption", SUM('Fact_Meter_Readings'[Consumption_m3]),
    "ReadingCount", COUNT('Fact_Meter_Readings'[ReadingKey])
)

-- ============================================
-- PRE-FILTER FOR PYTHON
-- ============================================

-- Only pass relevant data
Python Filtered Data = 
CALCULATETABLE(
    'Python Input Data',
    'Dim_Date'[Year] >= YEAR(TODAY()) - 2,  -- Last 2 years only
    'Dim_Customer'[CustomerSegment] <> "Test"  -- Exclude test data
)
```

### Security and Deployment

```python
# ============================================
# SECURITY: Secure Credential Handling
# ============================================

# DON'T: Hardcode credentials
# api_key = "sk-1234567890abcdef"  # NEVER DO THIS

# DO: Use Power BI data source credentials
# Credentials are managed in Power BI service

# DO: Validate input data
def validate_input_data(df):
    """Validate data before processing"""
    required_columns = ['CustomerID', 'Consumption_m3', 'ReadingDate']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Check for suspicious data
    if df['Consumption_m3'].max() > 1000000:
        raise ValueError("Suspicious consumption values detected")
    
    return True

# Call validation
validate_input_data(dataset)
```

### Best Practice Explanation

**Python/R Integration Best Practices:**
1. **Limit data volume:** Aggregate before passing to Python
2. **Handle errors gracefully:** Don't crash the report
3. **Document dependencies:** requirements.txt for packages
4. **Test locally first:** Before deploying to service

**Performance Considerations:**
- Python visuals have 150,000 row limit
- R visuals have 150,000 row limit
- Processing happens on user's machine (desktop) or gateway (service)

**What Interviewers Are Testing:**
- Ability to integrate advanced analytics
- Understanding of Power BI's Python/R limitations
- Knowledge of data science workflows
- Experience with production ML deployment

---

## QUESTION 13: Complete Executive Dashboard Scenario

### Business Scenario
Business Stream's CEO needs a comprehensive executive dashboard that provides:
- Real-time view of company performance
- Comparison against OFWAT targets and industry peers
- Customer insights and satisfaction trends
- Financial performance and forecasts
- Operational efficiency metrics
- Ability to drill down to root causes

### The Challenge
Design and build a complete executive dashboard including:
1. Complete data model specification
2. All key DAX measures
3. Visual layout and design
4. Interactivity and navigation
5. Performance optimization
6. Mobile responsiveness

### Your Complete Executive Dashboard Solution

#### Data Model Specification

```
FACT TABLES:
├── Fact_Meter_Readings (Daily grain, 2.5B rows)
│   ├── DateKey, CustomerKey, MeterKey, TariffKey
│   ├── Consumption_m3, Revenue_GBP
│   └── LeakConfidenceScore, IsEstimated
├── Fact_Financials (Monthly grain)
│   ├── DateKey, AccountKey
│   ├── Revenue, Costs, BadDebtProvision
│   └── Budget, Forecast
├── Fact_Customer_Satisfaction (Survey grain)
│   ├── SurveyDate, CustomerKey
│   ├── C_MEX_Score, ComplaintCount
│   └── NPS_Score
└── Fact_Service_Incidents (Incident grain)
    ├── IncidentDate, RegionKey
    ├── IncidentType, Duration_Hours
    └── Properties_Affected

DIMENSION TABLES:
├── Dim_Date (SCD Type 0 - static)
├── Dim_Customer (SCD Type 2)
├── Dim_Tariff (SCD Type 2)
├── Dim_Region (SCD Type 1)
├── Dim_OFWAT_Targets (Annual targets)
└── Dim_Peer_Data (Industry comparison)
```

#### Core DAX Measures

```dax
-- ============================================
-- EXECUTIVE KPI MEASURES
-- ============================================

-- 1. REVENUE METRICS
Total Revenue YTD = 
CALCULATE(
    [Total Revenue],
    DATESYTD('Dim_Date'[FullDate])
)

Revenue vs Budget = 
DIVIDE([Total Revenue YTD] - [Budget YTD], [Budget YTD], 0)

Revenue Forecast = 
VAR CurrentYTD = [Total Revenue YTD]
VAR DaysElapsed = DATEDIFF(DATE(YEAR(TODAY()), 1, 1), TODAY(), DAY)
VAR DaysInYear = 365
VAR RunRate = CurrentYTD / DaysElapsed * DaysInYear
RETURN
    RunRate

-- 2. CUSTOMER METRICS
Active Customers = 
CALCULATE(
    DISTINCTCOUNT('Dim_Customer'[CustomerID]),
    'Dim_Customer'[IsCurrent] = TRUE
)

Customer Satisfaction Score = 
AVERAGE('Fact_Customer_Satisfaction'[C_MEX_Score])

Net Promoter Score = 
VAR Promoters = CALCULATE(COUNTROWS('Fact_Customer_Satisfaction'), 'Fact_Customer_Satisfaction'[NPS_Score] >= 9)
VAR Detractors = CALCULATE(COUNTROWS('Fact_Customer_Satisfaction'), 'Fact_Customer_Satisfaction'[NPS_Score] <= 6)
VAR Total = COUNTROWS('Fact_Customer_Satisfaction')
RETURN
    DIVIDE(Promoters - Detractors, Total, 0) * 100

-- 3. OPERATIONAL METRICS
Leakage Mld = 
VAR LeakVolume = SUM('Fact_Meter_Readings'[EstimatedLeakVolume_m3])
VAR Days = DISTINCTCOUNT('Dim_Date'[DateKey])
RETURN
    DIVIDE(LeakageVolume / 1000, Days, 0)

Leakage vs Target = 
VAR Actual = [Leakage Mld]
VAR Target = MAX('Dim_OFWAT_Targets'[LeakageTarget_Mld])
RETURN
    DIVIDE(Actual - Target, Target, 0)

Cost to Serve = 
DIVIDE(SUM('Fact_Financials'[Operational_Costs]), [Active Customers], 0)

-- 4. COMPOSITE EXECUTIVE SCORECARD
Executive Score = 
VAR RevenueScore = IF([Revenue vs Budget] >= 0, 25, 25 * (1 + [Revenue vs Budget]))
VAR SatisfactionScore = MIN([Customer Satisfaction Score] / 4 * 25, 25)
VAR LeakageScore = MAX(25 - ([Leakage vs Target] * 100), 0)
VAR CostScore = IF([Cost to Serve] <= 50, 25, 25 * (50 / [Cost to Serve]))
RETURN
    RevenueScore + SatisfactionScore + LeakageScore + CostScore

-- 5. PEER COMPARISON
Peer Ranking = 
VAR MyScore = [Executive Score]
VAR AllScores = ALL('Dim_Company'[ExecutiveScore])
VAR BetterThan = COUNTROWS(FILTER(AllScores, [ExecutiveScore] < MyScore))
VAR TotalPeers = COUNTROWS(AllScores)
RETURN
    TotalPeers - BetterThan
```

#### Dashboard Layout

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                    BUSINESS STREAM - CEO EXECUTIVE DASHBOARD                      ║
║                         Last Updated: 14:32 GMT | Data: Current                  ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                   ║
║  ┌─────────────────────────────────────────────────────────────────────────┐     ║
║  │  EXECUTIVE SCORECARD                                                    │     ║
║  │  Overall Score: 82/100 [████████████████░░░░░░░░] Top Quartile          │     ║
║  └─────────────────────────────────────────────────────────────────────────┘     ║
║                                                                                   ║
║  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐        ║
║  │ REVENUE             │ │ CUSTOMERS           │ │ OPERATIONS          │        ║
║  │                     │ │                     │ │                     │        ║
║  │ £124.5M YTD         │ │ 502K Active         │ │ 42.3 Mld Leakage    │        ║
║  │ ▲ 3.2% vs Budget    │ │ ▲ 1.2% YoY          │ │ ▼ 2.1% vs Target    │        ║
║  │                     │ │                     │ │                     │        ║
║  │ Forecast: £498M     │ │ Sat: 87% ▲ 2%       │ │ OFWAT: On Track     │        ║
║  │                     │ │ NPS: +42            │ │ Cost: £48/cust      │        ║
║  │ [Sparkline]         │ │ [Trend]             │ │ [Trend]             │        ║
║  └─────────────────────┘ └─────────────────────┘ └─────────────────────┘        ║
║                                                                                   ║
║  ┌─────────────────────────────────────┐ ┌─────────────────────────────────────┐ ║
║  │ REVENUE BY SEGMENT                  │ │ OFWAT PERFORMANCE MATRIX            │ ║
║  │                                     │ │                                     │ ║
║  │ [Waterfall Chart]                   │ │ [Heatmap with RAG status]           │ ║
║  │ Domestic: £45M (36%)                │ │ PC1: Customer Sat: 🟢 87%           │ ║
║  │ SME: £38M (31%)                     │ │ PC2: Leakage: 🟢 42.3 Mld           │ ║
║  │ Large: £28M (23%)                   │ │ PC3: PCC: 🟡 142 l/p/d              │ ║
║  │ Government: £13M (10%)              │ │ PC4: Interruptions: 🟢 0.8/1000     │ ║
║  │                                     │ │ PC5: Flooding: 🟢 0.3/10000         │ ║
║  │                                     │ │ PC6: Pollution: 🟢 Score 12         │ ║
║  └─────────────────────────────────────┘ └─────────────────────────────────────┘ ║
║                                                                                   ║
║  ┌─────────────────────────────────────┐ ┌─────────────────────────────────────┐ ║
║  │ TOP 5 ALERTS                        │ │ QUICK ACTIONS                       │ ║
║  │                                     │ │                                     │ ║
║  │ 🔴 Revenue shortfall in North: -£2.3M │ │ [📊 View Regional Breakdown]      │ ║
║  │ 🟡 Leakage spike in DMA-042: +15%     │ │ [👥 View Customer Detail]         │ ║
║  │ 🟡 PCC above target: 142 vs 135       │ │ [📈 View Financial Forecast]      │ ║
║  │ 🟢 Customer satisfaction up 2%        │ │ [📋 Export OFWAT Report]          │ ║
║  │ 🟢 Cost to serve reduced by 3%        │ │ [🔔 Set Alert Thresholds]         │ ║
║  └─────────────────────────────────────┘ └─────────────────────────────────────┘ ║
║                                                                                   ║
║  ┌─────────────────────────────────────────────────────────────────────────┐     ║
║  │ TREND ANALYSIS (24 Months)                                              │     ║
║  │ [Multi-metric line chart: Revenue, Customers, Satisfaction, Leakage]    │     ║
║  └─────────────────────────────────────────────────────────────────────────┘     ║
║                                                                                   ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

#### Interactivity and Navigation

```dax
-- ============================================
-- DRILL-THROUGH MEASURES
-- ============================================

-- Dynamic title based on selection
Dashboard Title = 
VAR SelectedRegion = SELECTEDVALUE('Dim_Region'[RegionName])
VAR SelectedSegment = SELECTEDVALUE('Dim_Customer'[CustomerSegment])
RETURN
    "Business Stream" &
    IF(NOT(ISBLANK(SelectedRegion)), " - " & SelectedRegion, "") &
    IF(NOT(ISBLANK(SelectedSegment)), " (" & SelectedSegment & ")", "") &
    " - Executive Dashboard"

-- Context-aware KPI display
Context Revenue = 
IF(
    ISFILTERED('Dim_Region') || ISFILTERED('Dim_Customer'),
    [Total Revenue],
    [Total Revenue YTD]
)

-- ============================================
-- BOOKMARK NAVIGATION
-- ============================================

-- Bookmark state indicator
Current View = 
SWITCH(TRUE(),
    [Selected Bookmark] = "Executive Summary", "Executive View",
    [Selected Bookmark] = "Financial Deep Dive", "Financial Analysis",
    [Selected Bookmark] = "Customer Insights", "Customer Analytics",
    [Selected Bookmark] = "Operational Review", "Operations Dashboard",
    "Default View"
)
```

#### Performance Optimization

```dax
-- ============================================
-- AGGREGATION TABLE FOR EXECUTIVE VIEW
-- ============================================

-- Pre-aggregated monthly metrics
Agg_Executive_Monthly = 
SUMMARIZECOLUMNS(
    'Dim_Date'[Year],
    'Dim_Date'[Month],
    'Dim_Customer'[CustomerSegment],
    'Dim_Region'[RegionName],
    "Revenue", SUM('Fact_Meter_Readings'[Revenue_GBP]),
    "Consumption", SUM('Fact_Meter_Readings'[Consumption_m3]),
    "ActiveCustomers", DISTINCTCOUNT('Fact_Meter_Readings'[CustomerKey]),
    "LeakageVolume", SUM('Fact_Meter_Readings'[EstimatedLeakVolume_m3])
)

-- Use aggregation for high-level views
Executive Revenue = 
IF(
    HASONEVALUE('Dim_Date'[Year]) && NOT(HASONEVALUE('Dim_Date'[DateKey])),
    SUM('Agg_Executive_Monthly'[Revenue]),
    [Total Revenue]
)

-- ============================================
-- INCREMENTAL REFRESH CONFIGURATION
-- ============================================

-- Fact_Meter_Readings table
-- Incremental refresh: Last 5 years, refresh last 7 days
-- Detect data changes: Yes
-- Only refresh complete days: Yes
```

#### Mobile Layout

```
MOBILE (Phone Portrait):
┌─────────────────────────┐
│  [Score: 82/100]        │
│  [Top Quartile]         │
├─────────────────────────┤
│  [Revenue: £124.5M]     │
│  [▲ 3.2% vs Budget]     │
├─────────────────────────┤
│  [Customers: 502K]      │
│  [Sat: 87%] [NPS: +42]  │
├─────────────────────────┤
│  [Leakage: 42.3 Mld]    │
│  [▼ 2.1% vs Target]     │
├─────────────────────────┤
│  [TOP ALERTS]           │
│  🔴 Revenue shortfall   │
│  🟡 Leakage spike       │
├─────────────────────────┤
│  [ACTIONS]              │
│  [Details] [Export]     │
└─────────────────────────┘
```

### Best Practice Explanation

**Executive Dashboard Principles:**
1. **At-a-glance comprehension:** No scrolling for key metrics
2. **Progressive disclosure:** Detail available on demand
3. **Action-oriented:** Every alert has a recommended action
4. **Mobile-first thinking:** Critical info on small screens

**What Interviewers Are Testing:**
- Ability to design for executive audience
- Understanding of complete BI solution architecture
- Knowledge of performance optimization at scale
- Experience with end-to-end dashboard delivery

---

## SUMMARY: KEY CAPABILITIES TESTED

| Question | Primary Skills Tested |
|----------|----------------------|
| 1. Advanced DAX | Context transition, iterators, virtual tables |
| 2. Star Schema | Dimensional modeling, SCD, performance |
| 3. RLS | Security architecture, multi-tenancy, audit |
| 4. Performance | VertiPaq optimization, aggregation, incremental refresh |
| 5. Executive Design | Visual perception, cognitive load, Tufte principles |
| 6. Churn Analysis | Predictive modeling, weighted scoring, segmentation |
| 7. Leakage Detection | Domain expertise, anomaly detection, regulatory |
| 8. OFWAT Compliance | Regulatory reporting, audit trails, exception mgmt |
| 9. SCD Handling | Temporal analysis, point-in-time queries |
| 10. Bookmarks | UX design, navigation, scenario comparison |
| 11. Deployment | Enterprise architecture, governance, DevOps |
| 12. Python/R | Advanced analytics integration, ML deployment |
| 13. Complete Dashboard | End-to-end solution design, executive communication |

---

*"A dashboard is a story, not a spreadsheet. The visual grammar of Tufte teaches us that every pixel should serve the narrative of data. In the DAX dimension, context is king, and those who master the transition between filter and row context hold the keys to the kingdom."*

---

**Document Prepared For:** Business Stream Data Scientist Interview Preparation  
**Focus Areas:** Power BI, DAX, Data Visualization, Water Utilities Domain  
**Difficulty Level:** Advanced/Expert  
**Total Questions:** 13
