# Business Stream Data Architecture Interview Questions
## Senior Data Scientist Role - Water Utilities Sector

---

## Question 1: Medallion Architecture for Water Utility Data Platform

### Architecture Challenge

Business Stream needs to redesign its data platform to handle:
- **Customer data**: 500K+ accounts, billing history, consumption patterns
- **Meter readings**: Transitioning from quarterly manual reads to daily smart meter data (projected 15M readings/day)
- **Operational data**: Leak detection sensors, pressure monitoring, water quality metrics
- **Regulatory data**: Ofwat reporting, environmental compliance, leakage targets

Design a medallion architecture (Bronze-Silver-Gold) using Microsoft Fabric that supports both regulatory reporting AND the strategic shift to prescriptive analytics.

### Detailed Component Recommendations

**Bronze Layer (Raw Ingestion)**
```
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                              │
├─────────────────────────────────────────────────────────────┤
│  Data Sources              │  Ingestion Pattern              │
│  ──────────────────────────┼─────────────────────────────────│
│  • Smart Meters (AMR/AMI)  │  Event Hubs → Fabric Eventstreams│
│  • CIS (Customer Info Sys) │  Data Factory Copy Activity      │
│  • SCADA/Operational       │  Azure IoT Hub → KQL Database    │
│  • External (Weather,      │  ADF Pipelines with CDC          │
│    Wholesale data)         │                                  │
└─────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**
- Use **Fabric Eventstreams** for meter data (streaming) with auto-checkpointing
- Implement **Change Data Capture (CDC)** for SQL Server sources using Data Factory
- Store as **Delta Lake format** in OneLake with `bronze/` prefix
- Partition strategy: `bronze/meter_readings/year=2024/month=01/day=15/`

**Silver Layer (Cleansed & Conformed)**
```
┌─────────────────────────────────────────────────────────────┐
│                    SILVER LAYER                              │
├─────────────────────────────────────────────────────────────┤
│  Processing: Fabric Spark Notebooks / Pipelines              │
│  ───────────────────────────────────────────────────────────│
│  • Data quality validation (Great Expectations style)        │
│  • Deduplication (idempotent writes)                         │
│  • Unit of measure normalization (m³, litres, kL)            │
│  • Customer-meter relationship resolution                    │
│  • Temporal alignment (handle clock skew in meter data)      │
└─────────────────────────────────────────────────────────────┘
```

**Gold Layer (Business-Ready)**
```
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER                                │
├─────────────────────────────────────────────────────────────┤
│  Semantic Models:                                            │
│  ───────────────────────────────────────────────────────────│
│  • Customer 360 (demographics + consumption + risk)          │
│  • Consumption Forecasting Features (time-series ready)      │
│  • Regulatory Reporting Views (Ofwat-ready)                  │
│  • Leak Detection Features (anomaly detection input)         │
│                                                              │
│  Serving: Power BI Datasets, ML Feature Store                │
└─────────────────────────────────────────────────────────────┘
```

### Trade-off Analysis

| Decision | Option A | Option B | Recommendation |
|----------|----------|----------|----------------|
| **Streaming vs Batch for Meters** | Pure streaming (Event Hubs) | Micro-batch (15-min windows) | **Micro-batch** - balances latency with cost; 15-min aggregation sufficient for billing |
| **Silver Processing** | Spark (distributed) | SQL-based (Warehouse) | **Hybrid** - Spark for complex transformations, SQL for simple lookups |
| **Gold Model Storage** | Lakehouse tables | Import to Power BI | **Direct Lake mode** - query OneLake directly, no data movement |

### Best Practices & Anti-Patterns

✅ **DO:**
- Implement **schema evolution handling** in Bronze (Avro with schema registry)
- Use **liquid clustering** for frequently filtered columns (customer_id, reading_date)
- Create **domain-specific medallions** (customer/, operational/, regulatory/)
- Implement **time-travel** (Delta Lake `VERSION AS OF`) for audit requirements

❌ **DON'T:**
- Allow direct Gold layer writes from source systems (breaks lineage)
- Store PII in Bronze without encryption at rest
- Use single partitioning strategy across all data types
- Skip data quality gates between layers

---

## Question 2: ETL vs ELT Decision Framework

### Architecture Challenge

Business Stream has diverse data processing needs:

| Use Case | Volume | Latency | Complexity | Source |
|----------|--------|---------|------------|--------|
| Monthly billing calculations | 500K accounts | 24 hours | High (tariff rules) | SQL Server |
| Real-time leak alerts | 15M meters | <5 minutes | Medium (threshold-based) | IoT Hub |
| Regulatory Ofwat returns | Aggregated | 30 days | Very High (validation) | Multiple |
| Customer churn prediction | 500K accounts | Weekly | High (ML features) | Lakehouse |
| Ad-hoc analytics | Variable | Interactive | Low-Medium | Various |

Design a decision framework that determines ETL vs ELT for each scenario, with specific Azure Data Factory and Fabric implementations.

### Detailed Component Recommendations

**Decision Matrix Framework:**

```
                    HIGH COMPLEXITY
                           │
         ┌─────────────────┼─────────────────┐
         │   ETL (ADF +    │   ETL (Spark    │
         │   Azure SQL)    │   + ML)         │
    LOW  │                 │                 │  HIGH
 LATENCY ├─────────────────┼─────────────────┤ LATENCY
         │   ELT (Fabric   │   Hybrid        │
         │   SQL)          │   (Streaming +  │
         │                 │   Batch)        │
         └─────────────────┼─────────────────┘
                           │
                    LOW COMPLEXITY
```

**Scenario-Specific Architectures:**

**1. Monthly Billing (ETL Pattern)**
```yaml
Pattern: ETL with Azure Data Factory
Why ETL: 
  - Complex tariff calculations require business rules engine
  - Must validate before loading (billing accuracy critical)
  - Historical corrections need audit trail

Implementation:
  Source: SQL Server (CIS) → ADF Copy Activity
  Transform: Azure Batch/Functions for tariff calculation
  Load: SQL Server (Billing tables) with SCD Type 2
  
Monitoring:
  - Data Factory pipeline validation activities
  - Row-level audit logging
  - Reconciliation reports (source vs target)
```

**2. Real-time Leak Alerts (Hybrid Pattern)**
```yaml
Pattern: Streaming ELT with hot path + cold path
Why Hybrid:
  - Need immediate alerts (<5 min) AND historical analysis
  - Complex ML models can't run in real-time

Implementation:
  Hot Path (Immediate):
    IoT Hub → Stream Analytics → Threshold rules → Event Grid → Alerts
    
  Cold Path (ML-enhanced):
    IoT Hub → Event Hubs → Fabric Eventstreams → KQL Database
    → Scheduled Spark job for anomaly detection → Gold layer
```

**3. Regulatory Ofwat Returns (ETL with Heavy Validation)**
```yaml
Pattern: ETL with comprehensive validation framework
Why ETL:
  - Regulatory submission cannot fail
  - Complex cross-table validations required
  - Audit trail mandatory

Implementation:
  Bronze: Raw extracts from all source systems
  Silver: Conformed dimensions, cleansed facts
  Gold: Regulatory-specific views with embedded validation
  
Validation Layers:
  1. Schema validation (Great Expectations)
  2. Business rule validation (ADF Data Flow)
  3. Reconciliation (source-to-target counts)
  4. Regulatory compliance checks (custom Python)
```

### Trade-off Analysis

| Factor | ETL Advantage | ELT Advantage | Winner for Billing |
|--------|---------------|---------------|-------------------|
| **Data Quality** | Validation before load | Post-load quality checks | ETL |
| **Flexibility** | Fixed pipeline | Schema-on-read | ELT (for analytics) |
| **Performance** | Optimized transforms | Push-down to compute | ETL (for complex rules) |
| **Cost** | Higher compute (ADF) | Storage cheaper than compute | Depends on frequency |
| **Auditability** | Clear transformation lineage | Versioned queries | ETL |

### Best Practices & Anti-Patterns

✅ **DO:**
- Use **ADF Mapping Data Flows** for visual ETL development
- Implement **parameterized pipelines** for reusable patterns
- Create **data quality scorecards** at each transformation stage
- Use **Fabric Dataflows Gen2** for citizen developer ELT scenarios

❌ **DON'T:**
- Apply ETL to all scenarios (creates unnecessary bottlenecks)
- Skip data lineage documentation (GDPR requirement)
- Use ELT for regulated financial calculations (audit risk)
- Ignore data skew in distributed transformations

---

## Question 3: Data Quality Framework Design

### Architecture Challenge

Business Stream's data quality issues are causing:
- 3% of bills require manual correction (customer dissatisfaction)
- Regulatory submissions flagged for data inconsistencies
- ML models producing unreliable consumption forecasts
- GDPR data subject requests failing due to incomplete records

Design a comprehensive data quality framework that:
1. Detects issues at ingestion (Bronze layer)
2. Prevents propagation to downstream systems
3. Provides actionable alerts to data stewards
4. Maintains audit trails for regulatory compliance

### Detailed Component Recommendations

**Framework Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY FRAMEWORK                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│   │   PROFIling  │───→│  VALIDATION  │───→│   REMEDIATION │         │
│   │              │    │              │    │               │         │
│   │ • Statistics │    │ • Schema     │    │ • Quarantine  │         │
│   │ • Patterns   │    │ • Business   │    │ • Auto-fix    │         │
│   │ • Anomalies  │    │   rules      │    │ • Alert       │         │
│   └──────────────┘    └──────────────┘    └──────────────┘          │
│          │                   │                   │                   │
│          └───────────────────┴───────────────────┘                   │
│                              │                                       │
│                    ┌─────────┴─────────┐                             │
│                    │  QUALITY SCORECARD │                            │
│                    │  & LINEAGE STORE   │                            │
│                    └───────────────────┘                             │
└─────────────────────────────────────────────────────────────────────┘
```

**Implementation with Microsoft Fabric:**

**1. Profiling Layer (Fabric Spark + Great Expectations)**
```python
# Example: Meter readings quality suite
import great_expectations as gx

context = gx.get_context()

# Create expectation suite for meter readings
suite = context.add_expectation_suite("meter_readings_quality")

# Schema expectations
suite.add_expectation({
    "expectation_type": "expect_column_to_exist",
    "kwargs": {"column": "meter_id"}
})

# Business rule expectations
suite.add_expectation({
    "expectation_type": "expect_column_values_to_be_between",
    "kwargs": {
        "column": "consumption_m3",
        "min_value": 0,
        "max_value": 1000,  # Max daily consumption threshold
        "mostly": 0.99
    }
})

# Referential integrity
suite.add_expectation({
    "expectation_type": "expect_column_values_to_be_in_set",
    "kwargs": {
        "column": "meter_type",
        "value_set": ["DOMESTIC", "COMMERCIAL", "INDUSTRIAL"]
    }
})

# Temporal consistency
suite.add_expectation({
    "expectation_type": "expect_column_pair_values_to_be_equal",
    "kwargs": {
        "column_A": "reading_date",
        "column_B": "billing_period_start",
        "or_equal": True
    }
})
```

**2. Validation Pipeline (Azure Data Factory)**
```yaml
Pipeline: Quality_Gate_Meter_Readings

Activities:
  1. Get Metadata:
     - Check file arrival in Bronze
     
  2. Spark Job (Fabric Notebook):
     - Run Great Expectations suite
     - Output: validation_results.json
     
  3. If Condition (quality_score > 95%):
     - True: Proceed to Silver processing
     - False: Route to Quarantine
     
  4. Quarantine Activity:
     - Copy failed records to: bronze/quarantine/meter_readings/
     - Send alert to: data-stewards@businessstream.co.uk
     - Create ServiceNow ticket via Logic App
     
  5. Audit Logging:
     - Write to: gold/data_quality/audit_log/
     - Fields: timestamp, dataset, score, failed_expectations, row_count
```

**3. Quality Scorecard (Power BI)**
```
Metrics Dashboard:
├── Overall Data Health Score (weighted average)
├── Dataset-level Scores
│   ├── Meter Readings: 97.3%
│   ├── Customer Data: 99.1%
│   └── Billing Transactions: 94.2%
├── Dimension Scores
│   ├── Completeness: 98.5%
│   ├── Uniqueness: 99.9%
│   ├── Validity: 96.2%
│   ├── Timeliness: 92.1%
│   └── Consistency: 95.7%
└── Trend Analysis (30-day rolling)
```

**4. Data Steward Alerting**
```yaml
Alert Configuration:
  Critical (Immediate):
    - PII data quality failures (GDPR risk)
    - Billing calculation data issues
    - Trigger: PagerDuty + Teams notification
    
  High (Within 1 hour):
    - >5% row failures in meter data
    - Schema drift detected
    - Trigger: Email + Teams
    
  Medium (Within 4 hours):
    - Warnings on non-critical fields
    - Trending quality degradation
    - Trigger: Email digest
```

### Trade-off Analysis

| Approach | Real-time Validation | Batch Validation | Recommendation |
|----------|---------------------|------------------|----------------|
| **When to use** | Streaming data (IoT) | Historical loads, DWH | Hybrid |
| **Compute cost** | High (per-event) | Lower (amortized) | Balance with business impact |
| **False positives** | Higher (limited context) | Lower (full dataset view) | Use ML-based anomaly detection |
| **Implementation** | Stream Analytics | Spark batch jobs | Both |

### Best Practices & Anti-Patterns

✅ **DO:**
- Implement **quality gates at every layer** (Bronze → Silver → Gold)
- Use **statistical profiling** to set dynamic thresholds (not static)
- Create **data quality SLAs** with business owners
- Implement **self-healing pipelines** for common issues (format standardization)
- Maintain **quality lineage** (which rules caught which issues)

❌ **DON'T:**
- Apply same quality rules to all data (context matters)
- Block pipelines for minor warnings (risk alert fatigue)
- Skip quarantine for failed records (need audit trail)
- Ignore data quality trends (degradation signals)

---

## Question 4: Data Lineage and Audit Trails for GDPR Compliance

### Architecture Challenge

Business Stream must comply with GDPR requirements including:
- **Right to Access**: Provide complete customer data within 30 days
- **Right to Erasure**: Delete customer data across all systems
- **Data Portability**: Export data in machine-readable format
- **Audit Trail**: Demonstrate who accessed what data when

The challenge: Customer data flows through 12+ systems (CIS, billing, CRM, analytics, third-party processors). Design a lineage and audit system that makes GDPR compliance operationally feasible.

### Detailed Component Recommendations

**Lineage Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA LINEAGE PLATFORM                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │              MICROSOFT PURVIEW (Lineage Engine)              │   │
│   │  • Automated scanning of Fabric assets                       │   │
│   │  • Custom lineage via Atlas API                              │   │
│   │  • Classification of sensitive data                          │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│   ┌──────────────────────────┼──────────────────────────┐           │
│   │                          │                          │           │
│   ▼                          ▼                          ▼           │
│ ┌──────────┐           ┌──────────┐           ┌──────────┐          │
│ │  LINEAGE │           │  AUDIT   │           │  POLICY  │          │
│ │  GRAPH   │           │   LOG    │           │  ENGINE  │          │
│ │          │           │          │           │          │          │
│ │ • Column │           │ • Access │           │ • Retention│         │
│ │   level  │           │ • Transform│         │ • Access   │         │
│ │ • Cross- │           │ • Export │           │   control  │         │
│ │   system │           │ • Delete │           │ • Masking  │          │
│ └──────────┘           └──────────┘           └──────────┘          │
└─────────────────────────────────────────────────────────────────────┘
```

**Implementation Components:**

**1. Purview Integration with Fabric**
```yaml
Scan Configuration:
  Fabric Workspace:
    - Lakehouse tables (Bronze/Silver/Gold)
    - Dataflows Gen2
    - Pipelines
    - Notebooks
    
  SQL Server:
    - CIS database
    - Billing database
    - CRM tables
    
  Classification Rules:
    - PII: customer_name, email, phone, address
    - Sensitive: consumption_data, payment_info
    - Public: meter_id (anonymized), tariff_codes
```

**2. Custom Lineage for Complex Transformations**
```python
# Fabric Notebook: Publishing lineage to Purview
from pyapacheatlas.core import AtlasEntity, AtlasProcess
from pyapacheatlas.auth import ServicePrincipalAuthentication

# Define custom lineage for ML feature engineering
input_entity = AtlasEntity(
    name="silver_customer_consumption",
    typeName="azure_datalake_gen2_resource_set",
    qualifiedName="https://onelake.dfs.fabric.microsoft.com/..."
)

output_entity = AtlasEntity(
    name="gold_churn_features",
    typeName="azure_datalake_gen2_resource_set", 
    qualifiedName="https://onelake.dfs.fabric.microsoft.com/..."
)

process = AtlasProcess(
    name="feature_engineering_churn",
    typeName="Process",
    inputs=[input_entity],
    outputs=[output_entity],
    attributes={
        "description": "ML feature engineering for churn prediction",
        "owner": "data-science-team",
        "lastModified": datetime.now().isoformat()
    }
)

# Publish to Purview
guid_assignments = client.upload_entities([input_entity, output_entity, process])
```

**3. Audit Log Implementation**
```sql
-- Gold layer audit table
CREATE TABLE gold.data_governance.audit_log (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    timestamp TIMESTAMP NOT NULL,
    user_principal VARCHAR(255) NOT NULL,
    action VARCHAR(50) NOT NULL,  -- SELECT, INSERT, UPDATE, DELETE, EXPORT
    resource_type VARCHAR(50),    -- TABLE, COLUMN, FILE
    resource_path VARCHAR(1000),  -- Full qualified name
    customer_id VARCHAR(50),      -- For GDPR tracking
    query_text VARCHAR(MAX),      -- Actual SQL/query
    rows_affected INT,
    ip_address VARCHAR(45),
    application_name VARCHAR(255),
    gdpr_request_id VARCHAR(100), -- Link to GDPR request if applicable
    
    -- Partitioning for query performance
    PARTITIONED BY (DATE(timestamp))
);

-- Enable column-level audit for PII
CREATE AUDIT SPECIFICATION CustomerDataAudit
FOR SERVER AUDIT GDPRComplianceAudit
ADD (SELECT, UPDATE ON gold.customer.customer_pii BY PUBLIC)
WITH (STATE = ON);
```

**4. GDPR Request Processing Pipeline**
```yaml
Pipeline: GDPR_Request_Handler

Activities:
  1. Receive Request:
     - Source: SharePoint list / API
     - Types: ACCESS, DELETE, PORTABILITY, RECTIFICATION
     
  2. Discover Data (Purview API):
     - Query: "Find all assets containing customer_id = {request.customer_id}"
     - Output: List of systems and tables
     
  3. Execute Request:
     - Access: Export from all discovered locations
     - Delete: Execute deletion across all systems
     - Portability: Export in JSON format
     
  4. Verification:
     - Re-query Purview to confirm deletion
     - Generate completion certificate
     
  5. Audit:
     - Log complete request lifecycle
     - Store for regulatory evidence
```

### Trade-off Analysis

| Approach | Automated Lineage | Manual Documentation | Recommendation |
|----------|-------------------|----------------------|----------------|
| **Accuracy** | High (from code) | Risk of staleness | Automated primary, manual for gaps |
| **Coverage** | Standard connectors | Any system | Both |
| **Maintenance** | Low | High | Automated |
| **Cost** | Purview licensing | FTE time | Purview for enterprise |

### Best Practices & Anti-Patterns

✅ **DO:**
- Implement **column-level lineage** (not just table-level)
- Use **consistent customer identifiers** across all systems (surrogate keys)
- Create **GDPR-specific data catalogs** (tag all customer data assets)
- Implement **time-to-live (TTL)** policies for audit logs
- Test **GDPR request SLAs** quarterly (30-day requirement)

❌ **DON'T:**
- Rely solely on Purview auto-scanning (custom code needs manual lineage)
- Store audit logs in same system as production data (segregation)
- Skip lineage for "temporary" tables (often become permanent)
- Ignore cross-border data transfer documentation

---

## Question 5: Real-time vs Batch Processing for Smart Meter Data

### Architecture Challenge

Business Stream is deploying smart meters with the following characteristics:
- **Current state**: 500K meters, quarterly manual reads (4 reads/year/meter)
- **Target state**: 2M smart meters, daily reads (730 reads/year/meter)
- **Future state**: 15-min interval reads (35,040 reads/year/meter)

That's a **8,760x increase** in data volume. Design an architecture that:
1. Handles the volume increase without proportional cost increase
2. Supports both billing (batch) and operational (real-time) use cases
3. Enables the shift to prescriptive analytics (consumption insights)
4. Maintains data quality at scale

### Detailed Component Recommendations

**Lambda Architecture (Batch + Speed Layers):**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SMART METER DATA PLATFORM                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   INGESTION                    PROCESSING                    SERVING │
│   ─────────                    ──────────                    ─────── │
│                                                                      │
│  ┌─────────┐              ┌──────────────┐              ┌─────────┐ │
│  │ IoT Hub │─────────────→│ Speed Layer  │─────────────→│  Alerts │ │
│  │         │   (Hot Path) │ (Stream      │  (Real-time) │  (5 min)│ │
│  │ • 15-min │              │  Analytics)  │              │         │ │
│  │   reads  │              │              │              │ • Leak  │ │
│  │ • Events │              │ • Threshold  │              │   detect│ │
│  │          │              │   rules      │              │ • High  │ │
│  └─────────┘              │ • Anomaly    │              │   usage │ │
│       │                   │   detection  │              │         │ │
│       │                   └──────────────┘              └─────────┘ │
│       │                                                              │
│       │                   ┌──────────────┐              ┌─────────┐ │
│       └──────────────────→│ Batch Layer  │─────────────→│  Billing│ │
│          (Cold Path)      │ (Spark/      │  (Daily)     │  Reports│ │
│                           │  Fabric)     │              │         │ │
│                           │              │              │ • Daily │ │
│                           │ • Aggregation│              │   reads │ │
│                           │ • ML features│              │ • Tariff│ │
│                           │ • Validation │              │   calc  │ │
│                           └──────────────┘              └─────────┘ │
│                                                                      │
│                           ┌──────────────┐              ┌─────────┐ │
│                           │ Serving Layer│─────────────→│Analytics│ │
│                           │ (Unified)    │              │         │ │
│                           │              │              │ • Power │ │
│                           │ • KQL DB     │              │   BI    │ │
│                           │ • Delta Lake │              │ • ML    │ │
│                           └──────────────┘              └─────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

**Component Specifications:**

**1. Ingestion (Azure IoT Hub)**
```yaml
Configuration:
  Tier: S2 (for 2M devices)
  Partitions: 32 (for parallel processing)
  Message retention: 1 day
  
Device Twins:
  - meter_id
  - customer_id
  - tariff_code
  - last_reading
  - communication_status
  
Message Format:
  ```json
  {
    "meter_id": "MTR-12345",
    "timestamp": "2024-01-15T10:30:00Z",
    "readings": {
      "consumption_m3": 0.45,
      "pressure_bar": 2.1,
      "temperature_c": 12.3
    },
    "quality_flag": "VALID"
  }
  ```
```

**2. Speed Layer (Azure Stream Analytics)**
```sql
-- Leak detection query
WITH HourlyConsumption AS (
  SELECT 
    meter_id,
    System.Timestamp() as window_end,
    SUM(readings.consumption_m3) as hourly_consumption
  FROM iothub_input
  TIMESTAMP BY timestamp
  GROUP BY meter_id, TumblingWindow(hour, 1)
),
BaselineStats AS (
  SELECT 
    meter_id,
    AVG(hourly_consumption) as avg_consumption,
    STDEV(hourly_consumption) as stdev_consumption
  FROM reference_data  -- Pre-computed from historical data
)
SELECT 
  h.meter_id,
  h.window_end,
  h.hourly_consumption,
  CASE 
    WHEN h.hourly_consumption > b.avg_consumption + 3 * b.stdev_consumption 
    THEN 'LEAK_SUSPECTED'
    WHEN h.hourly_consumption > b.avg_consumption + 2 * b.stdev_consumption 
    THEN 'HIGH_USAGE'
    ELSE 'NORMAL'
  END as alert_type
INTO eventhub_alerts
FROM HourlyConsumption h
JOIN BaselineStats b ON h.meter_id = b.meter_id
WHERE h.hourly_consumption > b.avg_consumption + 2 * b.stdev_consumption
```

**3. Batch Layer (Fabric Spark)**
```python
# Daily aggregation job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("MeterDailyAggregation").getOrCreate()

# Read raw data from Bronze
raw_df = spark.read.format("delta").load("/bronze/meter_readings")

# Daily aggregation
daily_agg = raw_df \
    .filter(col("quality_flag") == "VALID") \
    .groupBy(
        "meter_id",
        "customer_id",
        window("timestamp", "1 day").alias("reading_date")
    ) \
    .agg(
        sum("consumption_m3").alias("daily_consumption_m3"),
        avg("pressure_bar").alias("avg_pressure_bar"),
        min("temperature_c").alias("min_temperature_c"),
        max("temperature_c").alias("max_temperature_c"),
        count("*").alias("reading_count"),
        sum(when(col("quality_flag") == "VALID", 1).otherwise(0)).alias("valid_readings"),
        sum(when(col("quality_flag") != "VALID", 1).otherwise(0)).alias("invalid_readings")
    )

# Merge to Silver (SCD Type 1 for aggregations)
delta_table = DeltaTable.forPath(spark, "/silver/meter_daily")
delta_table.alias("target").merge(
    daily_agg.alias("source"),
    "target.meter_id = source.meter_id AND target.reading_date = source.reading_date"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**4. Cost Optimization Strategy**

| Volume Tier | Architecture | Estimated Monthly Cost |
|-------------|--------------|----------------------|
| <1M meters/day | IoT Hub S1 + Stream Analytics 1 SU | £800 |
| 1-5M meters/day | IoT Hub S2 + Stream Analytics 3 SU + Fabric F64 | £2,500 |
| 5-15M meters/day | IoT Hub S2 + Stream Analytics 6 SU + Fabric F128 | £5,000 |
| >15M meters/day | Consider Event Hubs Dedicated + Kubernetes | £8,000+ |

### Trade-off Analysis

| Factor | Real-time (Speed) | Batch | Hybrid (Recommended) |
|--------|-------------------|-------|---------------------|
| **Latency** | <1 second | Hours | 5-15 min for ops, daily for billing |
| **Cost** | High (always-on) | Low (scheduled) | Optimized per use case |
| **Complexity** | High | Low | Medium |
| **Accuracy** | Approximate (windowed) | Exact (full dataset) | Both available |
| **Use Case Fit** | Alerts, monitoring | Billing, reporting | Complete coverage |

### Best Practices & Anti-Patterns

✅ **DO:**
- Use **tumbling windows** for consistent aggregations (not sliding)
- Implement **backpressure handling** (IoT Hub throttling)
- Create **data retention policies** (hot: 7 days, warm: 90 days, cold: 7 years)
- Use **reference data joins** for enrichment (tariff lookups)
- Implement **dead letter queues** for failed processing

❌ **DON'T:**
- Process all data in real-time (prohibitively expensive)
- Skip data validation in speed layer (quality still matters)
- Use same storage for hot and cold paths (different access patterns)
- Ignore clock skew in distributed meter networks

---

## Question 6: Azure Data Factory Pipeline Orchestration

### Architecture Challenge

Business Stream's data ecosystem has grown organically, resulting in:
- 150+ individual ADF pipelines with no central orchestration
- Complex dependencies (billing depends on meter data, which depends on customer master)
- No unified error handling (failures require manual intervention)
- Inconsistent logging and monitoring
- Monthly billing runs that take 48+ hours with frequent restarts

Design a unified orchestration framework using Azure Data Factory that provides:
1. Centralized pipeline scheduling and dependency management
2. Standardized error handling and retry logic
3. Comprehensive logging and alerting
4. Support for both batch and event-driven execution

### Detailed Component Recommendations

**Orchestration Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ADF ORCHESTRATION FRAMEWORK                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │              MASTER ORCHESTRATION PIPELINE                   │   │
│   │  • Parameter-driven execution                                │   │
│   │  • Dynamic pipeline invocation                               │   │
│   │  • Centralized logging                                       │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│           ┌──────────────────┼──────────────────┐                   │
│           ▼                  ▼                  ▼                   │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐           │
│   │  INGESTION   │   │TRANSFORMATION│   │   SERVING    │           │
│   │   DOMAIN     │   │   DOMAIN     │   │   DOMAIN     │           │
│   │              │   │              │   │              │           │
│   │ • Meter Data │   │ • Cleansing  │   │ • Reporting  │           │
│   │ • Customer   │   │ • Enrichment │   │ • ML Export  │           │
│   │   Master     │   │ • Aggregation│   │ • API Load   │           │
│   │ • Reference  │   │              │   │              │           │
│   └──────────────┘   └──────────────┘   └──────────────┘           │
│                                                                      │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │              SHARED SERVICES                                 │   │
│   │  • Error Handling Framework  • Audit Logging                 │   │
│   │  • Notification System       • Configuration Management      │   │
│   └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

**Master Pipeline Design:**

```json
{
  "name": "Master_Orchestrator",
  "properties": {
    "parameters": {
      "execution_date": {
        "type": "string",
        "defaultValue": "@utcnow('yyyy-MM-dd')"
      },
      "execution_mode": {
        "type": "string",
        "allowedValues": ["FULL", "INCREMENTAL", "RETRY_FAILED"]
      },
      "domain_filter": {
        "type": "array",
        "defaultValue": ["ALL"]
      }
    },
    "activities": [
      {
        "name": "Initialize_Execution_Context",
        "type": "SetVariable",
        "dependsOn": [],
        "userProperties": [],
        "typeProperties": {
          "variableName": "execution_context",
          "value": {
            "value": "@concat('{\"execution_id\": \"', guid(), '\", \"start_time\": \"', utcnow(), '\", \"triggered_by\": \"', pipeline().TriggerName, '\"}')",
            "type": "Expression"
          }
        }
      },
      {
        "name": "Load_Pipeline_Config",
        "type": "Lookup",
        "dependsOn": [
          {
            "activity": "Initialize_Execution_Context",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "policy": {
          "timeout": "0.00:05:00",
          "retry": 2
        },
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": {
              "value": "SELECT * FROM config.pipeline_dependencies WHERE domain IN ('@{join(parameters('domain_filter'), ''','')}') OR '@{parameters('domain_filter')[0]}' = 'ALL' ORDER BY execution_order",
              "type": "Expression"
            }
          }
        }
      },
      {
        "name": "Execute_Domain_Pipelines",
        "type": "ForEach",
        "dependsOn": [
          {
            "activity": "Load_Pipeline_Config",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "items": {
            "value": "@activity('Load_Pipeline_Config').output.value",
            "type": "Expression"
          },
          "activities": [
            {
              "name": "Execute_Child_Pipeline",
              "type": "ExecutePipeline",
              "typeProperties": {
                "pipeline": {
                  "referenceName": {
                    "value": "@item().pipeline_name",
                    "type": "Expression"
                  },
                  "type": "PipelineReference"
                },
                "waitOnCompletion": true,
                "parameters": {
                  "execution_date": {
                    "value": "@parameters('execution_date')",
                    "type": "Expression"
                  },
                  "execution_mode": {
                    "value": "@parameters('execution_mode')",
                    "type": "Expression"
                  },
                  "parent_execution_id": {
                    "value": "@variables('execution_context')",
                    "type": "Expression"
                  }
                }
              }
            }
          ]
        }
      }
    ]
  }
}
```

**Error Handling Framework:**

```json
{
  "name": "Error_Handler",
  "properties": {
    "parameters": {
      "error_context": { "type": "object" },
      "severity": { 
        "type": "string", 
        "allowedValues": ["CRITICAL", "HIGH", "MEDIUM", "LOW"] 
      }
    },
    "activities": [
      {
        "name": "Log_Error",
        "type": "SqlServerStoredProcedure",
        "typeProperties": {
          "storedProcedureName": "[logging].[sp_LogPipelineError]",
          "storedProcedureParameters": {
            "execution_id": { "value": "@{parameters('error_context').execution_id}", "type": "String" },
            "pipeline_name": { "value": "@{parameters('error_context').pipeline_name}", "type": "String" },
            "activity_name": { "value": "@{parameters('error_context').activity_name}", "type": "String" },
            "error_message": { "value": "@{parameters('error_context').error_message}", "type": "String" },
            "severity": { "value": "@{parameters('severity')}", "type": "String" }
          }
        }
      },
      {
        "name": "Send_Alert",
        "type": "WebActivity",
        "dependsOn": [{ "activity": "Log_Error", "dependencyConditions": ["Succeeded"] }],
        "typeProperties": {
          "url": "https://prod-uk.logic.azure.com/workflows/.../triggers/manual/paths/invoke",
          "method": "POST",
          "body": {
            "severity": "@{parameters('severity')}",
            "message": "@{parameters('error_context').error_message}",
            "pipeline": "@{parameters('error_context').pipeline_name}",
            "execution_id": "@{parameters('error_context').execution_id}"
          }
        }
      }
    ]
  }
}
```

**Configuration Database Schema:**

```sql
-- Pipeline dependency configuration
CREATE TABLE config.pipeline_dependencies (
    dependency_id INT PRIMARY KEY,
    domain VARCHAR(50) NOT NULL,  -- INGESTION, TRANSFORMATION, SERVING
    pipeline_name VARCHAR(255) NOT NULL,
    execution_order INT NOT NULL,
    parent_dependencies VARCHAR(MAX),  -- JSON array of pipeline names
    schedule_cron VARCHAR(100),
    max_retry_attempts INT DEFAULT 3,
    retry_interval_minutes INT DEFAULT 5,
    timeout_hours INT DEFAULT 2,
    is_active BIT DEFAULT 1
);

-- Execution audit log
CREATE TABLE logging.pipeline_execution (
    execution_id UNIQUEIDENTIFIER PRIMARY KEY,
    parent_execution_id UNIQUEIDENTIFIER NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2 NULL,
    status VARCHAR(20) NOT NULL,  -- RUNNING, SUCCEEDED, FAILED, CANCELLED
    rows_processed BIGINT NULL,
    error_message VARCHAR(MAX) NULL,
    parameters_json VARCHAR(MAX) NULL
);
```

### Trade-off Analysis

| Approach | Monolithic Orchestrator | Micro-pipelines | Hybrid (Recommended) |
|----------|------------------------|-----------------|---------------------|
| **Complexity** | High | Low | Medium |
| **Debugging** | Hard (long chains) | Easy (isolated) | Medium |
| **Reuse** | Low | High | High |
| **Failure Impact** | Large (cascading) | Isolated | Controlled |
| **Monitoring** | Single view | Fragmented | Unified dashboard |

### Best Practices & Anti-Patterns

✅ **DO:**
- Implement **idempotent pipelines** (safe to re-run)
- Use **parameterized configurations** (no hard-coded values)
- Create **pipeline templates** for common patterns
- Implement **circuit breakers** (stop on critical failures)
- Use **checkpointing** for long-running pipelines

❌ **DON'T:**
- Create circular dependencies (A depends on B, B depends on A)
- Use static scheduling without dependency checks
- Skip logging for "simple" pipelines
- Hard-code connection strings (use Linked Services + Key Vault)

---

## Question 7: Slowly Changing Dimensions in Utility Data Warehouse

### Architecture Challenge

Business Stream's data warehouse must track historical changes for:
- **Customers**: Address changes, tariff changes, account status changes
- **Meters**: Location changes, meter replacements, calibration updates
- **Tariffs**: Rate changes, seasonal adjustments, regulatory changes
- **Service Points**: Connection/disconnection dates, pressure zone changes

The challenge: Design SCD patterns that balance:
1. Historical accuracy (regulatory requirement)
2. Query performance (analyst productivity)
3. Storage efficiency (cost control)
4. Implementation complexity (maintainability)

### Detailed Component Recommendations

**SCD Pattern Selection Matrix:**

| Entity | Change Frequency | History Needed | SCD Type | Rationale |
|--------|-----------------|----------------|----------|-----------|
| Customer Demographics | Low | Full | Type 2 | Audit trail for GDPR |
| Customer Address | Medium | Full | Type 2 | Billing accuracy |
| Tariff Assignment | Medium | Full | Type 2 | Revenue reconciliation |
| Tariff Rates | Low | Point-in-time | Type 1 | Current rates only |
| Meter Location | Low | Full | Type 2 | Leak traceability |
| Meter Readings | High (append-only) | N/A | N/A | Immutable facts |

**Type 2 SCD Implementation (Fabric SQL Warehouse):**

```sql
-- Customer dimension with Type 2 SCD
CREATE TABLE dim.customer (
    -- Surrogate key (auto-increment)
    customer_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Natural key (business identifier)
    customer_id VARCHAR(50) NOT NULL,
    
    -- Attributes
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    postcode VARCHAR(20),
    tariff_code VARCHAR(20),
    account_status VARCHAR(20),
    
    -- SCD Type 2 columns
    valid_from DATE NOT NULL,
    valid_to DATE NULL,  -- NULL = current record
    is_current BIT NOT NULL DEFAULT 1,
    
    -- Audit columns
    created_at DATETIME2 DEFAULT GETDATE(),
    created_by VARCHAR(100) DEFAULT SUSER_SNAME(),
    updated_at DATETIME2 NULL,
    updated_by VARCHAR(100) NULL,
    source_system VARCHAR(50) NULL,
    
    -- Constraints
    CONSTRAINT uk_customer_natural_key UNIQUE (customer_id, valid_from)
);

-- Index for efficient current record lookups
CREATE INDEX ix_customer_current ON dim.customer(customer_id) WHERE is_current = 1;

-- Index for historical queries
CREATE INDEX ix_customer_history ON dim.customer(customer_id, valid_from, valid_to);
```

**SCD Merge Procedure:**

```sql
CREATE PROCEDURE [etl].[sp_MergeCustomerDimension]
    @BatchDate DATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Step 1: Identify changed records
    WITH ChangedRecords AS (
        SELECT 
            s.customer_id,
            s.customer_name,
            s.email,
            s.phone,
            s.address_line1,
            s.address_line2,
            s.city,
            s.postcode,
            s.tariff_code,
            s.account_status,
            s.source_system
        FROM staging.customer s
        INNER JOIN dim.customer d 
            ON s.customer_id = d.customer_id 
            AND d.is_current = 1
        WHERE 
            -- Check for any attribute change
            s.customer_name != d.customer_name
            OR s.email != d.email
            OR s.phone != d.phone
            OR s.address_line1 != d.address_line1
            OR s.address_line2 != d.address_line2
            OR s.city != d.city
            OR s.postcode != d.postcode
            OR s.tariff_code != d.tariff_code
            OR s.account_status != d.account_status
    ),
    -- Step 2: Expire current records
    ExpiredRecords AS (
        UPDATE d
        SET 
            valid_to = DATEADD(day, -1, @BatchDate),
            is_current = 0,
            updated_at = GETDATE(),
            updated_by = SUSER_SNAME()
        OUTPUT 
            INSERTED.customer_id,
            INSERTED.customer_name,
            INSERTED.email,
            INSERTED.phone,
            INSERTED.address_line1,
            INSERTED.address_line2,
            INSERTED.city,
            INSERTED.postcode,
            INSERTED.tariff_code,
            INSERTED.account_status,
            INSERTED.source_system
        FROM dim.customer d
        INNER JOIN ChangedRecords c ON d.customer_id = c.customer_id
        WHERE d.is_current = 1
    )
    -- Step 3: Insert new records
    INSERT INTO dim.customer (
        customer_id, customer_name, email, phone,
        address_line1, address_line2, city, postcode,
        tariff_code, account_status, valid_from, valid_to, is_current,
        created_at, created_by, source_system
    )
    SELECT 
        customer_id, customer_name, email, phone,
        address_line1, address_line2, city, postcode,
        tariff_code, account_status,
        @BatchDate, NULL, 1,
        GETDATE(), SUSER_SNAME(), source_system
    FROM staging.customer
    WHERE customer_id IN (SELECT customer_id FROM ChangedRecords)
    
    -- Step 4: Insert new customers (not in dimension)
    INSERT INTO dim.customer (
        customer_id, customer_name, email, phone,
        address_line1, address_line2, city, postcode,
        tariff_code, account_status, valid_from, valid_to, is_current,
        created_at, created_by, source_system
    )
    SELECT 
        s.customer_id, s.customer_name, s.email, s.phone,
        s.address_line1, s.address_line2, s.city, s.postcode,
        s.tariff_code, s.account_status,
        @BatchDate, NULL, 1,
        GETDATE(), SUSER_SNAME(), s.source_system
    FROM staging.customer s
    LEFT JOIN dim.customer d ON s.customer_id = d.customer_id
    WHERE d.customer_id IS NULL;
    
END;
```

**Point-in-Time Query Pattern:**

```sql
-- Get customer state as of a specific date
CREATE FUNCTION [dim].[fn_GetCustomerAtDate]
(
    @CustomerId VARCHAR(50),
    @AsOfDate DATE
)
RETURNS TABLE
AS
RETURN
    SELECT *
    FROM dim.customer
    WHERE customer_id = @CustomerId
    AND valid_from <= @AsOfDate
    AND (valid_to IS NULL OR valid_to >= @AsOfDate);
GO

-- Usage: Get customer state on billing date
SELECT c.*, b.billing_amount
FROM fact.billing b
CROSS APPLY dim.fn_GetCustomerAtDate(b.customer_id, b.billing_date) c
WHERE b.billing_date BETWEEN '2024-01-01' AND '2024-01-31';
```

### Trade-off Analysis

| SCD Type | Storage | Query Complexity | History | Use Case |
|----------|---------|------------------|---------|----------|
| **Type 0** | Minimal | Simple | None | Immutable attributes |
| **Type 1** | Minimal | Simple | None | Corrections only |
| **Type 2** | High | Medium | Full | Audit requirements |
| **Type 3** | Medium | Simple | Limited | Current + previous only |
| **Type 4** | Very High | Complex | Full + current | Large dimensions |

### Best Practices & Anti-Patterns

✅ **DO:**
- Use **surrogate keys** for all dimension references in fact tables
- Implement **soft deletes** (mark as deleted, don't remove)
- Create **date-effective views** for common query patterns
- Use **partitioning** on `valid_from` for large dimensions
- Implement **CDC** at source for change detection

❌ **DON'T:**
- Use natural keys in fact tables (breaks on SCD changes)
- Allow overlapping date ranges (data integrity issue)
- Skip indexing on `is_current` flag (performance killer)
- Mix SCD types within same dimension without documentation

---

## Question 8: Data Governance Framework Design

### Architecture Challenge

Business Stream lacks a unified data governance approach, resulting in:
- No clear data ownership ("everyone's data is no one's data")
- Inconsistent data definitions ("customer" means different things in different systems)
- No data quality SLAs (analysts spend 60% of time cleaning data)
- Regulatory compliance gaps (GDPR, Ofwat data requirements)
- Shadow IT data solutions (Excel files with sensitive data)

Design a data governance framework that:
1. Establishes clear ownership and accountability
2. Creates a unified business glossary
3. Implements data quality monitoring with SLAs
4. Ensures regulatory compliance
5. Enables self-service analytics without chaos

### Detailed Component Recommendations

**Governance Framework Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA GOVERNANCE FRAMEWORK                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │              GOVERNANCE ORGANIZATION                         │   │
│   │                                                              │   │
│   │  Data Governance Council (Executive)                         │   │
│   │  ├── CDO (Chief Data Officer)                                │   │
│   │  ├── Data Stewards (Domain Owners)                           │   │
│   │  ├── Data Custodians (Technical Owners)                      │   │
│   │  └── Data Consumers (Business Users)                         │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│   ┌──────────────────────────┼──────────────────────────┐           │
│   │                          │                          │           │
│   ▼                          ▼                          ▼           │
│ ┌──────────┐           ┌──────────┐           ┌──────────┐          │
│ │ BUSINESS │           │ TECHNICAL│           │OPERATIONAL│         │
│ │ GOVERNANCE│          │ GOVERNANCE│          │ GOVERNANCE│         │
│ │          │           │          │           │          │          │
│ │ • Glossary│          │ • Metadata│          │ • Quality│          │
│ │ • Policies│          │ • Lineage │          │   SLAs   │          │
│ │ • Standards│         │ • Catalog │          │ • Access │          │
│ │          │           │ • Classification│    │   Mgmt   │          │
│ └──────────┘           └──────────┘           └──────────┘          │
│      │                      │                      │                │
│      └──────────────────────┼──────────────────────┘                │
│                             ▼                                       │
│                   ┌──────────────────┐                              │
│                   │  MICROSOFT PURVIEW │                             │
│                   │  (Unified Platform) │                            │
│                   └──────────────────┘                              │
└─────────────────────────────────────────────────────────────────────┘
```

**Business Glossary Implementation:**

```yaml
# Example: Customer entity in business glossary
Term: Customer
Definition: An individual or organization that receives water services from Business Stream
Owner: Customer Data Steward (customer.data@businessstream.co.uk)
Status: Approved
Last_Reviewed: 2024-01-15

Attributes:
  - Name: customer_id
    Definition: Unique identifier for the customer
    Data_Type: VARCHAR(50)
    Source_System: CIS
    Sensitivity: Internal
    
  - Name: customer_name
    Definition: Legal name of the customer
    Data_Type: VARCHAR(255)
    Source_System: CIS
    Sensitivity: PII
    
  - Name: tariff_code
    Definition: Code identifying the pricing structure
    Data_Type: VARCHAR(20)
    Source_System: Billing
    Sensitivity: Internal
    
Related_Terms:
  - Account
  - Service_Point
  - Meter
  
Data_Quality_Rules:
  - customer_id must be unique
  - customer_name must not be null
  - tariff_code must exist in dim.tariff
```

**Data Quality SLA Framework:**

```yaml
SLA_Tiers:
  Tier_1_Critical:  # Billing, Regulatory
    Examples:
      - Customer billing data
      - Ofwat regulatory returns
      - GDPR personal data
    Availability: 99.9%
    Data_Quality_Score: >= 99.5%
    Incident_Response: 15 minutes
    RTO: 4 hours
    RPO: 1 hour
    
  Tier_2_Important:  # Operational
    Examples:
      - Meter reading data
      - Leak detection alerts
      - Customer service data
    Availability: 99.5%
    Data_Quality_Score: >= 98%
    Incident_Response: 1 hour
    RTO: 8 hours
    RPO: 4 hours
    
  Tier_3_Analytics:  # Reporting, ML
    Examples:
      - Consumption analytics
      - Churn prediction features
      - Ad-hoc analysis datasets
    Availability: 99%
    Data_Quality_Score: >= 95%
    Incident_Response: 4 hours
    RTO: 24 hours
    RPO: 24 hours
```

**Purview Implementation:**

```python
# Automated data quality score publishing to Purview
from pyapacheatlas.core import AtlasEntity, AtlasClassification
from pyapacheatlas.auth import ServicePrincipalAuthentication

# Authenticate
auth = ServicePrincipalAuthentication(
    tenant_id="...",
    client_id="...",
    client_secret="..."
)

client = PurviewClient(
    account_name="businessstream-purview",
    authentication=auth
)

# Update asset with quality score
quality_classification = AtlasClassification(
    typeName="data_quality_score",
    attributes={
        "score": 97.5,
        "last_measured": "2024-01-15T10:00:00Z",
        "measured_by": "great_expectations",
        "sla_tier": "Tier_1_Critical"
    }
)

client.classify_entity(
    guid="asset-guid-here",
    classifications=[quality_classification]
)

# Apply sensitivity labels
pii_classification = AtlasClassification(
    typeName="PII",
    attributes={
        "sensitivity_level": "High",
        "gdpr_category": "Personal Data"
    }
)

client.classify_entity(
    guid="asset-guid-here",
    classifications=[pii_classification]
)
```

**Access Control Matrix:**

| Role | Bronze | Silver | Gold | PII Access | Admin |
|------|--------|--------|------|------------|-------|
| Data Engineer | RW | RW | R | With Approval | No |
| Data Analyst | - | R | RW | Masked | No |
| Data Scientist | - | R | RW | Anonymized | No |
| Business User | - | - | R | Aggregated | No |
| Data Steward | R | RW | RW | Full | Yes |
| CDO | R | R | R | Full | Yes |

### Trade-off Analysis

| Governance Level | Centralized | Federated | Hybrid (Recommended) |
|-----------------|-------------|-----------|---------------------|
| **Speed** | Slow (bottleneck) | Fast (distributed) | Balanced |
| **Consistency** | High | Variable | Domain-level |
| **Scalability** | Limited | High | High |
| **Adoption** | Resistance | Enthusiasm | Gradual |
| **Cost** | High FTE | Lower FTE | Medium |

### Best Practices & Anti-Patterns

✅ **DO:**
- Start with **critical data domains** (don't boil the ocean)
- Implement **data stewardship by domain** (not by system)
- Create **self-service data catalog** (reduce "where is" questions)
- Use **automated classification** (Purview scanning)
- Implement **data quality scorecards** visible to all

❌ **DON'T:**
- Create governance without executive sponsorship
- Implement policies without enforcement mechanisms
- Ignore shadow IT (bring it into the light)
- Skip data lineage for "simple" transformations
- Make governance a blocker (enable with guardrails)

---

## Question 9: Scaling from Thousands to Millions of Smart Meters

### Architecture Challenge

Business Stream's data platform must scale from:
- **Current**: 500K meters × 4 reads/year = 2M readings/year
- **Year 1**: 1M meters × 365 reads/year = 365M readings/year
- **Year 3**: 2M meters × 365 reads/year = 730M readings/year
- **Future**: 2M meters × 35,040 reads/year (15-min) = 70B readings/year

That's a **35,000x increase** in data volume. Design an architecture that:
1. Handles the scale without linear cost increase
2. Maintains query performance for analytics
3. Supports both real-time and historical analysis
4. Enables the transition from batch to streaming

### Detailed Component Recommendations

**Scalable Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SCALABLE METER DATA PLATFORM                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  INGESTION (Auto-scaling)                                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │  IoT Hub    │  │Event Hubs   │  │  API Gateway │                  │
│  │  (Devices)  │  │(High volume)│  │  (Legacy)    │                  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │
│         └─────────────────┴─────────────────┘                        │
│                           │                                          │
│  PROCESSING (Tiered)                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │ Hot Path    │  │ Warm Path   │  │ Cold Path   │                  │
│  │ (< 1 min)   │  │ (minutes)   │  │ (hours)     │                  │
│  │             │  │             │  │             │                  │
│  │ Stream      │  │ Micro-batch │  │ Batch Spark │                  │
│  │ Analytics   │  │ (15 min)    │  │ (daily)     │                  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │
│         └─────────────────┴─────────────────┘                        │
│                           │                                          │
│  STORAGE (Tiered)                                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │ Hot Store   │  │ Warm Store  │  │ Cold Store  │                  │
│  │ (KQL DB)    │  │ (Delta Lake)│  │ (Archive)   │                  │
│  │ 7 days      │  │ 2 years     │  │ 7 years     │                  │
│  │ SSD         │  │ Standard    │  │ Cool tier   │                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Auto-scaling Configuration:**

```yaml
# IoT Hub auto-scaling
IoT_Hub:
  Tier: S2 (start)
  Auto_Scale_Rules:
    - Metric: Device-to-cloud messages
      Threshold: 80% of capacity
      Action: Scale to next tier
      Cooldown: 10 minutes
      
    - Metric: Device-to-cloud messages
      Threshold: 30% of capacity for 1 hour
      Action: Scale down
      Cooldown: 30 minutes

# Stream Analytics auto-scaling
Stream_Analytics:
  SU_Allocation:
    Minimum: 3
    Maximum: 48
    
  Scaling_Triggers:
    - Metric: CPU utilization
      Threshold: 75%
      Action: Add 3 SU
      
    - Metric: Backlogged input events
      Threshold: 1000
      Action: Add 6 SU
```

**Data Partitioning Strategy:**

```python
# Delta Lake partitioning for meter data
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("MeterPartitioning").getOrCreate()

# Read raw data
df = spark.read.format("delta").load("/bronze/meter_readings")

# Add partitioning columns
df_partitioned = df \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp"))) \
    .withColumn("meter_bucket", (col("meter_id").hashCode() % 100).abs())  # 100 buckets

# Write with partitioning
(df_partitioned
    .write
    .format("delta")
    .partitionBy("year", "month", "day", "meter_bucket")
    .mode("append")
    .save("/silver/meter_readings")
)

# Z-order for efficient queries
spark.sql("""
    OPTIMIZE delta.`/silver/meter_readings`
    ZORDER BY (meter_id, timestamp)
""")
```

**Cost Optimization by Volume Tier:**

| Daily Volume | Architecture | Monthly Cost | Cost per Million Records |
|--------------|--------------|--------------|------------------------|
| <1M | IoT Hub S1 + Stream Analytics 3 SU | £800 | £0.80 |
| 1-10M | IoT Hub S2 + Stream Analytics 6 SU + Fabric F64 | £2,500 | £0.25 |
| 10-50M | IoT Hub S2 + Stream Analytics 12 SU + Fabric F128 | £5,000 | £0.10 |
| 50-200M | Event Hubs Dedicated + Kubernetes + Fabric F256 | £12,000 | £0.06 |
| >200M | Custom architecture + reserved capacity | £25,000+ | £0.04 |

**Query Optimization:**

```sql
-- KQL Database query optimization
.set query_results_cache_max_age = time(1h)

-- Materialized view for common aggregations
.create materialized-view MeterDailyConsumption on table MeterReadings
{
    MeterReadings
    | summarize 
        DailyConsumption = sum(Consumption),
        ReadingCount = count(),
        AvgPressure = avg(Pressure)
        by MeterId, bin(Timestamp, 1d)
}

-- Query the materialized view (much faster)
MeterDailyConsumption
| where MeterId == "MTR-12345"
| where Timestamp between (startofday(ago(30d)) .. now())
| render timechart
```

### Trade-off Analysis

| Approach | Vertical Scaling | Horizontal Scaling | Recommendation |
|----------|------------------|-------------------|----------------|
| **Cost at scale** | Expensive | Efficient | Horizontal |
| **Complexity** | Low | High | Accept trade-off |
| **Availability** | Single point of failure | Resilient | Horizontal |
| **Performance** | Predictable | Requires tuning | Horizontal |

### Best Practices & Anti-Patterns

✅ **DO:**
- Implement **data lifecycle management** (hot → warm → cold)
- Use **partition pruning** for query performance
- Implement **backpressure handling** (don't lose data)
- Create **materialized views** for common aggregations
- Use **reserved capacity** for predictable workloads

❌ **DON'T:**
- Scale without monitoring (waste money)
- Use single partition key (hot spots)
- Store all data in hot tier (expensive)
- Ignore data compression (waste storage)
- Skip query optimization (poor UX)

---

## Question 10: Integration Architecture - Operational to Analytics

### Architecture Challenge

Business Stream has a complex operational landscape:
- **CIS (Customer Information System)**: SQL Server, customer master data
- **Billing System**: SQL Server, invoices, payments, tariffs
- **SCADA**: Real-time operational data, pressure, flow rates
- **CRM**: Dynamics 365, customer interactions
- **Third-party**: Wholesale water data, weather APIs
- **Legacy**: Mainframe systems for historical data

Design an integration architecture that:
1. Connects all operational systems to the analytics platform
2. Minimizes impact on operational systems (no performance degradation)
3. Provides near real-time data for operational analytics
4. Handles schema changes gracefully
5. Maintains data consistency across systems

### Detailed Component Recommendations

**Integration Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    INTEGRATION PLATFORM                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  OPERATIONAL SYSTEMS                                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │   CIS    │ │  Billing │ │  SCADA   │ │   CRM    │ │  Legacy  │ │
│  │SQL Server│ │SQL Server│ │  IoT     │ │Dynamics  │ │Mainframe │ │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ │
│       │            │            │            │            │        │
│       └────────────┴────────────┴────────────┴────────────┘        │
│                              │                                       │
│  INTEGRATION LAYER                                                   │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              AZURE DATA FACTORY + FABRIC                     │    │
│  │                                                              │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │    │
│  │  │   CDC    │  │  APIs    │  │ Streaming│  │  Files   │     │    │
│  │  │(SQL CDC) │  │(REST/SOAP│  │(IoT Hub) │  │ (SFTP)   │     │    │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘     │    │
│  │                                                              │    │
│  │  ┌──────────────────────────────────────────────────────┐   │    │
│  │  │              ENTERPRISE SERVICE BUS                   │   │    │
│  │  │  • Message routing  • Transformation  • Orchestration │   │    │
│  │  └──────────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│  ANALYTICS PLATFORM                                                  │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              MICROSOFT FABRIC                                │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │    │
│  │  │  Bronze  │→→│  Silver  │→→│   Gold   │→→│  Power   │     │    │
│  │  │  (Raw)   │  │(Cleansed)│  │(Business)│  │   BI     │     │    │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘     │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**CDC Implementation for SQL Server:**

```sql
-- Enable CDC on source database
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on specific tables
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'customers',
    @role_name = NULL,  -- No specific role required
    @supports_net_changes = 1;  -- Enable net changes

-- Query CDC changes
DECLARE @from_lsn BINARY(10), @to_lsn BINARY(10);
SET @from_lsn = sys.fn_cdc_get_min_lsn('dbo_customers');
SET @to_lsn = sys.fn_cdc_get_max_lsn();

SELECT * 
FROM cdc.fn_cdc_get_all_changes_dbo_customers(@from_lsn, @to_lsn, N'all');
```

**ADF Pipeline for CDC:**

```json
{
  "name": "CDC_Customer_Extract",
  "properties": {
    "activities": [
      {
        "name": "Get_CDC_Range",
        "type": "Lookup",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT sys.fn_cdc_get_max_lsn() as max_lsn"
          }
        }
      },
      {
        "name": "Extract_CDC_Changes",
        "type": "Copy",
        "dependsOn": [{ "activity": "Get_CDC_Range", "dependencyConditions": ["Succeeded"] }],
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": {
              "value": "SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_customers('@{activity('Get_Last_LSN').output.firstRow.last_lsn}', '@{activity('Get_CDC_Range').output.firstRow.max_lsn}', N'all')",
              "type": "Expression"
            }
          },
          "sink": {
            "type": "LakehouseSink",
            "location": {
              "type": "LakehouseLocation",
              "folderPath": "bronze/cdc/customers"
            },
            "format": {
              "type": "DeltaFormat"
            }
          }
        }
      },
      {
        "name": "Update_Watermark",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [{ "activity": "Extract_CDC_Changes", "dependencyConditions": ["Succeeded"] }],
        "typeProperties": {
          "storedProcedureName": "[etl].[sp_UpdateWatermark]",
          "storedProcedureParameters": {
            "table_name": { "value": "customers", "type": "String" },
            "last_lsn": { "value": "@{activity('Get_CDC_Range').output.firstRow.max_lsn}", "type": "String" }
          }
        }
      }
    ]
  }
}
```

**API Integration Pattern:**

```python
# Custom activity for API integration
import requests
import pandas as pd
from azure.identity import DefaultAzureCredential

def extract_weather_data(latitude, longitude, start_date, end_date):
    """Extract weather data from external API"""
    
    # Get token from Azure AD
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default")
    
    # Call weather API
    url = f"https://api.weather.gov/data/{latitude},{longitude}"
    headers = {"Authorization": f"Bearer {token.token}"}
    
    response = requests.get(url, headers=headers)
    data = response.json()
    
    # Transform to dataframe
    df = pd.DataFrame(data['properties']['periods'])
    
    # Add metadata
    df['extracted_at'] = pd.Timestamp.now()
    df['source'] = 'weather_api'
    
    return df

# Write to Bronze
(df.write
    .format("delta")
    .mode("append")
    .save("/bronze/external/weather")
)
```

**Schema Change Handling:**

```python
# Schema evolution handler
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("SchemaEvolution").getOrOrCreate()

# Enable schema evolution
df = spark.read.format("delta") \
    .option("mergeSchema", "true") \
    .load("/bronze/customers")

# Or use auto-merge
delta_table = DeltaTable.forPath(spark, "/bronze/customers")
delta_table.alias("target").merge(
    new_data_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### Trade-off Analysis

| Integration Pattern | Latency | Complexity | Impact on Source | Recommendation |
|--------------------|---------|-----------|------------------|----------------|
| **CDC** | Minutes | Medium | Low | Primary for SQL Server |
| **API Polling** | Hours | Low | Medium | For external APIs |
| **Streaming** | Seconds | High | Low | For IoT/SCADA |
| **Full Extract** | Daily | Low | High | For small tables only |
| **Event Sourcing** | Real-time | Very High | Low | For new systems |

### Best Practices & Anti-Patterns

✅ **DO:**
- Use **CDC** instead of full extracts (minimize source impact)
- Implement **watermark tables** for incremental loads
- Create **schema registries** for API contracts
- Use **dead letter queues** for failed messages
- Implement **circuit breakers** for external APIs

❌ **DON'T:**
- Query operational databases directly for analytics
- Skip error handling for external API calls
- Ignore schema changes (breaks pipelines)
- Use synchronous calls for long-running operations
- Skip data validation at integration points

---

## Question 11: Disaster Recovery and Business Continuity

### Architecture Challenge

Business Stream's data platform is critical for:
- **Billing**: £500M+ annual revenue processing
- **Regulatory**: Ofwat submissions with legal deadlines
- **Operations**: Leak detection affecting public safety
- **Customer Service**: 24/7 account access

Design a disaster recovery strategy that:
1. Meets RPO/RTO requirements by data criticality
2. Handles regional Azure outages
3. Provides tested recovery procedures
4. Minimizes data loss in worst-case scenarios
5. Complies with regulatory data residency (UK/EU)

### Detailed Component Recommendations

**DR Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DISASTER RECOVERY ARCHITECTURE                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  PRIMARY REGION (UK South)              SECONDARY REGION (UK West)   │
│  ┌─────────────────────────────┐        ┌─────────────────────────┐ │
│  │      PRODUCTION             │        │      STANDBY            │ │
│  │                             │        │                         │ │
│  │  ┌─────────────────────┐    │        │  ┌─────────────────┐    │ │
│  │  │   Microsoft Fabric  │    │◄──────►│  │ Microsoft Fabric│    │ │
│  │  │   • Lakehouse       │    │  Sync  │  │ • Replica       │    │ │
│  │  │   • Warehouses      │    │        │  │ • Read-only     │    │ │
│  │  │   • Pipelines       │    │        │  │   (failover)    │    │ │
│  │  └─────────────────────┘    │        │  └─────────────────┘    │ │
│  │                             │        │                         │ │
│  │  ┌─────────────────────┐    │        │  ┌─────────────────┐    │ │
│  │  │   SQL Server (IaaS) │    │◄──────►│  │ SQL Server      │    │ │
│  │  │   • Always On AG    │    │  Sync  │  │ • Secondary     │    │ │
│  │  │   • Synchronous     │    │        │  │   replica       │    │ │
│  │  └─────────────────────┘    │        │  └─────────────────┘    │ │
│  │                             │        │                         │ │
│  │  ┌─────────────────────┐    │        │  ┌─────────────────┐    │ │
│  │  │   Azure Data Factory│    │◄──────►│  │ Azure Data      │    │ │
│  │  │   • Pipelines       │    │  Deploy│  │   Factory       │    │ │
│  │  │   • Linked Services │    │        │  │ • ARM templates │    │ │
│  │  └─────────────────────┘    │        │  └─────────────────┘    │ │
│  └─────────────────────────────┘        └─────────────────────────┘ │
│                                                                      │
│  BACKUP STRATEGY                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  • OneLake: Geo-redundant storage (GRS)                     │    │
│  │  • SQL Server: Full daily + T-log every 15 min              │    │
│  │  • Key Vault: Soft-delete + purge protection                │    │
│  │  • ARM Templates: Azure DevOps Git repository               │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**RPO/RTO by Data Tier:**

```yaml
Tier_1_Critical:  # Billing, Customer PII
  Examples:
    - Customer billing data
    - Payment transactions
    - Active tariff configurations
  RPO: 15 minutes
  RTO: 1 hour
  Replication: Synchronous (Always On AG)
  Backup: T-log every 15 minutes
  
Tier_2_Important:  # Operational
  Examples:
    - Meter reading data (last 30 days)
    - Leak detection alerts
    - Customer service cases
  RPO: 4 hours
  RTO: 4 hours
  Replication: Asynchronous (Fabric replication)
  Backup: Incremental every 4 hours
  
Tier_3_Analytics:  # Historical, ML
  Examples:
    - Historical consumption data
    - ML model training data
    - Ad-hoc analysis datasets
  RPO: 24 hours
  RTO: 24 hours
  Replication: Backup-based recovery
  Backup: Daily full backup
```

**Fabric DR Implementation:**

```python
# Enable geo-redundancy for OneLake
# (Configured at workspace level in Fabric portal)

# Backup pipeline for critical tables
{
  "name": "DR_Backup_Critical_Tables",
  "properties": {
    "activities": [
      {
        "name": "Backup_Customer_Data",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "type": "LakehouseSource",
            "location": {
              "type": "LakehouseLocation",
              "folderPath": "gold/customer"
            }
          },
          "sink": {
            "type": "AzureBlobFSSink",
            "location": {
              "type": "AzureBlobFSLocation",
              "folderPath": "dr-backup/customer",
              "fileSystem": "backup"
            },
            "format": {
              "type": "DeltaFormat"
            }
          }
        }
      }
    ],
    "schedule": {
      "recurrence": {
        "frequency": "Hour",
        "interval": 4
      }
    }
  }
}
```

**SQL Server Always On Configuration:**

```sql
-- Create availability group for critical databases
CREATE AVAILABILITY GROUP [BusinessStream_AG]
WITH (
    AUTOMATED_BACKUP_PREFERENCE = SECONDARY,
    DB_FAILOVER = ON,
    DTC_SUPPORT = NONE
)
FOR DATABASE [CIS], [Billing], [CRM]
REPLICA ON
    N'UK-SOUTH-SQL-01' WITH (
        ENDPOINT_URL = N'TCP://UK-SOUTH-SQL-01.businessstream.local:5022',
        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
        FAILOVER_MODE = AUTOMATIC,
        BACKUP_PRIORITY = 50,
        SECONDARY_ROLE(ALLOW_CONNECTIONS = READ_ONLY)
    ),
    N'UK-WEST-SQL-01' WITH (
        ENDPOINT_URL = N'TCP://UK-WEST-SQL-01.businessstream.local:5022',
        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
        FAILOVER_MODE = AUTOMATIC,
        BACKUP_PRIORITY = 50,
        SECONDARY_ROLE(ALLOW_CONNECTIONS = READ_ONLY)
    );

-- Add listener for transparent failover
ALTER AVAILABILITY GROUP [BusinessStream_AG]
ADD LISTENER N'BusinessStream-AG-Listener' (
    WITH IP ((N'10.0.1.100', N'255.255.255.0'), (N'10.1.1.100', N'255.255.255.0')),
    PORT = 1433
);
```

**DR Testing Procedure:**

```yaml
Quarterly_DR_Test:
  Pre-Test:
    - Schedule maintenance window
    - Notify stakeholders
    - Prepare test scenarios
    
  Test_Execution:
    1. Simulate primary region failure
    2. Initiate failover to secondary
    3. Verify data accessibility
    4. Run critical business processes
    5. Measure actual RTO
    
  Validation:
    - Data consistency checks
    - Application connectivity tests
    - Performance benchmarks
    - User acceptance testing
    
  Post-Test:
    - Document findings
    - Update runbooks
    - Schedule remediation
    - Failback to primary
```

### Trade-off Analysis

| DR Strategy | Cost | RPO/RTO | Complexity | Recommendation |
|-------------|------|---------|------------|----------------|
| **Active-Active** | Very High | Near-zero | Very High | Not justified |
| **Warm Standby** | Medium | Hours | Medium | **Recommended** |
| **Cold Standby** | Low | Days | Low | Tier 3 only |
| **Backup Only** | Very Low | Days+ | Low | Not for Tier 1 |

### Best Practices & Anti-Patterns

✅ **DO:**
- Implement **automated failover** for critical systems
- Test DR procedures **quarterly** (not just annually)
- Document **runbooks** with step-by-step procedures
- Use **infrastructure as code** for reproducible environments
- Monitor **replication lag** (alert if > threshold)

❌ **DON'T:**
- Skip DR testing (untested DR is no DR)
- Assume cloud = automatic DR (must configure)
- Ignore data residency requirements (GDPR)
- Use same credentials in DR (security risk)
- Forget to test failback (DR is temporary)

---

## Question 12: Cost Optimization for Cloud Data Platforms

### Architecture Challenge

Business Stream's cloud data costs are growing 30% year-over-year:
- **Current monthly spend**: £45,000
- **Projected (without optimization)**: £75,000 in 18 months
- **Budget constraint**: Must stay under £50,000/month

The challenge: Design a cost optimization strategy that:
1. Reduces current spend without impacting service levels
2. Prevents future cost overruns as data grows
3. Implements chargeback/showback for accountability
4. Maintains performance for critical workloads
5. Provides visibility into cost drivers

### Detailed Component Recommendations

**Cost Optimization Framework:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    COST OPTIMIZATION FRAMEWORK                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              COST VISIBILITY                                 │   │
│  │  • Azure Cost Management + Billing                          │   │
│  │  • Fabric Capacity Metrics                                  │   │
│  │  • Custom cost allocation tags                              │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│   ┌──────────────────────────┼──────────────────────────┐           │
│   │                          │                          │           │
│   ▼                          ▼                          ▼           │
│ ┌──────────┐           ┌──────────┐           ┌──────────┐          │
│ │ COMPUTE  │           │ STORAGE  │           │ NETWORK  │          │
│ │OPTIMIZE  │           │OPTIMIZE  │           │OPTIMIZE  │          │
│ │          │           │          │           │          │          │
│ │ • Auto-  │           │ • Tiering│           │ • Data   │          │
│ │   scale  │           │ • Lifecycle│         │   locality│         │
│ │ • Reserved│          │ • Compression│        │ • CDN    │          │
│ │   capacity│          │ • Deduplication│      │          │          │
│ │ • Spot   │           │          │           │          │          │
│ │   instances│         │          │           │          │          │
│ └──────────┘           └──────────┘           └──────────┘          │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              GOVERNANCE & ACCOUNTABILITY                     │   │
│  │  • Cost centers and chargeback                              │   │
│  │  • Budget alerts and quotas                                 │   │
│  │  • Regular cost reviews                                     │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Current Cost Breakdown (Example):**

```
Monthly Spend: £45,000

┌────────────────────────────────────────────────────────────┐
│ Compute                                                    │
│ ├── Microsoft Fabric F128:           £18,000 (40%)        │
│ ├── Azure Data Factory:              £6,000  (13%)        │
│ ├── Azure SQL Database:              £5,000  (11%)        │
│ └── Stream Analytics:                £2,000  (4%)         │
│                                                            │
│ Storage                                                    │
│ ├── OneLake Storage:                 £8,000  (18%)        │
│ ├── SQL Database Storage:            £2,500  (6%)         │
│ └── Backup Storage:                  £1,500  (3%)         │
│                                                            │
│ Networking & Other                                         │
│ ├── Data Transfer:                   £1,500  (3%)         │
│ └── Monitoring, Key Vault, etc:      £500    (1%)         │
└────────────────────────────────────────────────────────────┘
```

**Optimization Strategies:**

**1. Fabric Capacity Optimization**

```yaml
# Right-size Fabric capacity
Current: F128 (128 CU) - £18,000/month
Optimized:
  - Production: F64 (64 CU) - £9,000/month
  - Development: F32 (32 CU) - £4,500/month (auto-pause nights/weekends)
  - Burst to F128 during month-end billing (pay-as-you-go)
  
Auto-Scale Rules:
  - Scale up: CPU > 80% for 10 minutes
  - Scale down: CPU < 30% for 30 minutes
  - Pause: No activity for 2 hours (dev only)
  
Savings: £4,500/month (25% of compute)
```

**2. Reserved Capacity**

```yaml
# 1-year reserved capacity for predictable workloads
Reserved_Instances:
  - Fabric F64: 100% reserved = 35% savings
  - SQL Database P4: 100% reserved = 40% savings
  - Data Factory: 50% reserved = 20% savings
  
Total_Commitment: £200,000/year
Savings: £70,000/year vs pay-as-you-go
Monthly_Savings: £5,800
```

**3. Storage Tiering**

```python
# Implement lifecycle management for OneLake
from azure.storage.filedatalake import DataLakeServiceClient

# Define lifecycle policy
lifecycle_policy = {
    "rules": [
        {
            "name": "BronzeToCool",
            "enabled": True,
            "type": "Lifecycle",
            "definition": {
                "filters": {
                    "prefixMatch": ["bronze/"],
                    "blobTypes": ["blockBlob"]
                },
                "actions": {
                    "baseBlob": {
                        "tierToCool": { "daysAfterModificationGreaterThan": 30 }
                    }
                }
            }
        },
        {
            "name": "ArchiveOldData",
            "enabled": True,
            "type": "Lifecycle",
            "definition": {
                "filters": {
                    "prefixMatch": ["bronze/", "silver/historical/"],
                    "blobTypes": ["blockBlob"]
                },
                "actions": {
                    "baseBlob": {
                        "tierToArchive": { "daysAfterModificationGreaterThan": 365 }
                    }
                }
            }
        }
    ]
}

# Apply policy
service_client = DataLakeServiceClient(
    account_url="https://businessstream.dfs.core.windows.net",
    credential=credential
)

service_client.set_service_properties(
    analytics_logging=None,
    hour_metrics=None,
    minute_metrics=None,
    cors=None,
    target_version=None,
    delete_retention_policy=None,
    static_website=None,
    encryption=None,
    **lifecycle_policy
)
```

**4. Data Compression**

```python
# Enable compression for Parquet files
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Compression").getOrCreate()

# Write with compression
(df.write
    .format("parquet")
    .option("compression", "zstd")  # Better than snappy for cold data
    .mode("append")
    .save("/silver/compressed_data")
)

# Expected savings: 60-70% for meter reading data
```

**5. Chargeback Implementation**

```yaml
# Cost allocation tags
Tagging_Strategy:
  Mandatory_Tags:
    - CostCenter: "CC-12345"  # Finance code
    - Environment: "Production|Development|Test"
    - Project: "SmartMeter|GDPR|Reporting"
    - Owner: "team@businessstream.co.uk"
    - DataClassification: "Public|Internal|Confidential|Restricted"
    
  Allocation_Rules:
    - Compute: Based on actual usage
    - Storage: Based on data ownership
    - Network: Based on data transfer source
    
  Reporting:
    - Monthly cost reports by cost center
    - Budget vs actual variance analysis
    - Anomaly detection for unexpected spikes
```

**Cost Monitoring Dashboard:**

```sql
-- Monthly cost by service
SELECT 
    ServiceName,
    ResourceGroup,
    Tags['CostCenter'] as CostCenter,
    SUM(Cost) as MonthlyCost,
    LAG(SUM(Cost)) OVER (PARTITION BY ServiceName ORDER BY BillingMonth) as PreviousMonth,
    (SUM(Cost) - LAG(SUM(Cost)) OVER (PARTITION BY ServiceName ORDER BY BillingMonth)) / 
        LAG(SUM(Cost)) OVER (PARTITION BY ServiceName ORDER BY BillingMonth) * 100 as PercentChange
FROM azure_cost_export
WHERE BillingMonth = '2024-01'
GROUP BY ServiceName, ResourceGroup, Tags['CostCenter'], BillingMonth
ORDER BY MonthlyCost DESC;
```

### Trade-off Analysis

| Optimization | Savings | Effort | Risk | Priority |
|--------------|---------|--------|------|----------|
| **Right-sizing** | 20-30% | Low | Low | 1 |
| **Reserved capacity** | 30-40% | Low | Medium | 2 |
| **Storage tiering** | 40-60% | Medium | Low | 3 |
| **Auto-pause** | 10-20% | Low | Low | 4 |
| **Data compression** | 50-70% | Medium | Low | 5 |
| **Query optimization** | 10-30% | High | Medium | 6 |

### Best Practices & Anti-Patterns

✅ **DO:**
- Implement **budget alerts** at 80%, 90%, 100% thresholds
- Use **cost allocation tags** from day one
- Review **cost reports weekly** (not monthly)
- Implement **auto-scaling** for variable workloads
- Use **spot instances** for non-critical batch jobs

❌ **DON'T:**
- Ignore dev/test environments (often 30% of spend)
- Over-provision "just in case" (waste money)
- Skip reserved capacity for predictable workloads
- Forget to delete unused resources
- Neglect data lifecycle management

---

## Summary: Key Architectural Principles

Throughout these 12 questions, several principles emerge as critical for Business Stream's data architecture:

### The Architecture Reveals the Organization's Soul

1. **Medallion Architecture**: Bronze-Silver-Gold provides clear separation of concerns and enables both regulatory compliance and advanced analytics

2. **Hybrid Processing**: Lambda architecture (batch + streaming) balances cost and latency requirements

3. **Governance by Design**: Data quality, lineage, and compliance must be built in, not bolted on

4. **Scalability Planning**: Design for 10x growth from day one, not just current needs

5. **Cost Consciousness**: Every architectural decision has a cost implication - optimize continuously

### Every Pipeline Tells a Story

The data architecture must support Business Stream's transformation from:
- **Descriptive** (what happened) → **Diagnostic** (why it happened) → **Predictive** (what will happen) → **Prescriptive** (what should we do)

This requires:
- Clean, governed data (Silver layer)
- Rich feature stores (Gold layer)
- Real-time capabilities (streaming)
- ML-ready infrastructure (Feature Store)

---

*End of Business Stream Data Architecture Interview Questions*
