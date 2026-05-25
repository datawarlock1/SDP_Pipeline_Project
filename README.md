# 🚀 Production-Grade Lakeflow SDP Pipeline - E-Commerce Orders

## Executive Summary

Enterprise-scale **Lakeflow Spark Declarative Pipeline (SDP)** implementing medallion architecture with advanced production patterns: automated deduplication via AUTO CDC, pre-validation quarantine, late-arriving data detection, customer lifecycle segmentation, and 24/7 autonomous monitoring with multi-tier recovery.

**Pipeline Status**: CONTINUOUS mode, real-time streaming  
**Data Governance**: Unity Catalog fully integrated  
**Observability**: Auto-recovery monitoring + production dashboard

---

## Table of Contents

1. [Architecture & Data Flow](#architecture--data-flow)
2. [Pipeline Configuration](#pipeline-configuration)
3. [Table Specifications](#table-specifications)
4. [Advanced Features Deep Dive](#advanced-features-deep-dive)
5. [Monitoring & Auto-Recovery](#monitoring--auto-recovery)
6. [Dashboard & Observability](#dashboard--observability)
7. [Troubleshooting Guide](#troubleshooting-guide)
8. [Project Structure](#project-structure)

---

## Architecture & Data Flow

### Medallion Architecture with Quality Gates

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION LAYER                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    /Volumes/main/default/demo_orders/*.json
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🟤 BRONZE LAYER - Raw Ingestion (STREAMING)                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  bronze_orders                                                  │    │
│  │  • Auto Loader: Incremental file ingestion                     │    │
│  │  • Schema inference + evolution                                │    │
│  │  • CDC metadata: ingestion_time, source_file, operation_type   │    │
│  │  • File lineage via _metadata enrichment                       │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
         ┌───────────────────────┐    ┌───────────────────────┐
         │   QUALITY GATE        │    │   QUARANTINE PATH     │
         │   (3 Constraints)     │    │   (Pre-Validation)    │
         └───────────────────────┘    └───────────────────────┘
                     │                             │
                     ▼                             ▼
┌────────────────────────────────┐   ┌────────────────────────────────┐
│  🥈 SILVER LAYER (STREAMING)   │   │  quarantine_orders (STREAMING) │
│  ┌──────────────────────────┐  │   │  • NULL_USER_ID                │
│  │  silver_orders           │  │   │  • INVALID_AMOUNT              │
│  │  • Valid records only    │  │   │  • NULL_ORDER_ID               │
│  │  • is_late_data flag     │  │   │  • Quarantine trending         │
│  │  • DROP ROW on violation │  │   └────────────────────────────────┘
│  └──────────────────────────┘  │
└────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  DEDUPLICATION LAYER  │
         │  (AUTO CDC FLOW)      │
         └───────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│  silver_orders_deduped (STREAMING)                                     │
│  • AUTO CDC INTO with SCD Type 1                                       │
│  • KEYS (order_id), SEQUENCE BY order_timestamp                        │
│  • Keeps latest record per order_id                                    │
│  • Feeds all downstream gold tables                                    │
└────────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER - Business Analytics (MATERIALIZED VIEWS)               │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  gold_user_dimension                                          │    │
│  │  • Customer segmentation: VIP/GOLD/SILVER/BRONZE              │    │
│  │  • Lifecycle status: Active/At Risk/Churned                   │    │
│  │  • Lifetime value & order metrics                             │    │
│  └──────────────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  gold_hourly_metrics                                          │    │
│  │  • Hourly aggregations: revenue, orders, active users         │    │
│  │  • Late data tracking: count + percentage                     │    │
│  └──────────────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  gold_daily_user_sales + gold_product_performance             │    │
│  │  • Daily user sales metrics                                   │    │
│  │  • Product performance KPIs                                   │    │
│  └──────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│  📊 OBSERVABILITY LAYER (MATERIALIZED VIEWS)                           │
│  • cdc_audit_log: Complete change tracking                             │
│  • cdc_metrics_summary: Operation type aggregations                    │
│  • data_quality_metrics: Ingestion health by hour                      │
│  • quarantine_summary: Quality issue trending                          │
└────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Metrics (Typical)
```
📥 Bronze Ingestion:     1,150 records
❌ Quarantined:            59 records (5.1% rejection)
✅ Silver Validated:     1,091 records
🔄 Deduped Silver:       1,091 records (no duplicates in demo)
📊 Gold Users:             533 aggregated user records
📊 Gold Products:          200 product performance records
⏱️  Processing Latency:    3-5 seconds (bronze → silver)
```

---

## Pipeline Configuration

| Property | Value |
| --- | --- |
| **Pipeline ID** | `1187c00e-e560-40b4-89c3-87263de49d9b` |
| **Pipeline Name** | `demo_ecommerce_orders_pipeline` |
| **Execution Mode** | `CONTINUOUS` (real-time streaming) |
| **Catalog** | `main` |
| **Target Schema** | `demo_sdp` |
| **Source Location** | `/Volumes/main/default/demo_orders` |
| **Transformations File** | `/Workspace/Users/mohitpdh285@gmail.com/demo_sdp_pipeline/transformations.sql` |
| **Dashboard ID** | `01f149fd7da51c65b72a29dc25ab3fe4` |
| **Cluster Mode** | Serverless (auto-scaling) |
| **Unity Catalog** | Enabled (full governance) |

---

## Table Specifications

### Layer 1: Bronze - Raw Ingestion

#### `main.demo_sdp.bronze_orders` (STREAMING TABLE)

**Purpose**: Incremental ingestion of raw JSON order files using Auto Loader.

**Key Columns**:
* `order_id`, `user_id`, `product_id`, `amount`, `status`, `order_timestamp`, `created_at` — Source data fields
* `ingestion_time` — `current_timestamp()` at ingestion (used for latency calculation)
* `source_file` — `_metadata.file_path` for lineage tracking
* `file_modified_time` — `_metadata.file_modification_time` for file-level audit
* `operation_type` — Hardcoded `'INSERT'` for CDC tracking

**Auto Loader Configuration**:
```sql
FROM STREAM read_files(
  '/Volumes/main/default/demo_orders',
  format => 'json',
  inferColumnTypes => true  -- Automatic schema inference
)
```

**Production Pattern**: Auto Loader handles schema evolution automatically. New fields in JSON files are added to the table without pipeline code changes.

---

### Layer 1b: Quarantine - Quality Violations

#### `main.demo_sdp.quarantine_orders` (STREAMING TABLE)

**Purpose**: Pre-validation capture of records that will fail silver layer constraints. Enables root cause analysis before data is dropped.

**Quarantine Logic**:
```sql
WHERE user_id IS NULL        → 'NULL_USER_ID'
   OR amount <= 0            → 'INVALID_AMOUNT'
   OR order_id IS NULL       → 'NULL_ORDER_ID'
```

**Why This Matters**: Without quarantine, violated records are silently dropped at silver layer. With quarantine, data engineers can:
1. Investigate data quality issues by querying `quarantine_orders`
2. Track trending quality problems via `quarantine_summary`
3. Fix upstream data sources or adjust validation rules
4. Potentially recover quarantined records with manual intervention

**Key Columns**:
* `quarantine_reason` — Classification of quality issue
* `quarantine_time` — Timestamp for SLA tracking
* `source_file` — Identifies problematic data files

---

### Layer 2: Silver - Validated Data

#### `main.demo_sdp.silver_orders` (STREAMING TABLE with CONSTRAINTS)

**Purpose**: Quality-validated orders with business rules enforcement.

**Data Quality Constraints**:
```sql
CONSTRAINT valid_user_id 
  EXPECT (user_id IS NOT NULL) 
  ON VIOLATION DROP ROW

CONSTRAINT valid_amount 
  EXPECT (amount > 0) 
  ON VIOLATION DROP ROW

CONSTRAINT valid_order_id 
  EXPECT (order_id IS NOT NULL) 
  ON VIOLATION DROP ROW
```

**Constraint Enforcement**: `DROP ROW` policy immediately discards violating records. Alternatives:
* `FAIL UPDATE` — Stops pipeline on any violation (strictest)
* `DROP ROW` — Continues processing, drops bad rows (production standard)
* `QUARANTINE` — Future SDP feature (not yet available)

**Late-Arriving Data Detection**:
```sql
CASE 
  WHEN TIMESTAMPDIFF(HOUR, CAST(order_timestamp AS TIMESTAMP), current_timestamp()) > 24 
  THEN true 
  ELSE false 
END as is_late_data
```

**Why Track Late Data**: Orders with `order_timestamp` more than 24 hours in the past indicate:
1. Upstream system delays or batch data loads
2. Backfill operations
3. Potential data quality issues

Late data impacts downstream aggregations (e.g., daily sales reports) and may require special handling.

**Key Columns**:
* `processed_time` — Silver layer processing timestamp (for latency calculation)
* `is_late_data` — Boolean flag for SLA monitoring

---

### Layer 2b: Silver Deduplication

#### `main.demo_sdp.silver_orders_deduped` (STREAMING TABLE via AUTO CDC)

**Purpose**: Remove duplicate orders using SDP's AUTO CDC feature with SCD Type 1.

**Implementation**:
```sql
CREATE OR REFRESH STREAMING TABLE silver_orders_deduped;

CREATE FLOW silver_dedup_flow AS AUTO CDC INTO silver_orders_deduped
FROM STREAM(silver_orders)
KEYS (order_id)                    -- Unique identifier
SEQUENCE BY order_timestamp        -- Ordering for conflict resolution
STORED AS SCD TYPE 1;              -- Keep latest, discard history
```

**AUTO CDC Deep Dive**:

**What is AUTO CDC?**  
SDP's automated Change Data Capture feature that handles upserts, deletes, and deduplication without writing manual merge logic.

**How It Works**:
1. **KEYS (order_id)**: Defines the primary key for uniqueness
2. **SEQUENCE BY order_timestamp**: When duplicate `order_id` values arrive, keeps the record with the latest `order_timestamp`
3. **SCD TYPE 1**: Overwrites existing records (no history tracking)

**Alternative: SCD TYPE 2** (if you need history):
```sql
STORED AS SCD TYPE 2;  -- Adds __START_AT, __END_AT, __IS_CURRENT columns
```

**Why Deduplication Matters**: Real-world data sources often send duplicates due to:
* Retry logic in upstream systems
* Partial file re-processing
* Multiple data sources producing the same orders

Without deduplication, downstream aggregations (e.g., total revenue) would be inflated.

**Production Note**: AUTO CDC requires the source table (`silver_orders`) to already exist before the FLOW is created. The pipeline automatically handles this dependency ordering.

---

### Layer 3: Gold - Business Analytics

#### `main.demo_sdp.gold_user_dimension` (MATERIALIZED VIEW)

**Purpose**: Customer lifecycle analytics with RFM-style segmentation.

**Customer Tier Logic** (Value-Based Segmentation):
```sql
CASE 
  WHEN SUM(amount) > 10000 THEN 'VIP'      -- High-value customers
  WHEN SUM(amount) > 5000  THEN 'GOLD'     -- Medium-high value
  WHEN SUM(amount) > 1000  THEN 'SILVER'   -- Medium value
  ELSE 'BRONZE'                             -- Low value
END as customer_tier
```

**Customer Status** (Recency-Based):
```sql
CASE 
  WHEN DATEDIFF(current_date(), MAX(DATE(order_timestamp))) <= 30  THEN 'Active'    -- Recent purchase
  WHEN DATEDIFF(current_date(), MAX(DATE(order_timestamp))) <= 90  THEN 'At Risk'   -- Churning
  ELSE 'Churned'                                                                      -- Inactive >90 days
END as customer_status
```

**Use Cases**:
* Marketing campaign targeting (e.g., re-engagement for "At Risk" customers)
* Personalized offers based on `customer_tier`
* Churn prediction model features
* Customer lifetime value (CLV) reporting

**Key Metrics**:
* `lifetime_value` — Total spend across all orders
* `lifetime_orders` — Total order count
* `avg_order_value` — Average spend per order
* `first_order_date` / `last_order_date` — Tenure tracking

---

#### `main.demo_sdp.gold_hourly_metrics` (MATERIALIZED VIEW)

**Purpose**: Time-series aggregations for operational monitoring.

**Why Hourly Instead of Real-Time Windows?**  
Materialized views (batch aggregations) are more efficient for historical reporting. For real-time metrics, use STREAMING tables with `window()` functions.

**Late Data Percentage Calculation**:
```sql
ROUND(SUM(CASE WHEN is_late_data THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as late_data_pct
```

**Dashboard Use Cases**:
* **Throughput Monitoring**: `orders_count` over time
* **Revenue Tracking**: `total_revenue` by hour
* **SLA Compliance**: `late_data_pct` should be < 5% (configurable threshold)
* **User Engagement**: `active_users` trending

---

#### `main.demo_sdp.gold_daily_user_sales` (MATERIALIZED VIEW)

**Purpose**: Daily user-level sales aggregations for personalized reporting.

**Query Pattern**: Optimized for queries like "Show me User 12345's daily sales for the last 30 days."

**Key Columns**:
* `order_date` — Partitioning key for efficient queries
* `user_id` — User identifier
* `total_sales`, `total_orders`, `avg_order_value` — Daily metrics
* `unique_products` — Product diversity per user per day

---

#### `main.demo_sdp.gold_product_performance` (MATERIALIZED VIEW)

**Purpose**: Product-level KPIs for inventory and merchandising teams.

**Key Metrics**:
* `total_orders` — Product popularity
* `total_revenue` — Revenue contribution
* `avg_price` — Average selling price
* `unique_customers` — Customer reach

**Use Cases**:
* Identify best-selling products
* Detect low-performing inventory
* Price optimization analysis
* Cross-sell recommendations

---

### Layer 4: Observability Tables

#### `main.demo_sdp.cdc_audit_log` (STREAMING TABLE)

**Purpose**: Complete audit trail of all changes for compliance and debugging.

**Key Columns**:
* `operation_type` — Always 'INSERT' in this demo (could be UPDATE/DELETE with real CDC)
* `processing_date`, `processing_hour` — Time-based partitioning for efficient queries
* `source_file` — File-level lineage

**Compliance Use Cases**:
* "Which file produced Order X?"
* "When was Order Y processed?"
* "How many records were processed on Date Z?"

---

#### `main.demo_sdp.cdc_metrics_summary` (MATERIALIZED VIEW)

**Purpose**: Aggregated CDC metrics for dashboard visualization.

**Aggregation**:
```sql
GROUP BY processing_date, processing_hour, operation_type
```

**Dashboard Use**: Powers "CDC Operations Over Time" line chart showing INSERT/UPDATE/DELETE trends.

---

#### `main.demo_sdp.data_quality_metrics` (MATERIALIZED VIEW)

**Purpose**: Ingestion health monitoring by hour.

**Key Metrics**:
* `total_records_ingested` — Volume tracking
* `files_processed` — File count
* `first_record_time` / `last_record_time` — Ingestion window

**SLA Monitoring**: Alert if `files_processed = 0` for 2+ consecutive hours (indicates ingestion failure).

---

#### `main.demo_sdp.quarantine_summary` (MATERIALIZED VIEW)

**Purpose**: Quality issue trending for root cause analysis.

**Aggregation**:
```sql
GROUP BY quarantine_date, quarantine_reason
```

**Alert Triggers**:
* Sudden spike in `quarantine_count` → Investigate upstream data source
* New `quarantine_reason` appears → Schema change or new data quality issue

---

## Advanced Features Deep Dive

### 1. AUTO CDC Deduplication

**Problem**: Streaming data sources often produce duplicate records due to at-least-once delivery guarantees.

**Traditional Solution** (Manual Deduplication):
```sql
-- ❌ DOESN'T WORK in Structured Streaming
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp DESC) as rn
  FROM silver_orders
) WHERE rn = 1
```
**Error**: `Window function is not supported in ROW_NUMBER() on streaming DataFrames`

**SDP Solution** (AUTO CDC):
```sql
-- ✅ WORKS with AUTO CDC
CREATE FLOW silver_dedup_flow AS AUTO CDC INTO silver_orders_deduped
FROM STREAM(silver_orders)
KEYS (order_id)
SEQUENCE BY order_timestamp
STORED AS SCD TYPE 1;
```

**How AUTO CDC Works**:
1. Maintains a state store with latest `order_timestamp` for each `order_id`
2. On new record arrival:
   * If `order_id` is new → INSERT
   * If `order_id` exists and new `order_timestamp` > old → UPDATE (SCD Type 1) or version (SCD Type 2)
   * If `order_id` exists and new `order_timestamp` <= old → IGNORE
3. State store is checkpointed for fault tolerance

**Performance**: AUTO CDC uses RocksDB for state management, efficiently handling millions of unique keys.

---

### 2. Pre-Validation Quarantine Pattern

**Traditional Approach**: Apply constraints directly, lose visibility into rejected records.

**Quarantine Pattern**:
```
Bronze → Quarantine (captures violations)
  ↓
Silver (applies constraints, drops violations)
```

**Benefits**:
1. **Root Cause Analysis**: Query `quarantine_orders` to see why records failed
2. **Data Quality Trending**: Track `quarantine_reason` over time
3. **Recovery**: Manually fix and re-inject quarantined records if validation rules were too strict
4. **Alerting**: Trigger alerts when quarantine rate exceeds threshold

**Implementation Detail**: Quarantine table streams from `bronze_orders` in parallel with `silver_orders`. Both tables process the same source data independently.

---

### 3. Late-Arriving Data Detection

**Definition**: Records where `order_timestamp` is significantly older than `current_timestamp()` at ingestion.

**Why It Happens**:
* Upstream system delays (batch jobs, network issues)
* Backfill operations
* Time zone misconfigurations

**Impact on Aggregations**:
```
Scenario: Daily sales report generated at 8 AM
Late Data: Order with order_timestamp = "2 days ago" arrives at 8 AM today
Issue: Yesterday's sales report is now incorrect (missing this order)
```

**Handling Late Data**:
1. **Watermarking**: Use `withWatermark()` to define acceptable lateness window
2. **Restatement**: Re-run aggregations for past dates when late data arrives
3. **Separate Metrics**: Track late data separately for auditing

**This Pipeline's Approach**: Flag late data with `is_late_data` column, track percentage in `gold_hourly_metrics`.

---

### 4. Customer Lifecycle Segmentation

**RFM Analysis** (Recency, Frequency, Monetary):
* **Recency**: `customer_status` based on `last_order_date`
* **Frequency**: `lifetime_orders` count
* **Monetary**: `lifetime_value` used for `customer_tier`

**Business Logic**:
```
VIP Customers (>$10k lifetime):
  - Dedicated account manager
  - Priority support
  - Exclusive offers

At Risk Customers (>90 days since last order):
  - Re-engagement email campaigns
  - Win-back promotions
  - Feedback surveys
```

**Incremental Updates**: Materialized views are incrementally updated when `silver_orders_deduped` changes, ensuring real-time segmentation.

---

## Monitoring & Auto-Recovery

### Monitoring Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  MONITORING NOTEBOOK                                         │
│  (03_Pipeline_Monitoring_AutoRecovery.py)                   │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌────────────────┐  ┌────────────────┐  ┌────────────────┐
│ Health Check   │  │ Data Freshness │  │  Auto-Recovery │
│  (Pipeline)    │  │  (Tables)      │  │   (Restart)    │
└────────────────┘  └────────────────┘  └────────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│  Databricks Pipeline API + SQL Warehouse                    │
└─────────────────────────────────────────────────────────────┘
```

### Function Reference

#### 1. `check_pipeline_health(pipeline_id)` → dict

**Returns**:
```python
{
  'pipeline_name': str,
  'state': str,              # RUNNING, IDLE, FAILED, STOPPING
  'is_healthy': bool,        # True if RUNNING or IDLE (for CONTINUOUS mode)
  'last_update_state': str,  # COMPLETED, FAILED, QUEUED
  'errors': list[str]        # Error messages if any
}
```

**Health Definition for CONTINUOUS Mode**:
* ✅ Healthy: `state` in ['RUNNING', 'IDLE']
* ❌ Unhealthy: `state` in ['FAILED', 'STOPPING'] or `last_update_state == 'FAILED'`

**Note**: CONTINUOUS pipelines alternate between RUNNING (processing data) and IDLE (waiting for new data). Both are healthy states.

---

#### 2. `check_data_freshness(table, timestamp_col, max_lag_minutes)` → dict

**Returns**:
```python
{
  'table': str,
  'latest_data_time': datetime,
  'lag_minutes': float,
  'is_fresh': bool,          # True if lag_minutes <= max_lag_minutes
  'alert': str               # Human-readable status message
}
```

**Use Cases**:
```python
# Alert if silver_orders hasn't received data in 30 minutes
freshness = check_data_freshness(
    'main.demo_sdp.silver_orders',
    'processed_time',
    max_lag_minutes=30
)
if not freshness['is_fresh']:
    send_pagerduty_alert(freshness['alert'])
```

**Recommended Thresholds**:
* Real-time pipelines: 15-30 minutes
* Near-real-time: 1-2 hours
* Batch pipelines: 24 hours

---

#### 3. `attempt_pipeline_recovery(pipeline_id, strategy)` → dict

**Strategies**:
1. **'restart'** (Soft Restart):
   * Keeps streaming checkpoints
   * Resumes from last processed offset
   * Fast recovery (~30 seconds)
   * Use for: Transient errors, OOM, network issues

2. **'full_refresh'** (Hard Restart):
   * Deletes all tables and checkpoints
   * Reprocesses all source data from scratch
   * Slow recovery (minutes to hours depending on data volume)
   * Use for: Corrupted state, schema changes, data quality rules changed

**Returns**:
```python
{
  'strategy': str,
  'success': bool,
  'message': str,
  'recovery_time_seconds': float
}
```

**Production Pattern**: Always try 'restart' first, escalate to 'full_refresh' only after multiple restart failures.

---

#### 4. `display_pipeline_metrics(pipeline_id)` → None

**Displays**:
```
=== PIPELINE HEALTH ===
Status: ✅ RUNNING
Last Update: ✅ COMPLETED

=== DATA FLOW ===
Bronze:            1,150 records
Silver:            1,091 records
Gold Users:          533 records
Gold Products:       200 records
Rejection Rate:     5.13%

=== DATA FRESHNESS ===
silver_orders:     ✅ 2.3 minutes ago (FRESH)
gold_user_dim:     ✅ 5.1 minutes ago (FRESH)
```

**Use Case**: Quick visual health check during on-call incidents.

---

#### 5. `monitor_pipeline(pipeline_id, check_interval_seconds, auto_recover, max_iterations)` → None

**Continuous Monitoring Loop**:
```python
while iteration < max_iterations:
    health = check_pipeline_health(pipeline_id)
    
    if not health['is_healthy']:
        if auto_recover:
            recovery = attempt_pipeline_recovery(pipeline_id, 'restart')
            if not recovery['success'] and attempt_count >= 3:
                attempt_pipeline_recovery(pipeline_id, 'full_refresh')
    
    sleep(check_interval_seconds)
```

**Auto-Recovery Logic** (3-Tier):
1. **Attempt 1**: Soft restart (keeps checkpoints)
2. **Attempt 2**: Soft restart (retry, maybe it was transient)
3. **Attempt 3**: Full refresh (nuclear option, reprocess everything)
4. **After 3 failures**: Stop auto-recovery, alert on-call engineer

**Stabilization Wait**: 2 minutes between recovery attempts to allow pipeline to stabilize.

**Production Deployment**:
```python
# Run in scheduled notebook job (e.g., cron: */5 * * * *)
monitor_pipeline(
    pipeline_id='1187c00e-e560-40b4-89c3-87263de49d9b',
    check_interval_seconds=300,      # 5 minutes
    auto_recover=True,
    max_iterations=None              # Run forever
)
```

**MTTR Impact**: Reduces Mean Time To Recovery from hours (manual intervention) to minutes (automated restart).

---

## Dashboard & Observability

### Dashboard Overview

**Dashboard ID**: `01f149fd7da51c65b72a29dc25ab3fe4`  
**Dashboard Name**: SDP Pipeline - Production Observability Dashboard  
**Status**: ✅ Active (8 widgets configured)

**Access Dashboard**: [SDP Pipeline - Production Observability Dashboard](#dashboard-01f149fd7da51c65b72a29dc25ab3fe4)

**Layout**: 2x4 grid (8 widgets total)

---

### Dashboard Widgets

#### Widget 1: Throughput Counter
* **Type**: Counter (single value)
* **Metric**: Records processed in last 5 minutes
* **Query**: 
```sql
SELECT COUNT(*) as records 
FROM main.demo_sdp.silver_orders 
WHERE processed_time >= CURRENT_TIMESTAMP() - INTERVAL 5 MINUTES
```
* **Alerting**: Alert if < 10 records/5min (indicates ingestion slowdown)
* **Business Context**: Real-time health indicator for data ingestion pipeline

---

#### Widget 2: Data Flow Sanity Check
* **Type**: Bar Chart (horizontal)
* **Metric**: Record counts across all pipeline layers
* **Query**:
```sql
SELECT 'Bronze' as layer, COUNT(*) as records FROM main.demo_sdp.bronze_orders
UNION ALL SELECT 'Quarantine', COUNT(*) FROM main.demo_sdp.quarantine_orders
UNION ALL SELECT 'Silver', COUNT(*) FROM main.demo_sdp.silver_orders
UNION ALL SELECT 'Silver Deduped', COUNT(*) FROM main.demo_sdp.silver_orders_deduped
UNION ALL SELECT 'Gold Users', COUNT(*) FROM main.demo_sdp.gold_user_dimension
UNION ALL SELECT 'Gold Products', COUNT(*) FROM main.demo_sdp.gold_product_performance
```
* **Expected Pattern**: Bronze > Silver > Quarantine (small percentage), Gold << Silver (aggregated)
* **Anomaly Detection**: If Silver > Bronze → duplicate detection issue; If Quarantine > 10% of Bronze → data quality problem

---

#### Widget 3: Processing Latency
* **Type**: Counter with trend (sparkline)
* **Metric**: Average Bronze → Silver processing latency (seconds)
* **Query**: 
```sql
SELECT AVG(TIMESTAMPDIFF(SECOND, ingestion_time, processed_time)) as avg_latency_seconds 
FROM main.demo_sdp.silver_orders 
WHERE processed_time >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
```
* **SLA Targets**: 
  * Real-time tier: < 10 seconds
  * Near-real-time tier: < 60 seconds
  * Batch tier: < 5 minutes
* **Root Cause Indicators**: Latency spike indicates cluster resource contention or backpressure

---

#### Widget 4: Quality Rejection Rate
* **Type**: Counter (percentage with trend)
* **Metric**: Percentage of records quarantined due to quality violations
* **Query**:
```sql
SELECT ROUND(
  (SELECT COUNT(*) FROM main.demo_sdp.quarantine_orders) * 100.0 / 
  (SELECT COUNT(*) FROM main.demo_sdp.bronze_orders), 2
) as rejection_rate_pct
```
* **Baseline**: ~5% (expected for demo data with intentional quality issues)
* **Alert Threshold**: > 10% indicates upstream data quality degradation
* **Action**: When triggered, query `quarantine_summary` to identify trending `quarantine_reason`

---

#### Widget 5: Customer Tier Distribution
* **Type**: Pie Chart (with percentages)
* **Metric**: Customer segmentation breakdown by lifetime value
* **Query**: 
```sql
SELECT customer_tier, COUNT(*) as customer_count 
FROM main.demo_sdp.gold_user_dimension 
GROUP BY customer_tier 
ORDER BY 
  CASE customer_tier 
    WHEN 'VIP' THEN 1 
    WHEN 'GOLD' THEN 2 
    WHEN 'SILVER' THEN 3 
    WHEN 'BRONZE' THEN 4 
  END
```
* **Business Use**: Track VIP customer growth over time, measure marketing campaign effectiveness
* **Expected Distribution** (demo data):
  * VIP: ~5%
  * GOLD: ~10%
  * SILVER: ~25%
  * BRONZE: ~60%

---

#### Widget 6: Quarantine Trending
* **Type**: Line Chart (multi-series by quarantine_reason)
* **Metric**: Quality issues over time by violation type
* **Query**: 
```sql
SELECT quarantine_date, quarantine_reason, quarantine_count 
FROM main.demo_sdp.quarantine_summary 
ORDER BY quarantine_date DESC 
LIMIT 100
```
* **Analysis Patterns**:
  * **Sudden spike**: Upstream system bug or schema change
  * **Gradual increase**: Data quality degradation over time
  * **New reason appears**: New validation rule added or upstream data source changed
* **Actionable Insights**: Drill into specific dates with spikes to identify root cause

---

#### Widget 7: Late Data SLA
* **Type**: Counter (percentage with color thresholds)
* **Metric**: Percentage of orders arriving >24 hours late
* **Query**: 
```sql
SELECT late_data_pct 
FROM main.demo_sdp.gold_hourly_metrics 
ORDER BY metric_date DESC, metric_hour DESC 
LIMIT 1
```
* **Target SLA**: < 5% late data (configurable by business requirements)
* **Color Thresholds**:
  * Green: < 5% (within SLA)
  * Yellow: 5-10% (warning)
  * Red: > 10% (SLA breach)
* **Root Causes**: Batch data loads, upstream system delays, backfill operations

---

#### Widget 8: Hourly Performance Table
* **Type**: Table (scrollable, with conditional formatting)
* **Metric**: Last 24 hours operational metrics
* **Query**: 
```sql
SELECT 
  metric_date, 
  metric_hour, 
  orders_count, 
  total_revenue, 
  active_users, 
  products_sold, 
  late_data_count,
  late_data_pct
FROM main.demo_sdp.gold_hourly_metrics 
ORDER BY metric_date DESC, metric_hour DESC 
LIMIT 24
```
* **Use Cases**:
  * **Operational drill-down**: Identify specific hours with anomalies
  * **Trend analysis**: Compare hour-over-hour metrics
  * **Capacity planning**: Peak hour identification for resource allocation
* **Key Columns**:
  * `orders_count`: Throughput metric (orders/hour)
  * `total_revenue`: Business impact metric
  * `active_users`: User engagement metric
  * `late_data_count`: Data quality metric (absolute count)

---

### Dashboard Refresh Strategy

**Current**: Manual refresh (user clicks refresh button)

**Recommended Production Setup**:
1. **Auto-refresh**: Configure dashboard to auto-refresh every 5 minutes
2. **Scheduled snapshots**: Export dashboard metrics to a historical table daily for trend analysis
3. **Alert integration**: Configure alerts in widgets 1, 4, and 7 to trigger PagerDuty/Slack notifications

**Query Performance**:
* All widgets query materialized views (fast, <1 second)
* Exception: Widget 1 (throughput) queries streaming table directly (2-3 seconds)
* Optimization: Cache dashboard results for 1 minute to reduce warehouse load

---

## Troubleshooting Guide

### Issue 1: Pipeline Stuck in RUNNING but No Data Flowing

**Symptoms**:
* Pipeline state = RUNNING
* `check_data_freshness()` shows stale data (> 1 hour lag)
* No errors in pipeline logs

**Diagnosis**:
```python
# Check if source files exist
%sh ls /Volumes/main/default/demo_orders/

# Check last processed file (look at bronze_orders.source_file)
%sql
SELECT MAX(source_file), MAX(ingestion_time) 
FROM main.demo_sdp.bronze_orders
```

**Causes**:
1. No new files in source location → Expected behavior (pipeline is idle)
2. Files exist but not being processed → Check Auto Loader schema inference (might be rejecting files due to schema mismatch)

**Resolution**:
```python
# Option 1: Trigger processing by adding a new file
# Option 2: Check pipeline event log for schema inference errors
```

---

### Issue 2: High Quarantine Rate (> 10%)

**Symptoms**:
* `quarantine_summary` shows spike in quarantine_count
* Specific `quarantine_reason` dominates

**Diagnosis**:
```sql
SELECT quarantine_reason, COUNT(*), ARRAY_AGG(source_file) as files
FROM main.demo_sdp.quarantine_orders
WHERE DATE(quarantine_time) = CURRENT_DATE()
GROUP BY quarantine_reason
```

**Causes & Fixes**:

| Reason | Cause | Fix |
|--------|-------|-----|
| `NULL_USER_ID` | Upstream anonymization bug | Fix source system, backfill data |
| `INVALID_AMOUNT` | Negative amounts in refunds | Adjust constraint to `amount <> 0` |
| `NULL_ORDER_ID` | Data corruption | Investigate source file, re-ingest clean data |

**Recovery**:
```sql
-- After fixing upstream issue, manually recover quarantined records
INSERT INTO main.demo_sdp.silver_orders
SELECT * FROM main.demo_sdp.quarantine_orders
WHERE quarantine_reason = 'INVALID_AMOUNT' AND amount IS NOT NULL
```

---

### Issue 3: AUTO CDC Deduplication Not Working

**Symptoms**:
* `silver_orders_deduped` has same count as `silver_orders`
* Expected deduplication but not happening

**Diagnosis**:
```sql
-- Check for actual duplicates
SELECT order_id, COUNT(*) as dup_count
FROM main.demo_sdp.silver_orders
GROUP BY order_id
HAVING COUNT(*) > 1
```

**Causes**:
1. **No Duplicates in Source Data** → Expected behavior (demo data might not have duplicates)
2. **SEQUENCE BY field is always increasing** → AUTO CDC sees each record as "newer" and keeps all
3. **KEYS definition incorrect** → Deduplicating on wrong field

**Resolution**:
```sql
-- Verify FLOW configuration
DESCRIBE DETAIL main.demo_sdp.silver_orders_deduped;
-- Check properties for KEYS and SEQUENCE_BY settings
```

---

### Issue 4: Materialized View Not Updating

**Symptoms**:
* `gold_user_dimension.last_updated` timestamp is stale
* New orders in `silver_orders_deduped` not reflected in aggregations

**Diagnosis**:
```sql
-- Check materialized view refresh status
DESCRIBE EXTENDED main.demo_sdp.gold_user_dimension;
-- Look for "Refresh Status" and "Last Refresh Time"
```

**Causes**:
1. **Materialized views are lazy** → Only refresh when queried (by default)
2. **Refresh failed** → Check pipeline event log for MV refresh errors

**Resolution**:
```sql
-- Manual refresh
REFRESH MATERIALIZED VIEW main.demo_sdp.gold_user_dimension;

-- Or configure automatic refresh (future SDP feature)
-- Currently, MVs refresh incrementally when source tables change
```

---

### Issue 5: Pipeline Fails with "Checkpoint Corruption"

**Symptoms**:
* Pipeline update fails immediately
* Error message: "Checkpoint location is corrupted"

**Diagnosis**:
```python
# Check pipeline latest update
health = check_pipeline_health('1187c00e-e560-40b4-89c3-87263de49d9b')
print(health['errors'])
```

**Cause**: Checkpoint state store (RocksDB) corruption due to:
* Unclean shutdown
* Disk full during checkpoint write
* Concurrent writes to same checkpoint location

**Resolution**:
```python
# Nuclear option: Full refresh (deletes checkpoints and reprocesses)
attempt_pipeline_recovery(
    '1187c00e-e560-40b4-89c3-87263de49d9b',
    strategy='full_refresh'
)
```

**Prevention**: Always stop pipelines gracefully (use "Stop" button, not "Terminate Cluster").

---

## Project Structure

```
/Workspace/Users/mohitpdh285@gmail.com/SDP_Pipeline_Project/
│
├── README.md                                    # ← You are here (comprehensive docs)
│
├── 03_Pipeline_Monitoring_AutoRecovery.py       # Monitoring notebook with 5 functions
│   ├── check_pipeline_health()
│   ├── check_data_freshness()
│   ├── attempt_pipeline_recovery()
│   ├── display_pipeline_metrics()
│   └── monitor_pipeline()                       # Continuous 24/7 monitoring
│
├── 04_Dashboard_Queries.sql                     # SQL queries for dashboard widgets
│   ├── Widget 1: Throughput Counter
│   ├── Widget 2: Data Flow Sanity Check
│   ├── Widget 3: Processing Latency
│   ├── Widget 4: Quality Rejection Rate
│   ├── Widget 5: Customer Tier Distribution
│   ├── Widget 6: Quarantine Trending
│   ├── Widget 7: Late Data SLA
│   └── Widget 8: Hourly Performance Table
│
├── transformations/
│   └── pipeline_transformations.sql             # Copy of pipeline transformations (13 tables)
│
└── config/
    └── pipeline_config.json                     # Pipeline configuration metadata
        ├── pipeline_id: 1187c00e-e560-40b4-89c3-87263de49d9b
        ├── catalog: main, schema: demo_sdp
        └── dashboard_id: 01f149fd7da51c65b72a29dc25ab3fe4

/Workspace/Users/mohitpdh285@gmail.com/demo_sdp_pipeline/
└── transformations.sql                          # ← ACTIVE pipeline file (used by SDP)
    └── Defines all 13 tables:
        1. bronze_orders (STREAMING)
        2. quarantine_orders (STREAMING)
        3. silver_orders (STREAMING)
        4. silver_orders_deduped (STREAMING via AUTO CDC FLOW)
        5. cdc_audit_log (STREAMING)
        6. gold_user_dimension (MATERIALIZED VIEW)
        7. gold_hourly_metrics (MATERIALIZED VIEW)
        8. gold_daily_user_sales (MATERIALIZED VIEW)
        9. gold_product_performance (MATERIALIZED VIEW)
        10. cdc_metrics_summary (MATERIALIZED VIEW)
        11. data_quality_metrics (MATERIALIZED VIEW)
        12. quarantine_summary (MATERIALIZED VIEW)
```

### File Relationships

```
transformations.sql (ACTIVE)
        │
        ├─── Read by SDP Pipeline at runtime
        │
        └─── Copied to transformations/pipeline_transformations.sql (backup)

03_Pipeline_Monitoring_AutoRecovery.py
        │
        └─── Calls Databricks Pipeline API to monitor pipeline_id

04_Dashboard_Queries.sql
        │
        └─── Queries executed by Dashboard widgets (ID: 01f149fd7da51c65b72a29dc25ab3fe4)

config/pipeline_config.json
        │
        └─── Stores pipeline_id, catalog/schema, dashboard_id metadata
```

---

## Quick Start Checklist

- [ ] **Step 1**: View pipeline at [demo_ecommerce_orders_pipeline](#pipeline-1187c00e-e560-40b4-89c3-87263de49d9b)
- [ ] **Step 2**: Verify pipeline is RUNNING (should be in CONTINUOUS mode)
- [ ] **Step 3**: Open [Production Observability Dashboard](#dashboard-01f149fd7da51c65b72a29dc25ab3fe4)
- [ ] **Step 4**: Open `03_Pipeline_Monitoring_AutoRecovery.py` and run Cell 17 (quick health test)
- [ ] **Step 5**: Run `display_pipeline_metrics()` to see current data flow
- [ ] **Step 6**: Query `main.demo_sdp.gold_user_dimension` to see customer segmentation
- [ ] **Step 7**: Schedule `monitor_pipeline()` for 24/7 autonomous monitoring

---

## Performance Tuning Tips

### 1. Auto Loader Schema Inference
**Current**: `inferColumnTypes => true` (infers on every file)  
**Optimization**: Use `schemaLocation` to cache inferred schema:
```sql
FROM STREAM read_files(
  '/Volumes/main/default/demo_orders',
  format => 'json',
  schemaLocation => '/tmp/demo_orders_schema'  -- Cache schema here
)
```
**Benefit**: 10-50% faster ingestion for large files.

### 2. Materialized View Refresh Strategy
**Current**: On-query refresh (lazy)  
**Future**: When SDP supports scheduled MV refresh, configure:
```sql
-- Future syntax (not yet available)
CREATE MATERIALIZED VIEW gold_user_dimension
REFRESH EVERY 15 MINUTES
AS SELECT ...
```

### 3. Partition Pruning
**Current**: No partitioning  
**Optimization**: Partition large gold tables by date:
```sql
CREATE MATERIALIZED VIEW gold_daily_user_sales
PARTITIONED BY (order_date)
AS SELECT ...
```
**Benefit**: 100x faster queries for "last 7 days" use cases.

### 4. AUTO CDC State Store Tuning
**Current**: Default RocksDB settings  
**Optimization** (if deduplication slows down with millions of unique keys):
```python
# In pipeline settings → Advanced → Spark Configuration
spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled true
```
**Benefit**: Asynchronous checkpointing reduces latency.

---

## Production Deployment Recommendations

### 1. Environment Separation
```
DEV Pipeline:   pipeline_dev_orders (catalog: dev)
STAGING Pipeline: pipeline_stg_orders (catalog: staging)
PROD Pipeline:  pipeline_prod_orders (catalog: main)
```

### 2. Data Retention Policies
```sql
-- Quarantine table: Keep 90 days for analysis
ALTER TABLE main.demo_sdp.quarantine_orders 
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 90 days');

-- Audit log: Keep 1 year for compliance
ALTER TABLE main.demo_sdp.cdc_audit_log
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 365 days');
```

### 3. Alerting Integration
```python
# In monitor_pipeline(), add PagerDuty/Slack alerts
if not health['is_healthy']:
    requests.post('https://hooks.slack.com/...', json={
        'text': f"Pipeline {pipeline_id} is DOWN"
    })
```

### 4. Disaster Recovery
```python
# Daily backup of critical gold tables
spark.sql("""
  CREATE TABLE IF NOT EXISTS main.demo_sdp_backup.gold_user_dimension_backup
  AS SELECT *, CURRENT_DATE() as backup_date
  FROM main.demo_sdp.gold_user_dimension
""")
```

---

**Pipeline Status**: ✅ Production-Ready Architecture  
**Dashboard Status**: ✅ Active (ID: 01f149fd7da51c65b72a29dc25ab3fe4)  
**Last Updated**: 2025-01-07  
**Pipeline Version**: v2.0 (with AUTO CDC deduplication)

**Questions? Issues?**  
1. Check [Troubleshooting Guide](#troubleshooting-guide)
2. Run `display_pipeline_metrics()` for quick diagnostics
3. Review pipeline event logs in Databricks UI
4. Access [Production Dashboard](#dashboard-01f149fd7da51c65b72a29dc25ab3fe4) for real-time monitoring
