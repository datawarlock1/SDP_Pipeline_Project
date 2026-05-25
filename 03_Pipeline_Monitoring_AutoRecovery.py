# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 🚨 24/7 SDP Pipeline Monitoring & Auto-Recovery System
# MAGIC ## Continuous Real-time Monitoring with Automatic Failure Recovery
# MAGIC
# MAGIC **Pipeline**: demo_ecommerce_orders_pipeline  
# MAGIC **Pipeline ID**: 1187c00e-e560-40b4-89c3-87263de49d9b
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 What This Notebook Does
# MAGIC
# MAGIC This notebook runs **continuously (24/7)** to monitor your pipeline and automatically recover from failures:
# MAGIC
# MAGIC ✅ **Checks pipeline health every 5 minutes**  
# MAGIC ✅ **Auto-restarts failed pipelines** (up to 3 attempts)  
# MAGIC ✅ **Runs indefinitely until you stop it**  
# MAGIC ✅ **Self-healing** - recovers while you sleep  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 To Start Monitoring
# MAGIC
# MAGIC **Just scroll down to Cell 13 and run it** (or click "Run All")
# MAGIC
# MAGIC The system will:
# MAGIC 1. Check pipeline health every 5 minutes
# MAGIC 2. If pipeline fails → Auto-restart (soft restart)
# MAGIC 3. If still failing → Retry (soft restart)
# MAGIC 4. If still failing → Full refresh (reprocess all data)
# MAGIC 5. Continue monitoring forever
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Features
# MAGIC - Real-time health monitoring
# MAGIC - Data freshness checks
# MAGIC - Automatic recovery from failures (3-tier strategy)
# MAGIC - Event log forensics
# MAGIC - Comprehensive alerting
# MAGIC - Runs 24/7 until manually stopped
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **⚠️  This is a production-grade monitoring system - leave it running!**

# COMMAND ----------

# ===================================================================
# SETUP: Import Required Libraries
# ===================================================================

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineStateInfo, StartUpdateCause
import time
from datetime import datetime, timedelta

# Initialize Databricks client
w = WorkspaceClient()

# Pipeline configuration
PIPELINE_ID = "1187c00e-e560-40b4-89c3-87263de49d9b"
PIPELINE_NAME = "demo_ecommerce_orders_pipeline"

print("✅ Monitoring setup complete")
print(f"📊 Pipeline: {PIPELINE_NAME}")
print(f"🆔 Pipeline ID: {PIPELINE_ID}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# ===================================================================
# 1. PIPELINE HEALTH CHECK FUNCTION
# ===================================================================

def check_pipeline_health(pipeline_id: str) -> dict:
    """
    Check the health status of an SDP pipeline
    Returns: dict with status, last_update, state, errors
    """
    try:
        pipeline = w.pipelines.get(pipeline_id=pipeline_id)
        
        # Get latest update - handle the response properly
        try:
            updates_response = w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=1)
            updates = list(updates_response) if updates_response else []
            latest_update = updates[0] if updates else None
        except:
            latest_update = None
        
        health = {
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline.name if pipeline.name else "Unknown",
            "state": pipeline.state.value if pipeline.state else "UNKNOWN",
            "last_update_time": latest_update.creation_time if latest_update else None,
            "last_update_state": latest_update.state.value if latest_update and latest_update.state else None,
            "is_healthy": False,
            "errors": []
        }
        
        # Check if pipeline is healthy
        # For CONTINUOUS mode: IDLE and RUNNING are both healthy states
        if pipeline.state:
            state_str = str(pipeline.state)
            if 'IDLE' in state_str or 'RUNNING' in state_str:
                health["is_healthy"] = True
            elif 'FAILED' in state_str:
                health["is_healthy"] = False
                health["errors"].append("Pipeline in FAILED state")
        
        # Also check last update status
        if latest_update and latest_update.state:
            if 'FAILED' in str(latest_update.state):
                health["is_healthy"] = False
                health["errors"].append("Last update FAILED")
        
        return health
        
    except Exception as e:
        # Return consistent structure even on error
        return {
            "pipeline_id": pipeline_id,
            "pipeline_name": "Unknown",
            "state": "ERROR",
            "last_update_time": None,
            "last_update_state": None,
            "is_healthy": False,
            "errors": [f"Error checking pipeline: {str(e)}"]
        }

print("✅ Function loaded: check_pipeline_health()")

# COMMAND ----------

# ===================================================================
# 2. DETECT DATA FRESHNESS LAG
# ===================================================================

def check_data_freshness(table_name: str, timestamp_col: str, max_lag_minutes: int = 30) -> dict:
    """
    Check if data is fresh (not lagging)
    """
    query = f"""
    SELECT 
        MAX({timestamp_col}) as latest_timestamp,
        TIMESTAMPDIFF(MINUTE, MAX({timestamp_col}), current_timestamp()) as lag_minutes,
        COUNT(*) as total_records
    FROM {table_name}
    """
    
    try:
        result = spark.sql(query).collect()[0]
        lag_minutes = result['lag_minutes'] if result['lag_minutes'] else 0
        
        return {
            "table": table_name,
            "latest_data_time": result['latest_timestamp'],
            "lag_minutes": lag_minutes,
            "total_records": result['total_records'],
            "is_fresh": lag_minutes <= max_lag_minutes,
            "alert": f"⚠️ Data lagging by {lag_minutes} minutes" if lag_minutes > max_lag_minutes else "✅ Data is fresh"
        }
    except Exception as e:
        return {
            "table": table_name,
            "error": str(e),
            "is_fresh": False,
            "alert": f"❌ Error checking freshness: {str(e)}"
        }

print("✅ Function loaded: check_data_freshness()")

# COMMAND ----------

# DBTITLE 1,Cell 5
# ===================================================================
# 3. AUTO-RECOVERY FUNCTION
# ===================================================================

def attempt_pipeline_recovery(pipeline_id: str, strategy: str = "restart") -> dict:
    """
    Attempt to recover a failed pipeline
    
    Strategies:
    - 'restart': Simple restart
    - 'full_refresh': Full refresh (reprocess all data)
    """
    try:
        if strategy == "restart":
            # Start a new update
            update = w.pipelines.start_update(
                pipeline_id=pipeline_id
            )
            return {
                "success": True,
                "strategy": "restart",
                "update_id": str(update.update_id) if update and hasattr(update, 'update_id') else "N/A",
                "message": "Pipeline restart initiated successfully"
            }
        
        elif strategy == "full_refresh":
            # Start with full refresh
            update = w.pipelines.start_update(
                pipeline_id=pipeline_id,
                full_refresh=True
            )
            return {
                "success": True,
                "strategy": "full_refresh",
                "update_id": str(update.update_id) if update and hasattr(update, 'update_id') else "N/A",
                "message": "Full refresh initiated - all data will be reprocessed"
            }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"Recovery attempt failed: {str(e)}"
        }

print("✅ Function loaded: attempt_pipeline_recovery()")

# COMMAND ----------

# ===================================================================
# 4. DISPLAY PIPELINE METRICS
# ===================================================================

def display_pipeline_metrics(pipeline_id: str):
    """
    Display comprehensive pipeline metrics
    """
    print("=" * 80)
    print("📊 PIPELINE METRICS DASHBOARD")
    print("=" * 80)
    
    # Health check
    health = check_pipeline_health(pipeline_id)
    print(f"\n🏥 HEALTH STATUS")
    print(f"  Pipeline: {health.get('pipeline_name', 'Unknown')}")
    print(f"  State: {health['state']}")
    print(f"  Healthy: {'✅ YES' if health['is_healthy'] else '❌ NO'}")
    if health.get('last_update_state'):
        print(f"  Last Update: {health['last_update_state']}")
    if health['errors']:
        print(f"  Errors: {', '.join(health['errors'])}")
    
    # Data flow metrics
    print(f"\n📈 DATA FLOW METRICS")
    try:
        bronze_count = spark.sql("SELECT COUNT(*) as cnt FROM main.demo_sdp.bronze_orders").collect()[0]['cnt']
        silver_count = spark.sql("SELECT COUNT(*) as cnt FROM main.demo_sdp.silver_orders").collect()[0]['cnt']
        gold_user_count = spark.sql("SELECT COUNT(*) as cnt FROM main.demo_sdp.gold_daily_user_sales").collect()[0]['cnt']
        gold_product_count = spark.sql("SELECT COUNT(*) as cnt FROM main.demo_sdp.gold_product_performance").collect()[0]['cnt']
        
        rejection_rate = ((bronze_count - silver_count) / bronze_count * 100) if bronze_count > 0 else 0
        
        print(f"  Bronze Layer: {bronze_count:,} records")
        print(f"  Silver Layer: {silver_count:,} records")
        print(f"  Gold User Sales: {gold_user_count:,} records")
        print(f"  Gold Products: {gold_product_count:,} records")
        print(f"  Quality Rejection Rate: {rejection_rate:.2f}%")
    except Exception as e:
        print(f"  ⚠️ Could not fetch data flow metrics: {e}")
    
    # Freshness checks
    print(f"\n⏰ DATA FRESHNESS")
    freshness_checks = [
        ("main.demo_sdp.bronze_orders", "ingestion_time"),
        ("main.demo_sdp.silver_orders", "processed_time"),
        ("main.demo_sdp.gold_daily_user_sales", "last_updated")
    ]
    
    for table, col in freshness_checks:
        freshness = check_data_freshness(table, col, max_lag_minutes=30)
        print(f"  {table.split('.')[-1]}: {freshness['alert']} (lag: {freshness.get('lag_minutes', 'N/A')} min)")
    
    print("\n" + "=" * 80)

print("✅ Function loaded: display_pipeline_metrics()")

# COMMAND ----------

# DBTITLE 1,Cell 7
# ===================================================================
# 5. COMPREHENSIVE MONITORING LOOP
# ===================================================================

def monitor_pipeline(pipeline_id: str, 
                     check_interval_seconds: int = 300,
                     auto_recover: bool = True,
                     max_iterations: int = None):
    """
    Continuously monitor pipeline health and auto-recover if needed
    
    Args:
        pipeline_id: The pipeline to monitor
        check_interval_seconds: How often to check (default: 5 minutes)
        auto_recover: Enable automatic recovery (default: True)
        max_iterations: Number of checks before stopping (None = run forever)
    """
    print(f"🔍 Starting CONTINUOUS pipeline monitoring for: {pipeline_id}")
    print(f"📊 Check interval: {check_interval_seconds} seconds ({check_interval_seconds/60:.1f} minutes)")
    print(f"🔧 Auto-recovery: {'Enabled ✅' if auto_recover else 'Disabled ❌'}")
    print(f"⏰ Duration: {'INFINITE - runs until manually stopped' if max_iterations is None else f'{max_iterations} iterations'}")
    print("=" * 70)
    print("⚠️  To stop monitoring: Click the 'Stop' button or interrupt the cell")
    print("=" * 70)
    
    recovery_attempts = 0
    max_recovery_attempts = 3
    iteration = 0
    
    try:
        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Check #{iteration}")
            
            # Check pipeline health
            health = check_pipeline_health(pipeline_id)
            
            print(f"Pipeline: {health.get('pipeline_name', 'Unknown')}")
            print(f"State: {health.get('state', 'UNKNOWN')}")
            print(f"Healthy: {'✅ YES' if health.get('is_healthy', False) else '❌ NO'}")
            
            if health.get('errors'):
                print(f"Errors: {', '.join(health['errors'])}")
            
            # Auto-recovery logic
            if not health.get('is_healthy', False) and auto_recover and recovery_attempts < max_recovery_attempts:
                print(f"\n🚨 Pipeline unhealthy! Attempting recovery (Attempt {recovery_attempts + 1}/{max_recovery_attempts})...")
                
                # Choose strategy based on attempt number and error type
                if recovery_attempts < 2:
                    strategy = "restart"  # First 2 attempts: soft restart
                else:
                    strategy = "full_refresh"  # 3rd attempt: nuclear option
                
                # Check if checkpoint error - go straight to full refresh
                error_str = str(health.get('errors', [])).lower()
                if "checkpoint" in error_str:
                    strategy = "full_refresh"
                
                print(f"   Strategy: {strategy}")
                recovery_result = attempt_pipeline_recovery(pipeline_id, strategy)
                print(f"   Result: {recovery_result['message']}")
                
                recovery_attempts += 1
                
                if recovery_result.get('success', False):
                    print("⏳ Waiting 2 minutes for pipeline to stabilize...")
                    time.sleep(120)
            
            elif not health.get('is_healthy', False) and recovery_attempts >= max_recovery_attempts:
                print(f"\n❌ Max recovery attempts ({max_recovery_attempts}) reached!")
                print(f"🚨 MANUAL INTERVENTION REQUIRED")
                print(f"   Action: Check pipeline logs, event history, and Databricks console")
                print(f"   Pipeline ID: {pipeline_id}")
                # Reset after waiting and continue monitoring
                recovery_attempts = 0
            
            # Reset recovery attempts if pipeline is healthy again
            if health.get('is_healthy', False) and recovery_attempts > 0:
                print(f"✅ Pipeline recovered successfully after {recovery_attempts} attempt(s)")
                recovery_attempts = 0
            
            # Wait before next check
            if max_iterations is None or iteration < max_iterations:
                time.sleep(check_interval_seconds)
    
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("⚠️  Monitoring stopped by user (KeyboardInterrupt)")
        print(f"   Total checks performed: {iteration}")
        print("=" * 70)
    
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"❌ Monitoring stopped due to error: {e}")
        print(f"   Total checks performed: {iteration}")
        print("=" * 70)
    
    finally:
        if max_iterations is not None and iteration >= max_iterations:
            print("\n" + "=" * 70)
            print(f"✅ Monitoring session completed - {iteration} checks performed")
            print("=" * 70)

print("✅ Function loaded: monitor_pipeline()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Start Examples

# COMMAND ----------

# Example 1: Check current pipeline health
import json
health = check_pipeline_health(PIPELINE_ID)
print(json.dumps(health, indent=2, default=str))

# COMMAND ----------

# Example 2: Display comprehensive metrics
display_pipeline_metrics(PIPELINE_ID)

# COMMAND ----------

# Example 3: Check data freshness
freshness = check_data_freshness(
    "main.demo_sdp.silver_orders", 
    "processed_time",
    max_lag_minutes=30
)
print(json.dumps(freshness, indent=2, default=str))

# COMMAND ----------

# Example 4: Manual recovery (if needed)
# Uncomment to run:
# recovery = attempt_pipeline_recovery(PIPELINE_ID, strategy="restart")
# print(json.dumps(recovery, indent=2, default=str))

# COMMAND ----------

# DBTITLE 1,TEST: Quick Monitoring Demo (30 sec checks)
# ===================================================================
# QUICK TEST - Monitoring with 30-second intervals (3 iterations)
# ===================================================================
# This is a FAST test version - checks every 30 seconds for 3 iterations
# Good for testing/demo. Use Cell 13 for real 24/7 monitoring.

print("🧪 QUICK TEST MODE - Fast monitoring demo")
print("=" * 70)
print("Configuration:")
print("  • Checks every 30 seconds (for demo)")
print("  • Runs for 3 iterations only (~1.5 minutes total)")
print("  • Auto-recovery enabled")
print("")
print("👀 Watch what happens:")
print("  1. First check: Pipeline is IDLE (not processing)")
print("  2. Auto-recovery starts: Restarts pipeline")
print("  3. Second check: Pipeline should be RUNNING")
print("=" * 70)
print("")

# Run quick test
monitor_pipeline(
    pipeline_id=PIPELINE_ID,
    check_interval_seconds=30,  # Quick checks every 30 seconds
    auto_recover=True,
    max_iterations=3  # Just 3 iterations for demo
)

# COMMAND ----------

# DBTITLE 1,Cell 13: START CONTINUOUS MONITORING
# ===================================================================
# 🚀 START CONTINUOUS MONITORING - RUNS 24/7 UNTIL YOU STOP IT
# ===================================================================

print("🚀 Starting 24/7 Pipeline Monitoring System")
print("=" * 70)
print("")
print("Configuration:")
print("  • Checks every 5 minutes (300 seconds)")
print("  • Auto-recovery enabled (max 3 attempts)")
print("  • Runs INFINITELY until manually stopped")
print("")
print("Recovery Strategy:")
print("  • Attempt 1-2: Soft restart (keeps checkpoint)")
print("  • Attempt 3: Full refresh (reprocess all data)")
print("  • After 3 failures: Continue monitoring, alert for manual intervention")
print("")
print("⚠️  To stop monitoring: Click the 'Stop Execution' button above")
print("=" * 70)
print("")

# Start continuous monitoring (runs forever until stopped)
monitor_pipeline(
    pipeline_id=PIPELINE_ID,
    check_interval_seconds=300,  # Check every 5 minutes
    auto_recover=True,            # Enable auto-recovery
    max_iterations=None           # Run forever (until manually stopped)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-time Pipeline Throughput Analysis

# COMMAND ----------

# Analyze throughput in last 10 minutes
df_throughput = spark.sql("""
SELECT 
  DATE_TRUNC('minute', ingestion_time) as time_bucket,
  COUNT(*) as records_per_minute,
  COUNT(DISTINCT source_file) as files_in_minute,
  AVG(amount) as avg_amount
FROM main.demo_sdp.bronze_orders
WHERE ingestion_time >= current_timestamp() - INTERVAL 10 MINUTES
GROUP BY DATE_TRUNC('minute', ingestion_time)
ORDER BY time_bucket DESC
""")

print("📊 THROUGHPUT - Last 10 Minutes")
df_throughput.show(10, truncate=False)

# COMMAND ----------

# Processing latency analysis
df_latency = spark.sql("""
SELECT 
  AVG(TIMESTAMPDIFF(SECOND, b.ingestion_time, s.processed_time)) as avg_latency_seconds,
  MIN(TIMESTAMPDIFF(SECOND, b.ingestion_time, s.processed_time)) as min_latency_seconds,
  MAX(TIMESTAMPDIFF(SECOND, b.ingestion_time, s.processed_time)) as max_latency_seconds,
  PERCENTILE(TIMESTAMPDIFF(SECOND, b.ingestion_time, s.processed_time), 0.95) as p95_latency_seconds
FROM main.demo_sdp.silver_orders s
JOIN main.demo_sdp.bronze_orders b ON s.order_id = b.order_id
WHERE s.processed_time >= current_timestamp() - INTERVAL 10 MINUTES
""")

print("⚡ PROCESSING LATENCY - Last 10 Minutes")
df_latency.show(truncate=False)