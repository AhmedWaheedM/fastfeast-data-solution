"""
SLA Metrics View Manager

Purpose: Manage creation and refresh of SLA metrics views (VIEW_SLA_METRICS, VIEW_SLA_DASHBOARD)
         as part of Gold layer orchestration.

Usage:
    from FastFeast.pipeline.gold.sla_metrics_views import SLAMetricsViewManager    
    manager = SLAMetricsViewManager(logger=logger, config=config)
    manager.create_or_refresh_views()

Architecture:
- Standalone module, no dependencies on loader.py or parallel_process.py
- Idempotent: safe to call multiple times
- Handles view creation, materialization, and refresh
- Includes helper methods for query validation
"""

import logging
from pathlib import Path
from typing import Optional

import pyarrow as pa

from FastFeast.utilities.db_utils import get_connection

VIEWS_SQL_PATH = Path(__file__).resolve().parents[2] / "dwh" / "gold" / "view_sla_metrics.sql"


class SLAMetricsViewManager:
    """Manages SLA metrics views in Gold layer."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize view manager.
        
        Args:
            logger: Optional logger instance (falls back to stdlib logging)
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def create_or_refresh_views(self) -> bool:
        """
        Create or replace SLA metrics views.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info("Starting SLA metrics view creation/refresh...")
            
            # Read DDL from file
            if not VIEWS_SQL_PATH.exists():
                self.logger.error(f"View DDL file not found: {VIEWS_SQL_PATH}")
                return False
            
            with open(VIEWS_SQL_PATH, 'r') as f:
                view_ddl = f.read()
            
            # Execute DDL
            conn = get_connection()
            try:
                conn.execute(view_ddl)
            finally:
                conn.close()
            
            self.logger.info("SLA metrics views created/refreshed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create SLA metrics views: {e}")
            return False
    
    def validate_views_exist(self) -> bool:
        """
        Validate that both views exist and are queryable.
        
        Returns:
            bool: True if both views exist and contain data
        """
        try:
            conn = get_connection()
            try:
                # Check VIEW_SLA_METRICS
                result_metrics = conn.execute(
                    "SELECT COUNT(*) as cnt FROM VIEW_SLA_METRICS"
                ).fetchall()
                metrics_count = result_metrics[0][0] if result_metrics else 0

                # Check VIEW_SLA_DASHBOARD
                result_dashboard = conn.execute(
                    "SELECT COUNT(*) as cnt FROM VIEW_SLA_DASHBOARD"
                ).fetchall()
                dashboard_count = result_dashboard[0][0] if result_dashboard else 0
            finally:
                conn.close()
            
            if metrics_count == 0:
                self.logger.warning("VIEW_SLA_METRICS is empty (fact table may be unpopulated)")
            if dashboard_count == 0:
                self.logger.warning("VIEW_SLA_DASHBOARD is empty")
            
            return metrics_count > 0 and dashboard_count > 0
            
        except Exception as e:
            self.logger.error(f"View validation failed: {e}")
            return False
    
    @staticmethod
    def _query_to_arrow(query: str) -> pa.Table:
        conn = get_connection()
        try:
            return conn.execute(query).fetch_arrow_table()
        finally:
            conn.close()

    def get_sla_dashboard_summary(self) -> Optional[pa.Table]:
        """
        Query SLA dashboard for high-level summary.
        
        Returns:
            pyarrow.Table or None: Aggregated SLA metrics, or None if query fails
        """
        try:
            query = """
            SELECT 
                restaurant_city,
                restaurant_name,
                driver_city,
                total_tickets,
                sla_breach_rate_pct,
                first_response_breach_rate_pct,
                resolution_breach_rate_pct,
                reopen_rate_pct,
                avg_resolution_time,
                total_refunds,
                total_revenue_impact
            FROM VIEW_SLA_DASHBOARD
            ORDER BY sla_breach_rate_pct DESC, total_tickets DESC
            """
            
            table = self._query_to_arrow(query)
            self.logger.info(f"SLA Dashboard loaded: {table.num_rows} rows")
            return table
            
        except Exception as e:
            self.logger.error(f"Failed to query SLA dashboard: {e}")
            return None
    
    def get_sla_breach_alerts(self, breach_threshold_pct: float = 10.0) -> Optional[pa.Table]:
        """
        Identify SLA breach hotspots for alerting.
        
        Args:
            breach_threshold_pct: Alert threshold (default 10%)
        
        Returns:
            pyarrow.Table or None: Breaching city/restaurant/driver combos
        """
        try:
            threshold = float(breach_threshold_pct)
            query = f"""
            SELECT 
                restaurant_city,
                restaurant_name,
                driver_city,
                total_tickets,
                sla_breach_rate_pct,
                resolution_breach_rate_pct,
                reopen_rate_pct,
                CASE 
                    WHEN sla_breach_rate_pct >= 20 THEN 'CRITICAL'
                    WHEN sla_breach_rate_pct >= {threshold} THEN 'WARNING'
                    ELSE 'OK'
                END as alert_level
            FROM VIEW_SLA_DASHBOARD
            WHERE sla_breach_rate_pct >= {threshold}
            ORDER BY sla_breach_rate_pct DESC
            """

            table = self._query_to_arrow(query)
            if table.num_rows == 0:
                self.logger.info(f"No SLA breaches above threshold {threshold}%")
            else:
                self.logger.warning(
                    f"SLA breach alerts found: {table.num_rows} rows (threshold={threshold}%)"
                )
            return table
            
        except Exception as e:
            self.logger.error(f"Failed to query SLA breach alerts: {e}")
            return None
    
    def get_high_reopen_tickets(self, reopen_threshold_pct: float = 5.0) -> Optional[pa.Table]:
        """
        Identify areas with high reopen rates (quality issue indicator).
        
        Args:
            reopen_threshold_pct: Reopen rate threshold (default 5%)
        
        Returns:
            pyarrow.Table or None: High-reopen city/restaurant/driver combos
        """
        try:
            threshold = float(reopen_threshold_pct)
            query = f"""
            SELECT 
                restaurant_city,
                restaurant_name,
                driver_city,
                total_tickets,
                reopen_rate_pct,
                sla_breach_rate_pct,
                avg_resolution_time
            FROM VIEW_SLA_DASHBOARD
            WHERE reopen_rate_pct >= {threshold}
            ORDER BY reopen_rate_pct DESC
            """

            table = self._query_to_arrow(query)
            if table.num_rows == 0:
                self.logger.info(f"No high-reopen areas above threshold {threshold}%")
            else:
                self.logger.warning(
                    f"High-reopen areas found: {table.num_rows} rows (threshold={threshold}%)"
                )
            return table
            
        except Exception as e:
            self.logger.error(f"Failed to query high-reopen tickets: {e}")
            return None
    
    def materialize_dashboard_daily(self) -> bool:
        """
        Create materialized view for BI dashboard (daily refresh).
        
        Useful for offline BI tools or high-query-volume scenarios.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            conn = get_connection()
            try:
                # Drop existing materialized view
                conn.execute("DROP TABLE IF EXISTS MV_SLA_DASHBOARD_DAILY")

                # Create materialized view
                conn.execute("""
                CREATE TABLE MV_SLA_DASHBOARD_DAILY AS
                SELECT *, CURRENT_TIMESTAMP as materialized_at
                FROM VIEW_SLA_DASHBOARD
                """)
            finally:
                conn.close()
            
            self.logger.info("Materialized view MV_SLA_DASHBOARD_DAILY created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create materialized view: {e}")
            return False


# Integration hook for loader.py
def integrate_with_loader(loader_instance) -> None:
    """
    Decorator helper to add SLA view refresh to DataLoader.load_to_gold().
    
    Usage in loader.py:
        from FastFeast.pipeline.gold.sla_metrics_views import integrate_with_loader        
        integrate_with_loader(loader_instance)
    """
    original_load_to_gold = loader_instance.load_to_gold
    
    def load_to_gold_with_views(file_name: str):
        # Run original load
        original_load_to_gold(file_name)
        
        # Refresh SLA views
        manager = SLAMetricsViewManager(logger=loader_instance.logger)
        if manager.create_or_refresh_views():
            manager.validate_views_exist()
    
    loader_instance.load_to_gold = load_to_gold_with_views


# Integration hook for orchestration
def integrate_with_orchestration(orchestrator_logger) -> None:
    """
    Standalone call for orchestration layer.
    
    Usage in parallel_process.py:
        from FastFeast.pipeline.gold.sla_metrics_views import integrate_with_orchestration        
        # After ThreadPoolExecutor batch complete:
        integrate_with_orchestration(logger)
    """
    manager = SLAMetricsViewManager(logger=orchestrator_logger)
    manager.create_or_refresh_views()


if __name__ == "__main__":
    # Quick test
    import logging
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    manager = SLAMetricsViewManager(logger=logger)
    
    print("\n=== Creating/Refreshing Views ===")
    manager.create_or_refresh_views()
    
    print("\n=== Validating Views ===")
    if manager.validate_views_exist():
        print("✓ Views validated successfully")
    
    print("\n=== SLA Dashboard Summary ===")
    summary = manager.get_sla_dashboard_summary()
    if summary is not None:
        print(f"rows={summary.num_rows}, cols={summary.num_columns}")
        print(summary.slice(0, min(10, summary.num_rows)).to_pydict())
    
    print("\n=== SLA Breach Alerts (>10%) ===")
    breaches = manager.get_sla_breach_alerts(breach_threshold_pct=10.0)
    if breaches is not None and breaches.num_rows > 0:
        print(breaches.to_pydict())
    
    print("\n=== High Reopen Areas (>5%) ===")
    reopens = manager.get_high_reopen_tickets(reopen_threshold_pct=5.0)
    if reopens is not None and reopens.num_rows > 0:
        print(reopens.to_pydict())
