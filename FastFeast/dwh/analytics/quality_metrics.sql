-- 1. Total records processed
SELECT 
    SUM(total_rows) AS total_records_processed 
FROM metrics_summary;


-- 2. Null Issue Percentage
SELECT 
    ROUND((SUM(null_issues) * 100.0) / NULLIF(SUM(total_rows), 0), 2) AS null_issue_rate_pct
FROM per_file_issues;


-- 3. Validation Issue Rates (Proxies for Duplicate/Orphan/Integrity rates)
SELECT 
    ROUND((SUM(enum_issues) * 100.0) / NULLIF(SUM(total_rows), 0), 2) AS enum_issue_rate_pct, 
    ROUND((SUM(format_issues) * 100.0) / NULLIF(SUM(total_rows), 0), 2) AS format_issue_rate_pct,
    ROUND((SUM(type_issues) * 100.0) / NULLIF(SUM(total_rows), 0), 2) AS type_issue_rate_pct,
    ROUND((SUM(range_issues) * 100.0) / NULLIF(SUM(total_rows), 0), 2) AS range_issue_rate_pct
FROM per_file_issues;


-- 4. Reject/quarantined record count
SELECT 
    SUM(issue_count) AS total_quarantined_records 
FROM per_file_issues;


-- 5. File processing success rate
SELECT 
    ROUND((COUNT(CASE WHEN is_clean = true THEN 1 END) * 100.0) / COUNT(*), 2) AS file_processing_success_rate_pct
FROM per_file_issues;


-- 6. Average processing latency
SELECT 
    ROUND(AVG(elapsed_ms), 2) AS avg_processing_latency_ms
FROM per_file_issues;


-- 7. Missing Columns Count
SELECT 
    SUM(missing_cols) AS total_missing_column_incidents
FROM per_file_issues;