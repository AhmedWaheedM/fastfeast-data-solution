-- ============================================================================
-- VIEW_SLA_METRICS
-- ============================================================================
-- Purpose: Ticket-level SLA metrics computed from ticket + ticket event timelines.
--
-- Metrics:
-- - first_response_time: minutes from ticket creation to first agent response event
-- - resolution_time: minutes from ticket creation to first resolved event
-- - first_response_breached: first_response_time > 1
-- - resolution_breached: resolution_time > 15
-- - is_reopened: any event occurs after first resolved event
--
-- Event Mapping Note:
-- This dataset does not contain event_type='agent_response'. We map first response
-- to first status transition to 'InProgress' or note containing 'Agent responded'.
--
-- Dependencies:
-- SILVER_TICKETS, SILVER_TICKET_EVENTS, FACT_TICKET_LIFECYCLE and Gold dimensions.
-- ============================================================================

CREATE OR REPLACE VIEW VIEW_SLA_METRICS AS
WITH ticket_base AS (
    SELECT
        t.ticket_id,
        t.order_id,
        CAST(t.created_at AS TIMESTAMP) AS ticket_created_ts
    FROM SILVER_TICKETS t
),
event_base AS (
    SELECT
        e.ticket_id,
        CAST(e.event_ts AS TIMESTAMP) AS event_ts,
        LOWER(COALESCE(e.new_status, '')) AS new_status,
        LOWER(COALESCE(e.notes, '')) AS notes
    FROM SILVER_TICKET_EVENTS e
),
first_response AS (
    SELECT
        eb.ticket_id,
        MIN(eb.event_ts) AS first_response_ts
    FROM event_base eb
    WHERE eb.new_status = 'inprogress' OR eb.notes LIKE '%agent responded%'
    GROUP BY eb.ticket_id
),
first_resolved AS (
    SELECT
        eb.ticket_id,
        MIN(eb.event_ts) AS resolved_ts
    FROM event_base eb
    WHERE eb.new_status = 'resolved'
    GROUP BY eb.ticket_id
),
reopened_flag AS (
    SELECT
        fr.ticket_id,
        MAX(CASE WHEN eb.event_ts > fr.resolved_ts THEN 1 ELSE 0 END) = 1 AS is_reopened
    FROM first_resolved fr
    LEFT JOIN event_base eb ON eb.ticket_id = fr.ticket_id
    GROUP BY fr.ticket_id
),
event_metrics AS (
    SELECT
        tb.ticket_id,
        tb.order_id,
        tb.ticket_created_ts,
        frs.first_response_ts,
        rsl.resolved_ts,
        CASE
            WHEN frs.first_response_ts IS NULL THEN NULL
            ELSE GREATEST(DATEDIFF('minute', tb.ticket_created_ts, frs.first_response_ts), 0)
        END AS first_response_time,
        CASE
            WHEN rsl.resolved_ts IS NULL THEN NULL
            ELSE GREATEST(DATEDIFF('minute', tb.ticket_created_ts, rsl.resolved_ts), 0)
        END AS resolution_time,
        CASE
            WHEN frs.first_response_ts IS NULL THEN NULL
            ELSE GREATEST(DATEDIFF('minute', tb.ticket_created_ts, frs.first_response_ts), 0) > 1
        END AS first_response_breached,
        CASE
            WHEN rsl.resolved_ts IS NULL THEN NULL
            ELSE GREATEST(DATEDIFF('minute', tb.ticket_created_ts, rsl.resolved_ts), 0) > 15
        END AS resolution_breached,
        COALESCE(rf.is_reopened, FALSE) AS is_reopened
    FROM ticket_base tb
    LEFT JOIN first_response frs ON frs.ticket_id = tb.ticket_id
    LEFT JOIN first_resolved rsl ON rsl.ticket_id = tb.ticket_id
    LEFT JOIN reopened_flag rf ON rf.ticket_id = tb.ticket_id
)
SELECT
    -- Ticket identifiers and computed SLA metrics
    f.TICKET_SK,
    em.ticket_id,
    em.order_id,
    em.ticket_created_ts,
    em.first_response_ts,
    em.resolved_ts,
    em.first_response_time,
    em.resolution_time,
    em.first_response_breached,
    em.resolution_breached,
    em.is_reopened,
    
    -- Existing role-playing keys and fact attributes
    f.TICKET_CREATED_DATE_SK,
    f.TICKET_CREATED_TIME_SK,
    f.ORDER_DATE_SK,
    f.ORDER_TIME_SK,
    f.TICKET_STATUS,
    f.ORDER_STATUS,
    f.IS_ESCALATED,
    f.REOPEN_COUNT,
    
    -- Dimension context
    COALESCE(dc.CITY_NAME, 'UNKNOWN') AS customer_city,
    COALESCE(dd.CITY_NAME, 'UNKNOWN') AS driver_city,
    COALESCE(dr.RESTAURANT_NAME, 'UNKNOWN') AS restaurant_name,
    COALESCE(dr.CITY_NAME, 'UNKNOWN') AS restaurant_city,
    COALESCE(dch.CHANNEL_NAME, 'UNKNOWN') AS channel_name,
    COALESCE(dp.PRIORITY_NAME, 'STANDARD') AS priority_name,
    COALESCE(dr_reason.REASON_NAME, 'OTHER') AS reason_name,
    
    -- Financial and audit
    f.REFUND_AMOUNT,
    f.COMPENSATION_AMOUNT,
    f.CREDIT_AMOUNT,
    f.TOTAL_REVENUE_IMPACT,
    f.BATCH_ID,
    f.TOTAL_EVENT_COUNT
    
FROM event_metrics em
LEFT JOIN FACT_TICKET_LIFECYCLE f
    ON f.TICKET_ID = em.ticket_id
LEFT JOIN DIM_CUSTOMER dc ON f.CUSTOMER_SK = dc.CUSTOMER_SK
LEFT JOIN DIM_DRIVER dd ON f.DRIVER_SK = dd.DRIVER_SK
LEFT JOIN DIM_RESTAURANT dr ON f.RESTAURANT_SK = dr.RESTAURANT_SK
LEFT JOIN DIM_AGENT da ON f.AGENT_SK = da.AGENT_SK
LEFT JOIN DIM_CHANNEL dch ON f.CHANNEL_SK = dch.CHANNEL_SK
LEFT JOIN DIM_PRIORITY dp ON f.PRIORITY_SK = dp.PRIORITY_SK
LEFT JOIN DIM_REASON dr_reason ON f.REASON_SK = dr_reason.REASON_SK
;


-- ============================================================================
-- VIEW_SLA_DASHBOARD
-- ============================================================================
-- Purpose: High-level SLA KPI aggregations for operational dashboarding,
--          grouped by business dimensions (City, Restaurant, Driver).
--
-- Architecture Design:
-- - Materializes aggregate metrics from VIEW_SLA_METRICS
-- - Groups by geographic (city/region) and operational (restaurant, driver) dimensions
-- - Calculates breach rates, reopen rates, avg resolution time
-- - Includes sample sizes for confidence in trends (record counts)
-- - Handles edge cases: division by zero, NULL/unknown values
-- - Supports time-series analysis (include date SK if needed)
--
-- Key Metrics:
-- 1. SLA_BREACH_RATE_PCT: Percent of tickets breaching first response OR resolution SLA
--    = (COUNT CASE WHEN first_response_breached OR resolution_breached) / COUNT(*) * 100
-- 2. AVG_RESOLUTION_TIME_MIN: Mean time from ticket creation to resolution
-- 3. REOPEN_RATE_PCT: Percent of tickets that were reopened after resolution
-- 4. FIRST_RESPONSE_BREACH_RATE_PCT: Focused metric on agent responsiveness
-- 5. RESOLUTION_BREACH_RATE_PCT: Focused metric on problem-solving speed
--
-- Grouping Logic:
-- - By RESTAURANT_CITY (primary), RESTAURANT_NAME (secondary), DRIVER_CITY (tertiary)
-- - Enables slicing by geography, service point, and delivery area
-- - Supports drill-down: City → Restaurant → Driver performance
--
-- Dependencies: VIEW_SLA_METRICS
--
-- Performance Notes:
-- - Aggregation view over VIEW_SLA_METRICS
-- - Grouping keys: city, restaurant, driver
-- ============================================================================

CREATE OR REPLACE VIEW VIEW_SLA_DASHBOARD AS
WITH sla_base AS (
    SELECT
        restaurant_city,
        restaurant_name,
        driver_city,
        first_response_breached,
        resolution_breached,
        first_response_time,
        resolution_time,
        is_reopened,
        refund_amount,
        total_revenue_impact,
        ticket_id
    FROM VIEW_SLA_METRICS
    WHERE 1=1
      -- Filter to fully loaded records (no unknowns/nulls in critical dims)
      AND restaurant_city != 'UNKNOWN'
      AND driver_city != 'UNKNOWN'
)
SELECT
    -- Grouping Dimensions
    restaurant_city,
    restaurant_name,
    driver_city,
    
    -- Record Count (confidence indicator)
    COUNT(DISTINCT ticket_id) AS total_tickets,
    
    -- SLA KPIs (%)
    ROUND(
        CAST(
            SUM(CASE WHEN first_response_breached OR resolution_breached THEN 1 ELSE 0 END) 
            AS DECIMAL(10,2)
        ) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS sla_breach_rate_pct,
    
    ROUND(
        CAST(
            SUM(CASE WHEN first_response_breached THEN 1 ELSE 0 END)
            AS DECIMAL(10,2)
        ) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS first_response_breach_rate_pct,
    
    ROUND(
        CAST(
            SUM(CASE WHEN resolution_breached THEN 1 ELSE 0 END)
            AS DECIMAL(10,2)
        ) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS resolution_breach_rate_pct,
    
    ROUND(
        CAST(
            SUM(CASE WHEN is_reopened THEN 1 ELSE 0 END)
            AS DECIMAL(10,2)
        ) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS reopen_rate_pct,
    
    -- Time Metrics (minutes)
    ROUND(AVG(first_response_time), 2) AS avg_first_response_time,
    ROUND(AVG(resolution_time), 2) AS avg_resolution_time,
    ROUND(MAX(resolution_time), 2) AS max_resolution_time,
    ROUND(MIN(resolution_time), 2) AS min_resolution_time,
    
    -- Financial Impact
    ROUND(SUM(COALESCE(refund_amount, 0)), 2) AS total_refunds,
    ROUND(SUM(COALESCE(total_revenue_impact, 0)), 2) AS total_revenue_impact,
    ROUND(AVG(COALESCE(refund_amount, 0)), 2) AS avg_refund_per_ticket,
    
    -- Refresh Metadata
    CURRENT_TIMESTAMP AS view_refreshed_at
    
FROM sla_base
GROUP BY restaurant_city, restaurant_name, driver_city
ORDER BY sla_breach_rate_pct DESC, total_tickets DESC
;


-- ============================================================================
-- ============================================================================
