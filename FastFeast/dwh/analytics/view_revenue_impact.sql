-- ============================================================================
-- VIEW_REVENUE_IMPACT
-- ============================================================================
-- Purpose:
--   Summarize financial impact of support tickets by business dimensions while
--   preventing fan-out amplification from downstream joins.
--
-- Anti-fanout strategy:
--   1) Start from FACT_TICKET_LIFECYCLE.
--   2) Collapse to one row per ticket_id in ticket_level CTE.
--   3) Join dimensions only after ticket grain is guaranteed.
-- ============================================================================

CREATE OR REPLACE VIEW VIEW_REVENUE_IMPACT AS
WITH fact_base AS (
    SELECT
        f.ticket_id,
        COALESCE(f.total_revenue_impact, 0) AS total_revenue_impact,
        COALESCE(f.refund_amount, 0) AS refund_amount,
        COALESCE(f.compensation_amount, 0) AS compensation_amount,
        COALESCE(f.credit_amount, 0) AS credit_amount,
        f.customer_sk,
        f.driver_sk,
        f.restaurant_sk,
        f.channel_sk,
        f.priority_sk,
        f.reason_sk
    FROM FACT_TICKET_LIFECYCLE f
    WHERE f.ticket_id IS NOT NULL
),
ticket_level AS (
    SELECT
        ticket_id,
        MAX(total_revenue_impact) AS total_revenue_impact,
        MAX(refund_amount) AS refund_amount,
        MAX(compensation_amount) AS compensation_amount,
        MAX(credit_amount) AS credit_amount,
        MAX(customer_sk) AS customer_sk,
        MAX(driver_sk) AS driver_sk,
        MAX(restaurant_sk) AS restaurant_sk,
        MAX(channel_sk) AS channel_sk,
        MAX(priority_sk) AS priority_sk,
        MAX(reason_sk) AS reason_sk
    FROM fact_base
    GROUP BY ticket_id
),
enriched AS (
    SELECT
        tl.ticket_id,
        tl.total_revenue_impact,
        tl.refund_amount,
        tl.compensation_amount,
        tl.credit_amount,
        COALESCE(dc.city_name, 'UNKNOWN') AS customer_city,
        COALESCE(dd.city_name, 'UNKNOWN') AS driver_city,
        COALESCE(dr.city_name, 'UNKNOWN') AS restaurant_city,
        COALESCE(dr.restaurant_name, 'UNKNOWN') AS restaurant_name,
        COALESCE(ch.channel_name, 'UNKNOWN') AS channel_name,
        COALESCE(pr.priority_name, 'UNKNOWN') AS priority_name,
        COALESCE(rs.reason_name, 'UNKNOWN') AS reason_name
    FROM ticket_level tl
    LEFT JOIN DIM_CUSTOMER dc ON tl.customer_sk = dc.customer_sk
    LEFT JOIN DIM_DRIVER dd ON tl.driver_sk = dd.driver_sk
    LEFT JOIN DIM_RESTAURANT dr ON tl.restaurant_sk = dr.restaurant_sk
    LEFT JOIN DIM_CHANNEL ch ON tl.channel_sk = ch.channel_sk
    LEFT JOIN DIM_PRIORITY pr ON tl.priority_sk = pr.priority_sk
    LEFT JOIN DIM_REASON rs ON tl.reason_sk = rs.reason_sk
)
SELECT
    restaurant_city,
    restaurant_name,
    channel_name,
    priority_name,
    reason_name,
    COUNT(*) AS impacted_tickets,
    ROUND(SUM(total_revenue_impact), 2) AS total_revenue_impact,
    ROUND(SUM(refund_amount), 2) AS total_refund_amount,
    ROUND(SUM(compensation_amount), 2) AS total_compensation_amount,
    ROUND(SUM(credit_amount), 2) AS total_credit_amount,
    ROUND(AVG(total_revenue_impact), 2) AS avg_revenue_impact_per_ticket,
    ROUND(
        SUM(CASE WHEN total_revenue_impact > 0 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0),
        2
    ) AS impacted_ticket_rate_pct
FROM enriched
GROUP BY
    restaurant_city,
    restaurant_name,
    channel_name,
    priority_name,
    reason_name
ORDER BY total_revenue_impact DESC, impacted_tickets DESC;
