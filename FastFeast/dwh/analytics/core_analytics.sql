
-- 1. Total Tickets
SELECT 
    COUNT(ticket_id) AS total_tickets 
FROM fact_tickets_lifecycle;


-- 2. SLA Breach Rate %
-- (Breached if first response > 1 min OR resolution > 15 mins)
SELECT 
    ROUND((COUNT(CASE WHEN first_response_time_mins > 1 OR resolution_time_mins > 15 THEN 1 END) * 100.0) / COUNT(*), 2) 
    AS sla_breach_rate_pct
FROM fact_tickets_lifecycle;


-- 3. Average Resolution Time
SELECT 
    ROUND(AVG(resolution_time_mins), 2) AS avg_resolution_time_mins 
FROM fact_tickets_lifecycle;


-- 4. First Response Time (Average)
SELECT 
    ROUND(AVG(first_response_time_mins), 2) AS avg_first_response_time_mins 
FROM fact_tickets;


-- 5. Reopen Rate %
SELECT 
    ROUND((COUNT(CASE WHEN is_reopened = TRUE THEN 1 END) * 100.0) / COUNT(*), 2) AS reopen_rate_pct 
FROM fact_tickets;


-- 6. Refund Amount & 7. Revenue Impact 
-- (Revenue loss due to complaints)
SELECT 
    SUM(refund_amount) AS total_refund_amount,
    SUM(revenue_impact_amount) AS total_revenue_impact
FROM fact_tickets;


-- 8. Tickets by City/Region
SELECT 
    r.region_name,
    c.city_name,
    COUNT(t.ticket_id) AS total_tickets
FROM fact_tickets t
JOIN dim_cities c ON t.city_id = c.city_id
JOIN dim_regions r ON c.region_id = r.region_id
GROUP BY 1, 2
ORDER BY total_tickets DESC;


-- 9. Tickets by Restaurant
SELECT 
    rest.restaurant_name,
    COUNT(t.ticket_id) AS total_tickets
FROM fact_tickets t
JOIN dim_restaurants rest ON t.restaurant_id = rest.restaurant_id
GROUP BY 1
ORDER BY total_tickets DESC;


-- 10. Tickets by Driver
SELECT 
    d.driver_id,
    COUNT(t.ticket_id) AS total_tickets
FROM fact_tickets t
JOIN dim_drivers d ON t.driver_id = d.driver_id
GROUP BY 1
ORDER BY total_tickets DESC;
