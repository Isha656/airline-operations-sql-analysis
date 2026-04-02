-- ============================================================
-- AIRLINE DATA ANALYSIS — SQL Portfolio Project
-- Author  : [Your Name]
-- Dataset : US Domestic Flights (flights, airline, airport tables)
-- Goal    : Uncover operational insights — delays, routes,
--           cancellations, and airline performance rankings
-- Tools   : SQL (MySQL / PostgreSQL compatible)
-- ============================================================


-- ============================================================
-- SECTION 1 — BASIC EXPLORATION
-- Understanding the dataset before deeper analysis
-- ============================================================

-- Query 1: Total flights operated per airline
-- Business question: Which airlines have the largest operations?
-- Technique: JOIN + COUNT aggregation

SELECT
    al.airline                  AS airline_name,
    COUNT(*)                    AS total_flights
FROM flights f
JOIN airline al ON f.airline = al.iata_code
GROUP BY al.airline
ORDER BY total_flights DESC;


-- Query 2: Busiest origin airports by departure volume
-- Business question: Which airports handle the most outbound traffic?
-- Technique: JOIN + COUNT aggregation

SELECT
    a.airport                   AS airport_name,
    a.city                      AS city,
    COUNT(*)                    AS total_departures
FROM flights f
JOIN airport a ON f.origin_airport = a.iata_code
GROUP BY a.airport, a.city
ORDER BY total_departures DESC
LIMIT 20;


-- ============================================================
-- SECTION 2 — DELAY ANALYSIS
-- Identifying where and why delays happen
-- ============================================================

-- Query 3: Average arrival delay per airline
-- Business question: Which airlines are most/least punctual overall?
-- Technique: AVG aggregation with ROUND for readability

SELECT
    al.airline                          AS airline_name,
    ROUND(AVG(f.arrival_delay), 1)      AS avg_arrival_delay_mins,
    COUNT(*)                            AS total_flights
FROM flights f
JOIN airline al ON f.airline = al.iata_code
GROUP BY al.airline
ORDER BY avg_arrival_delay_mins ASC;


-- Query 4: Most delayed departure airports
-- Business question: Which airports consistently cause departure delays?
-- Technique: AVG on departure_delay grouped by airport

SELECT
    a.airport                           AS airport_name,
    a.city                              AS city,
    ROUND(AVG(f.departure_delay), 1)    AS avg_departure_delay_mins,
    COUNT(*)                            AS total_flights
FROM flights f
JOIN airport a ON f.origin_airport = a.iata_code
GROUP BY a.airport, a.city
ORDER BY avg_departure_delay_mins DESC
LIMIT 20;


-- Query 5: Classify flights by delay severity (CASE WHEN)
-- Business question: What share of each airline's flights are truly problematic?
-- Technique: CASE WHEN buckets + percentage calculation
-- Why this matters: Raw averages hide distribution — a 15-min avg
--                   could mean all flights slightly late, or half perfect + half terrible

SELECT
    al.airline                                                          AS airline_name,
    COUNT(*)                                                            AS total_flights,

    -- Count flights in each delay bucket
    SUM(CASE WHEN f.arrival_delay <= 0         THEN 1 ELSE 0 END)      AS on_time,
    SUM(CASE WHEN f.arrival_delay BETWEEN 1
                              AND 30           THEN 1 ELSE 0 END)      AS minor_delay,
    SUM(CASE WHEN f.arrival_delay > 30         THEN 1 ELSE 0 END)      AS severe_delay,

    -- Convert to percentages for easy comparison
    ROUND(100.0 * SUM(CASE WHEN f.arrival_delay <= 0 THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                                AS on_time_pct,
    ROUND(100.0 * SUM(CASE WHEN f.arrival_delay > 30 THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                                AS severe_delay_pct

FROM flights f
JOIN airline al ON f.airline = al.iata_code
GROUP BY al.airline
ORDER BY on_time_pct DESC;


-- ============================================================
-- SECTION 3 — ROUTE ANALYSIS
-- Understanding which city pairs perform well or poorly
-- ============================================================

-- Query 6: Route performance — best and worst city pairs
-- Business question: Which routes are reliably on time vs consistently delayed?
-- Technique: Multi-table JOIN + CTE + CASE WHEN tier labels

WITH route_delays AS (
    -- Step 1: Calculate average delay and volume per route
    SELECT
        a1.city                             AS origin_city,
        a2.city                             AS destination_city,
        ROUND(AVG(f.arrival_delay), 1)      AS avg_delay_mins,
        COUNT(*)                            AS total_flights
    FROM flights f
    JOIN airport a1 ON f.origin_airport      = a1.iata_code
    JOIN airport a2 ON f.destination_airport = a2.iata_code
    GROUP BY a1.city, a2.city
)
-- Step 2: Label each route's performance tier
SELECT
    origin_city,
    destination_city,
    avg_delay_mins,
    total_flights,
    CASE
        WHEN avg_delay_mins <= 0  THEN 'Best performer'
        WHEN avg_delay_mins <= 15 THEN 'Average'
        ELSE                           'Needs improvement'
    END                                 AS performance_tier
FROM route_delays
ORDER BY avg_delay_mins ASC;


-- Query 7: Top airline operating from each airport
-- Business question: Which airline dominates each city's air traffic?
-- Technique: Subquery with MAX to find the market leader per airport

SELECT
    ranked.city                         AS airport_city,
    ranked.airline                      AS dominant_airline,
    ranked.total_flights
FROM (
    -- Inner query: count flights per airline per city
    SELECT
        ap.city,
        al.airline,
        COUNT(*)                        AS total_flights
    FROM flights f
    JOIN airline al ON f.airline        = al.iata_code
    JOIN airport ap ON f.origin_airport = ap.iata_code
    GROUP BY ap.city, al.airline
) ranked
WHERE ranked.total_flights = (
    -- Find the maximum flight count for each city
    SELECT MAX(inner_q.total_flights)
    FROM (
        SELECT ap2.city, COUNT(*) AS total_flights
        FROM flights f2
        JOIN airline al2 ON f2.airline        = al2.iata_code
        JOIN airport ap2 ON f2.origin_airport = ap2.iata_code
        GROUP BY ap2.city, al2.airline
    ) inner_q
    WHERE inner_q.city = ranked.city
)
ORDER BY ranked.city;


-- ============================================================
-- SECTION 4 — ADVANCED ANALYTICS (Window Functions)
-- Ranking and comparing within groups — a key analyst skill
-- ============================================================

-- Query 8: Rank airlines by punctuality within each airport
-- Business question: Who is the best-performing airline at each specific airport?
-- Technique: RANK() OVER (PARTITION BY) — window function
-- Why this matters: An airline may rank #1 nationally but #5 at a specific hub

SELECT
    ap.city                             AS airport_city,
    al.airline                          AS airline_name,
    ROUND(AVG(f.arrival_delay), 1)      AS avg_arrival_delay,
    COUNT(*)                            AS flights_operated,
    RANK() OVER (
        PARTITION BY ap.city            -- rank resets for each airport
        ORDER BY AVG(f.arrival_delay) ASC  -- lower delay = better rank
    )                                   AS punctuality_rank
FROM flights f
JOIN airline al ON f.airline        = al.iata_code
JOIN airport ap ON f.origin_airport = ap.iata_code
GROUP BY ap.city, al.airline
ORDER BY ap.city, punctuality_rank;


-- Query 9: Month-on-month delay trend per airline
-- Business question: Are airlines improving or getting worse over the year?
-- Technique: LAG() window function to compare current vs previous month

WITH monthly_avg AS (
    -- Step 1: Calculate average delay per airline per month
    SELECT
        al.airline,
        MONTH(f.scheduled_departure)        AS month_num,
        ROUND(AVG(f.arrival_delay), 1)      AS avg_delay
    FROM flights f
    JOIN airline al ON f.airline = al.iata_code
    GROUP BY al.airline, MONTH(f.scheduled_departure)
)
-- Step 2: Use LAG() to pull the previous month's value into the current row
SELECT
    airline,
    month_num,
    avg_delay                               AS current_month_delay,
    LAG(avg_delay) OVER (
        PARTITION BY airline
        ORDER BY month_num
    )                                       AS previous_month_delay,
    ROUND(
        avg_delay - LAG(avg_delay) OVER (
            PARTITION BY airline
            ORDER BY month_num
        ), 1
    )                                       AS month_over_month_change
FROM monthly_avg
ORDER BY airline, month_num;


-- ============================================================
-- SECTION 5 — CAPSTONE QUERY
-- Executive airline scorecard — all key metrics in one view
-- ============================================================

-- Query 10: Full airline performance scorecard
-- Business question: How does each airline rank across ALL key metrics?
-- Technique: CTE + CASE WHEN + Window function (RANK)
-- This query is the centrepiece of the project — it answers the
-- question an airline executive or operations analyst would ask first

WITH airline_metrics AS (
    -- Step 1: Compute all raw metrics per airline in one place
    SELECT
        al.airline                                                          AS airline_name,
        COUNT(*)                                                            AS total_flights,
        ROUND(AVG(f.arrival_delay), 1)                                      AS avg_arrival_delay,

        -- On-time rate: flights that arrived on time or early
        ROUND(100.0 * SUM(CASE WHEN f.arrival_delay <= 0 THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                                AS on_time_pct,

        -- Severe delay rate: flights more than 30 mins late
        ROUND(100.0 * SUM(CASE WHEN f.arrival_delay > 30 THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                                AS severe_delay_pct,

        -- Cancellation rate
        ROUND(100.0 * SUM(CASE WHEN f.cancelled = 1 THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                                AS cancellation_pct
    FROM flights f
    JOIN airline al ON f.airline = al.iata_code
    GROUP BY al.airline
)
-- Step 2: Add overall ranking using window function on top of the CTE
SELECT
    airline_name,
    total_flights,
    avg_arrival_delay           AS avg_delay_mins,
    on_time_pct,
    severe_delay_pct,
    cancellation_pct,
    RANK() OVER (
        ORDER BY on_time_pct DESC   -- best on-time rate = rank 1
    )                               AS overall_rank
FROM airline_metrics
ORDER BY overall_rank;


-- ============================================================
-- END OF PROJECT
-- For questions or feedback: [your email / LinkedIn]
-- ============================================================
