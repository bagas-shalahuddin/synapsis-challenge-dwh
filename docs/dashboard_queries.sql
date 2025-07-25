-- Dashboard Queries for Coal Mining Data
-- 1. Daily Production Trends (Line Chart)
-- Shows coal production trends over time 
SELECT 
    date,
    total_production_daily
FROM coal_mining.daily_production_metrics
WHERE date >= {{month_start_date}}
   AND date <= {{month_end_date}}
ORDER BY date;

-- 2. Mine Performance Comparison (Bar Chart)
-- Compares total production and average quality across different mining locations
SELECT 
    m.mine_name || ' (' || m.mine_code || ')' AS mine_name_with_code,
    AVG(pl.quality_grade) AS average_quality_grade,
    COUNT(*) AS total_records,
    SUM(pl.tons_extracted) AS total_production
FROM coal_mining.production_logs pl
JOIN coal_mining.mines m ON pl.mine_id = m.mine_id
WHERE pl.tons_extracted > 0
GROUP BY m.mine_id, m.mine_name, m.mine_code
ORDER BY m.mine_id;

-- 3. Weather vs Production Correlation (Scatter Plot)
-- Shows relationship between rainfall and production
SELECT 
    rainfall_mm,
    total_production_daily,
    weather_impact
FROM coal_mining.daily_production_metrics
WHERE total_production_daily > 0
ORDER BY rainfall_mm;
