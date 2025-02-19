# Module 4 Homework: dbt Analytics

### Quarter 5: Taxi Quarterly Revenue Growth

```sql
WITH RankedData AS (
    SELECT 
        service_type, 
        year_quarter, 
        yoy_growth_percentage,
        RANK() OVER(PARTITION BY service_type ORDER BY yoy_growth_percentage DESC) AS best_rank,
        RANK() OVER(PARTITION BY service_type ORDER BY yoy_growth_percentage ASC) AS worst_rank
    FROM `focus-heuristic-448107-a5.nytaxi_hw4.fct_taxi_monthly_zone_revenue`
    WHERE service_type IN ('Yellow', 'Green')
      AND CAST(SUBSTR(year_quarter, 1, 4) AS INT) = 2020
)
SELECT * FROM RankedData
ORDER BY service_type, best_rank;
```



### Question 6: P97/P95/P90 Taxi Monthly Fare




### Question 7: Top #Nth longest P90 travel time Location for FHV