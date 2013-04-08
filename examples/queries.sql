SELECT
  d1.day,
  d2.location,
  SUM(f.fact_count)
FROM
  fact_count_allthesales f
JOIN 
  dim_date d1 ON f.dim_date = d1.id
JOIN
  dim_location d2 ON f.dim_location = d2.id
GROUP BY
  d1.day, d2.location
