  with country_pop AS(
    SELECT
      case 
      when country = "Iran, Islamic Rep." then "Iran"
      when country = "Egypt, Arab Rep." then "Egypt"
      when country = "Macao SAR, China" then "Macau"
      when country = "Syrian Arab Republic" then "Syria"
      when country = "Hong Kong SAR, China" then "Hong Kong, China"
      when country = "Venezuela, RB" then "Venezuela"
      when country = "Korea, Rep." then "South Korea"
      else country end AS country,
      year_2018
      FROM
      `bigquery-public-data.world_bank_global_population.population_by_country`) 

  ,update_data AS (
    select 
      * EXCEPT (country_region,date),
      DATETIME_ADD(DATETIME(date, "23:59:59"), INTERVAL 0 HOUR) as date,
      case 
      when country_region = "US" then "United States"
      when country_region = "Macao SAR" then "Macau"

      when country_region = "Mainland China" then "China" 
      when country_region = "Iran (Islamic Republic of)" then "Iran"

      when country_region = "Republic of Korea" then "South Korea"
      when country_region = "Korea, South" then "South Korea"


      when country_region = "Hong Kong" then "Hong Kong, China"
      when country_region = "Hong Kong SAR" then "Hong Kong, China"    
      when province_state = "Hong Kong" then "Hong Kong, China"    

      when country_region = "Taiwan*" then "Taiwan"
      when country_region = "Burma" then "Myanmar"
      when country_region = "Viet Nam" then "Vietnam"
      when country_region = "Laos" then "Lao PDR"
      when country_region =  "Brunei" then "Brunei Darussalam"


      when country_region = "UK" then "United Kingdom"
      when country_region = "Russia" then "Russian Federation"    


      else country_region end as country_region
      from
      `bigquery-public-data.covid19_jhu_csse.summary`
      UNION ALL
    select 
      * EXCEPT (country_region,date),
      DATETIME_ADD(DATETIME(date, "23:59:59"), INTERVAL 0 HOUR) as date,
      "World" as country_region
      from
      `bigquery-public-data.covid19_jhu_csse.summary`     
      )  

  ,cases AS (
    SELECT
      *
      ,country_pop.year_2018 AS pop_year_2018
      ,max(date) OVER (PARTITION BY country_region) as latest_date

      FROM 
      update_data
      LEFT JOIN 
      country_pop ON update_data.country_region = country_pop.country
        ORDER BY date desc

  )


  ,semi_data as (

    SELECT
    cases.date AS date,
    cases.country_region AS country_region,
    max(cases.latest_date) as latest_date,
    SUM(cases.confirmed) AS total_confirmed,
    SUM(cases.deaths) AS total_death,
    SUM(cases.recovered) AS total_recovered,
    MAX(pop_year_2018) as pop,
    SUM(cases.confirmed)/MAX(pop_year_2018) * 1000000 AS confirmed_cases_per_1000000
    FROM
    cases

    GROUP BY
    country_region, date


    UNION ALL
    SELECT
    cases.date AS date,
    "ASEAN+3" AS country_region,
    max(cases.latest_date) as latest_date,
    SUM(cases.confirmed) AS total_confirmed,
    SUM(cases.deaths) AS total_death,
    SUM(cases.recovered) AS total_recovered,
    SUM(pop_year_2018) as pop,
    SUM(cases.confirmed)/SUM(pop_year_2018) * 1000000 AS confirmed_cases_per_1000000
    FROM
    cases

    WHERE 
    country_region in ('Vietnam',
      'China',
      'Japan',
      'South Korea',
      'Hong Kong, China',
      'Lao PDR',
      'Cambodia',
      'Thailand',
      'Myanmar',
      'Singapore',
      'Indonesia',
      'Malaysia',
      'Brunei Darussalam',
      'Philippines')
    GROUP BY
    country_region, date  

    UNION ALL
    SELECT
    cases.date AS date,
    "Plus-3" AS country_region,
    max(cases.latest_date) as latest_date,
    SUM(cases.confirmed) AS total_confirmed,
    SUM(cases.deaths) AS total_death,
    SUM(cases.recovered) AS total_recovered,
    SUM(pop_year_2018) as pop,
    SUM(cases.confirmed)/SUM(pop_year_2018) * 1000000 AS confirmed_cases_per_1000000
    FROM
    cases

    WHERE 
    country_region in ('China',
      'Hong Kong, China',    
      'Japan',
      'South Korea')
    GROUP BY
    country_region, date

    UNION ALL
    SELECT
    cases.date AS date,
    "ASEAN" AS country_region, 

    max(cases.latest_date) as latest_date,
    SUM(cases.confirmed) AS total_confirmed,
    SUM(cases.deaths) AS total_death,
    SUM(cases.recovered) AS total_recovered,
    SUM(pop_year_2018) as pop,
    SUM(cases.confirmed)/SUM(pop_year_2018) * 1000000 AS confirmed_cases_per_1000000      
    FROM
    cases

    WHERE 
    country_region in ('Vietnam',
      'Lao PDR',
      'Cambodia',
      'Thailand',
      'Myanmar',
      'Singapore',
      'Indonesia',
      'Malaysia',
      'Brunei Darussalam',
      'Philippines')
    GROUP BY
    country_region, date
  )

,week_data as (
select 
*,
extract(week from date) as week,
date = latest_date as latest,

case 
  when lag(total_confirmed) over (partition by country_region order by date) is null then 0
  else lag(total_confirmed) over (partition by country_region order by date) end
  as last_confirmed,
total_confirmed - case 
  when lag(total_confirmed) over (partition by country_region order by date) is null then 0
  else lag(total_confirmed) over (partition by country_region order by date) end
  as new_confirmed,
total_death - case 
  when lag(total_death) over (partition by  country_region order by date) is null then 0
  else lag(total_death) over (partition by  country_region order by date) end
  as new_deaths,
total_recovered - case 
  when lag(total_recovered) over (partition by country_region order by date) is null then 0
  else lag(total_recovered) over (partition by country_region order by date) end
  as new_recovered
from semi_data
)

select 
*,
avg(new_confirmed) over (partition by country_region, week) as week_total_confirmed,
avg(new_deaths) over (partition by country_region, week) as week_total_death,
avg(new_recovered) over (partition by country_region, week) as week_total_recovered
from week_data
