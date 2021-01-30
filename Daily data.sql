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
        WHERE date = "2021-01-28T23:59:59"
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



  ,final as(
  	select 
  	*,
  	extract(week from date) as week,
  	date = latest_date as latest,

  	case 
  	when total_confirmed is null or total_confirmed = 0 then 0
  	else total_recovered/total_confirmed end
  	as recovery_rate,
  	case 
  	when total_confirmed is null or total_confirmed = 0 then 0
  	else total_death/total_confirmed end
  	as fatality_rate,

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
  	as new_recovered,

  	(total_confirmed - case 
  		when lag(total_confirmed) over (partition by country_region order by date) is null then 0
  		else lag(total_confirmed) over (partition by country_region order by date) end) * 1000000 / pop
  	as new_cases_per_1m,


  	case 
  	when country_region = "World" then 1
  	when country_region = "ASEAN+3" then 2
  	when country_region = "Plus-3" then 3
  	when country_region = "ASEAN" then 4

  	when country_region = 'Brunei Darussalam' then 5
  	when country_region = 'Cambodia' then 6
  	when country_region = 'China' then 7
  	when country_region = 'Hong Kong, China' then 8
  	when country_region = 'Indonesia' then 9
  	when country_region = 'Japan' then 10
  	when country_region = 'Lao PDR' then 11
  	when country_region = 'Malaysia' then 12
  	when country_region = 'Myanmar' then 13
  	when country_region = 'Philippines' then 14
  	when country_region = 'Singapore' then 15
  	when  country_region = 'South Korea' then 16
  	when country_region = 'Thailand' then 17
  	when country_region = 'Vietnam' then 18

  	when country_region = 'Australia' then 19
  	when country_region = 'Belgium' then 20
  	when country_region = 'Brazil' then 21
  	when country_region = 'Canada' then 22
  	when country_region = 'Chile' then 23
  	when country_region = 'France' then 24
  	when country_region = 'Germany' then 25
  	when country_region = 'India' then 26
  	when country_region = 'Iran' then 27
  	when country_region = 'Italy' then 28
  	when country_region = 'Mexico' then 29
  	when country_region = 'Netherlands' then 30
  	when country_region = 'Peru' then 31
  	when country_region = 'Russia' then 32
  	when country_region = 'Saudi Arabia' then 33
  	when country_region = 'Spain' then 34
  	when country_region = 'Switzerland' then 35
  	when country_region = 'Turkey' then 36
  	when country_region = 'United Kingdom' then 37
  	when country_region = 'United States' then 38
  	else 38 + RANK() over (partition by extract(week from date) order by country_region) 
  	end  as ranking

  	from semi_data
  )

  select 
  *,
  case 
  when lag(new_confirmed) over (partition by country_region order by date) is null then 0
  else new_confirmed - lag(new_confirmed) over (partition by country_region order by date) end
  as abs_change_new_confirmed,
  case 
  when lag(new_deaths) over (partition by country_region order by date) is null then 0
  else new_deaths - lag(new_deaths) over (partition by country_region order by date) end
  as abs_change_new_deaths,
  case 
  when lag(new_recovered) over (partition by country_region order by date) is null then 0
  else new_recovered - lag(new_confirmed) over (partition by country_region order by date) end
  as abs_change_new_recovered, 
  case 
  when lag(new_confirmed) over (partition by country_region order by date) is null or lag(new_confirmed) over (partition by country_region order by date) = 0 then 0
  else (new_confirmed - lag(new_confirmed) over (partition by country_region order by date))/lag(new_confirmed) over (partition by country_region order by date) end
  as perct_change_new_confirmed,
  case 
  when lag(new_deaths) over (partition by country_region order by date) is null or lag(new_deaths) over (partition by country_region order by date) = 0 then 0
  else (new_deaths - lag(new_deaths) over (partition by country_region order by date))/lag(new_deaths) over (partition by country_region order by date) end
  as perct_change_new_deaths,
  case 
  when lag(new_recovered) over (partition by country_region order by date) is null or lag(new_recovered) over (partition by country_region order by date) = 0 then 0
  else (new_recovered - lag(new_recovered) over (partition by country_region order by date))/lag(new_recovered) over (partition by country_region order by date) end
  as perct_change_new_recovered,

  case 
  when lag(fatality_rate) over (partition by country_region order by date) is null then 0
  else fatality_rate - lag(fatality_rate) over (partition by country_region order by date) end
  as abs_change_fatality_rate, 
  case 
  when lag(fatality_rate) over (partition by country_region order by date) is null or lag(fatality_rate) over (partition by country_region order by date) = 0 then 0
  else (fatality_rate - lag(fatality_rate) over (partition by country_region order by date))/lag(fatality_rate) over (partition by country_region order by date) end
  as perct_change_fatality_rate,

  case 
  when lag(recovery_rate) over (partition by country_region order by date) is null then 0
  else recovery_rate - lag(recovery_rate) over (partition by country_region order by date) end
  as abs_change_new_recovery_rate, 
  case 
  when lag(recovery_rate) over (partition by country_region order by date) is null or lag(recovery_rate) over (partition by country_region order by date) = 0 then 0
  else (recovery_rate - lag(recovery_rate) over (partition by country_region order by date))/lag(recovery_rate) over (partition by country_region order by date) end
  as perct_change_recovery_rate,    


  
  from final
  
