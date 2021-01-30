<h1>Create a COVID-19 dashboard</h1>
<h3>Using JHU shared database on Google Biguery.</h3>

*<i>As in Jan 2021: Recoveries data in the US has been removed in the JHU dataset, thus affecting the recovery/ active data in the dashboard. Such data is still valid for other countries but will not for US-included aggregation.</i>

The JHU shared database, although contains most of the necessary data for visualization such as confirmed cases, death cases, or recovered cases, with geospatial data, still needs several modifications to enable daily tracking. For example, the data is provided in the cumulative form so we will need a daily computation to monitor daily changes or look at daily new cases. The level of data has also been changed, from generic country-level data to sub-administrative levels, around March 2020. 

In this dashboard, I applied some data manipulation techniques using SQL to generate additional features that allow up to look at the data from another angle.

Access the dashboard [here](http://bit.ly/covid19QL)
<h3>File list</h3>
Daily data.sql - SQL code to create a daily time series
Weekly  data.sql - SQL code to create weekly time series
COVID19.pdf - A PDF version of the dashboard 
