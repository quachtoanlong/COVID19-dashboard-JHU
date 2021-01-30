<h1>Create a COVID-19 dashboard</h1>
<h3>Using JHU shared database on Google Biguery.</h3>

The JHU shared databse, although contains most of the necessary data for visualization such as confirmed cases, death cases, or recovered cases, with geospatial data, still needs a number of modification to enable daily tracking. For example, the data is provided in cumulative form so we will need a daily computation to monitor daily changes or look at daily new cases. The level of data has also been changed, from generic country-level data to sub-administrative levels, around March 2020. 

In this dashboard, I applied some data manipulation techniques using SQL to generate additional features that allows up to look at the data from another angle.

Access the dashboard [here](http://bit.ly/covid19QL)
<h3>File list</h3>
Daily data.sql - SQL code to create daily time series
Weekly  data.sql - SQL code to create weekly time series
COVID19.pdf - A PDF version of the dashboard 
