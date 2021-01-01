# Example of main. 
# It only tries some implemented functions in order to see if they work properly

import covidData
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('CovidAnalysis').master('local').getOrCreate()
data = covidData.CovidData(spark)
data.get_cases_per_country_and_date('2020-02-24', 'Afghanistan').show()
data.get_cases_per_country_and_period_of_time('2020-02-24', '2020-05-25','Spain').show()
data.get_new_world_cases_a_day('2020-12-28').show()
data.get_new_cases_a_month_per_country('07').show()
data.get_new_cases_a_month_a_country(7, 'Spain').show()
data.get_total_cases_a_month_per_country(7).show(300)
data.get_countries_with_more_cases_a_month(7, 10).show()
data.get_countries_with_less_cases_a_month(7, 10).show()
data.get_average_cases_per_day_a_month(7).show()
data.get_average_cases_per_day_per_month_a_country('Spain').show()