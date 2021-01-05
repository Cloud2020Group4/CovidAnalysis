# Example of main. 
# It only tries some implemented functions in order to see if they work properly

import economicData, populationData, processData
import covidData, covidData_graphs, os, sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('CovidAnalysis').master('local').getOrCreate()
data = covidData.CovidData(spark)
'''
data.get_cases_per_country_and_date('2020-02-24', 'Afghanistan').show()
'''
#data.get_cases_a_country_and_period_of_time('2020-02-24', '2020-05-25','Spain', plot = True).show()
'''
data.get_new_world_cases_a_day('2020-12-28').show()
data.get_new_cases_a_month_per_country('07').show()
data.get_new_cases_a_month_a_country(7, 'Spain').show()
data.get_total_cases_a_month_per_country(7).show(300)
data.get_countries_with_more_cases_a_month(7, 10).show()
data.get_countries_with_less_cases_a_month(7, 10).show()
data.get_average_cases_per_day_a_month(7).show()
data.get_average_cases_per_day_per_month_a_country('Spain').show()
'''
#covidData_graphs.plot_cases_a_country_a_mont(spark)
#print(os.path.dirname(os.path.realpath(sys.argv[0])))

#data.get_new_world_cases_a_day('2020-07-07').show()
#data.get_new_world_cases_a_day_1('2020-07-07').show()

#data.get_countries_with_more_cases_a_month(7, 15, plot = True).show()
#data.get_countries_with_more_deaths_a_month(7, 15, plot = True).show()
#data.get_data_a_month_a_country(7, 'Spain', plot = True).show()

data.get_average_data_per_day_per_month_a_country('Spain', plot = True).show()


#economicData = economicData.EconomicData(spark)
#df = get_economic_data_per_country('Spain')
#df = get_economic_indicator_per_country('Spain', 'gdp_per_capita')
#df = get_countries_with_highest_economic_indicator(10, 'gdp_per_capita')
#df = get_countries_with_lowest_economic_indicator(10, 'gdp_per_capita')
#df = get_economic_indicator_by_continent('gdp_per_capita')
#df = get_economic_data_by_continent()
#df.coalesce(1).write.save(path='./output', format='csv', mode='append', sep='\t')
© 2021 GitHub, Inc.
Terms
Privacy
Security
Status
Help
Contact GitHub
Pricing
API
Training
Blog
About
