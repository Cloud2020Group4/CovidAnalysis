import covidData, economicData,populationData, processData
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CovidAnalysis').master('local').getOrCreate()
data = processData.ProcessData(spark)
df =data.get_countries_with_lowest_indicator(15,'gdp_per_capita')
df.show()