import covidData, economicData,populationData, processData, vaccinesData,physiciansData, machineLearning, vaccinesData
import shutil
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CovidAnalysis').master('local[*]').getOrCreate()
data = machineLearning.MachineLearning(spark, 'local')
df =data.ml_covid_data(['total_deaths_per_million', 'gdp_per_capita'])
df.show()
shutil.rmtree('/home/carla/Application/output', ignore_errors = True, onerror = None)
df.coalesce(1).write.format('csv').options(header=True).save('/home/carla/Application/output')
