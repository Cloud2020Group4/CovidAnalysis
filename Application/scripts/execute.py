import covidData, economicData,populationData, processData, vaccinesData,physiciansData, machineLearning, vaccinesData
import shutil
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CovidAnalysis').master('local[*]').getOrCreate()
data = processData.ProcessData(spark, 'local', '/mnt/c/hlocal/CovidAnalysis/Application/saved_outputs/results_14-01-2021_23-32-23')
df =data.get_countries_with_highest_indicator_per_continent(12,'extreme_poverty',plot=True)

df.show()
shutil.rmtree('/mnt/c/hlocal/CovidAnalysis/Application/saved_outputs/results_14-01-2021_23-32-23/output', ignore_errors = True, onerror = None)
df.coalesce(1).write.format('csv').options(header=True).save('/mnt/c/hlocal/CovidAnalysis/Application/saved_outputs/results_14-01-2021_23-32-23/output')
