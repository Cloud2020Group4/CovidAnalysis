'''
Module for general Economic Data Processing

Atributes:
   - spark: current SparkSession
   - df_covid_data: dataframe with data from the file 'owid-covid-data.csv'

Methods:
    Various methods that select some specific data and return them as a dataframe.
    Each method has a comment that explains its purpose.
'''
from pyspark.sql.functions import col, asc,desc, when, udf, max, avg
from os.path import dirname, abspath

class economicData:

    # the function receives a SparkSession and initializes the dataframe needed
    def __init__(self, sparkSes):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        self.df_covid_data = self.spark.read.csv(self.dir + '/datasets/owid-covid-data.csv', header = True, inferSchema=True)

    # Given a country gives its up-to-date economic indicators (gdp_per_capita, extreme_poverty, human_development_index)
    def get_economic_data_per_country(self, country):
        return (self.df_covid_data.filter(self.df_covid_data['location']== country)
                .groupBy('location', 'gdp_per_capita', 'extreme_poverty', 'human_development_index')
                .agg(max('date'))
                .select('location', 'gdp_per_capita', 'extreme_poverty', 'human_development_index'))

    # Return the up-to-date economic indicators (gdp_per_capita, extreme_poverty, human_development_index) for each continent
    def get_economic_data_by_continent(self):
        return (self.df_covid_data.filter(self.df_covid_data['gdp_per_capita'].isNotNull())
                .filter(self.df_covid_data['extreme_poverty'].isNotNull())
                .filter(self.df_covid_data['human_development_index'].isNotNull())
                .filter(self.df_covid_data['continent'].isNotNull())
                .groupBy('location','continent','gdp_per_capita', 'extreme_poverty', 'human_development_index')
                .agg(max('date'))
                .select('continent', 'gdp_per_capita', 'extreme_poverty', 'human_development_index')
                .groupBy('continent')
                .agg(avg('gdp_per_capita').alias('gdp_per_capita'), avg('extreme_poverty').alias('extreme_poverty'), avg('human_development_index').alias('human_development_index'))
                .sort(col('gdp_per_capita').desc(), col('extreme_poverty').asc(), col('human_development_index').desc()))