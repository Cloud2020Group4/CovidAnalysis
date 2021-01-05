'''
Module for general Population Data Processing

Atributes:
   - spark: current SparkSession
   - df_covid_data: dataframe with data from the file 'owid-covid-data.csv'

Methods:
    Various methods that select some specific data and return them as a dataframe.
    Each method has a comment that explains its purpose.
'''
from pyspark.sql.functions import col, asc,desc, when, udf, max, avg
from os.path import dirname, abspath

class populationData:

    # the function receives a SparkSession and initializes the dataframe needed
    def __init__(self, sparkSes):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        self.df_covid_data = self.spark.read.csv(self.dir + '/datasets/owid-covid-data.csv', header = True, inferSchema=True)

    # Given a country gives its up-to-date population indicators (population, population_density,median_age,aged_65_older,aged_70_older,life_expectancy)
    def get_population_data_per_country(self, country):
        return (self.df_covid_data.filter(self.df_covid_data['location']== country)
                .groupBy('location', 'population', 'population_density', 'median_age', 'aged_65_older', 'aged_70_older', 'life_expectancy')
                .agg(max('date'))
                .select('location', 'population', 'population_density', 'median_age', 'aged_65_older', 'aged_70_older', 'life_expectancy'))

    # Return the up-to-date population indicators (gdp_per_capita, extreme_poverty, human_development_index) in average for each continent
    def get_population_data_by_continent(self):
        return (self.df_covid_data.filter(self.df_covid_data['population'].isNotNull())
                .filter(self.df_covid_data['population_density'].isNotNull())
                .filter(self.df_covid_data['median_age'].isNotNull())
                .filter(self.df_covid_data['aged_65_older'].isNotNull())
                .filter(self.df_covid_data['aged_70_older'].isNotNull())
                .filter(self.df_covid_data['life_expectancy'].isNotNull())
                .filter(self.df_covid_data['continent'].isNotNull())
                .groupBy('location','continent','population', 'population_density', 'median_age', 'aged_65_older', 'aged_70_older', 'life_expectancy')
                .agg(max('date'))
                .select('continent', 'population', 'population_density', 'median_age', 'aged_65_older', 'aged_70_older', 'life_expectancy')
                .groupBy('continent')
                .agg(avg('population').alias('population'), avg('population_density').alias('population_density'), avg('median_age').alias('median_age'), avg('aged_65_older').alias('aged_65_older'), avg('aged_70_older').alias('aged_70_older'), avg('life_expectancy').alias('life_expectancy')))