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

class EconomicData:

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

    # Given a country gives its up-to-date economic_indicator it can be gdp_per_capita,extreme_poverty,human_development_index
    def get_economic_indicator_per_country(self, country, economic_indicator):
        return (self.df_covid_data.filter(self.df_covid_data['location']== country)
                .groupBy('location', economic_indicator)
                .agg(max('date'))
                .select('location', economic_indicator))

    # Returns the 'num_countries' countries with the highest up-to-date economic_indicator
    def get_countries_with_highest_economic_indicator(self, num_countries, economic_indicator):
        return (self.df_covid_data.filter(self.df_covid_data[economic_indicator].isNotNull())
                .groupBy('location', economic_indicator)
                .agg(max('date'))
                .select('location', economic_indicator)
                .sort(col(economic_indicator).desc()).limit(num_countries))
    
    # Returns the 'num_countries' countries with the lowest up-to-date economic_indicator
    def get_countries_with_lowest_economic_indicator(self, num_countries, economic_indicator):
        return (self.df_covid_data.filter(self.df_covid_data[economic_indicator].isNotNull())
                .groupBy('location', economic_indicator)
                .agg(max('date'))
                .select('location', economic_indicator)
                .sort(col(economic_indicator).asc()).limit(num_countries))

    # Returns the up-to-date economic_indicator for each continent
    def get_economic_indicator_by_continent(self, economic_indicator):
        return (self.df_covid_data.filter(self.df_covid_data[economic_indicator].isNotNull())
                .filter(self.df_covid_data['continent'].isNotNull())
                .groupBy('location','continent', economic_indicator)
                .agg(max('date'))
                .select('continent', economic_indicator)
                .groupBy('continent')
                .agg(avg(economic_indicator).alias(economic_indicator))
                .sort(col(economic_indicator).desc()))

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