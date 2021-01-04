'''
Module for general Data Processing

Atributes:
   - spark: current SparkSession
   - df_covid_data: dataframe with data from the file 'owid-covid-data.csv'

Methods:
    Various methods that select some specific data and return them as a dataframe.
    Each method has a comment that explains its purpose.
'''
from pyspark.sql.functions import col, asc,desc, when, udf, max, avg
from os.path import dirname, abspath

class processData:

    # the function receives a SparkSession and initializes the dataframe needed
    def __init__(self, sparkSes):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        self.df_covid_data = self.spark.read.csv(self.dir + '/datasets/owid-covid-data.csv', header = True, inferSchema=True)

    # Given a country gives its up-to-date indicator's value
    def get_indicator_per_country(self, country, indicator):
        return (self.df_covid_data.filter(self.df_covid_data['location']== country)
                .groupBy('location', indicator)
                .agg(max('date'))
                .select('location', indicator))

    # Returns the 'num_countries' countries with the highest up-to-date indicator's value
    def get_countries_with_highest_indicator(self, num_countries, indicator):
        return (self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull())
                .groupBy('location', indicator)
                .agg(max('date'))
                .select('location', indicator)
                .sort(col(indicator).desc()).limit(num_countries))
    
    # Returns the 'num_countries' countries with the lowest up-to-date indicator's value
    def get_countries_with_lowest_indicator(self, num_countries, indicator):
        return (self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull())
                .groupBy('location', indicator)
                .agg(max('date'))
                .select('location', indicator)
                .sort(col(indicator).asc()).limit(num_countries))

    # Returns the up-to-date indicator's average value for each continent
    def get_indicator_by_continent(self, indicator):
        return (self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull())
                .filter(self.df_covid_data['continent'].isNotNull())
                .groupBy('location','continent', indicator)
                .agg(max('date'))
                .select('continent', indicator)
                .groupBy('continent')
                .agg(avg(indicator).alias(indicator))
                .sort(col(indicator).desc()))