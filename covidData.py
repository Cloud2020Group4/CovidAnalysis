'''
Module for general Covid Data Processing

Atributes:
   - spark: current SparkSession
   - df_covid_data: dataframe with data from the file 'owid-covid-data.csv'
   - df_world_region: dataframe with data from the file 'countries.csv'

Methods:
    Various methods that select some specific data and return them as a dataframe.
    Each method has a commet that explains its purpose.
'''
import utils
from pyspark.sql.functions import month, desc

class CovidData:
    # the function receives a SparkSession and initializes the dataframes needed
    def __init__(self, sparkSes):
        self.spark = sparkSes
        self.df_covid_data = self.spark.read.csv('datasets/owid-covid-data.csv', header = True, inferSchema=True)
        self.df_world_region = self.spark.read.csv('datasets/countries.csv')

    # Given a date and a country returns the number of new cases in that day and that country
    def get_cases_per_country_and_date(self, date, country):
        return self.df_covid_data.filter((self.df_covid_data['date'] == date) & (self.df_covid_data['location'] == country)).select('location', 'date','new_cases')
    
    # Given two dates and a country returns the daily cases during that period of time in
    # the given country
    def get_cases_per_country_and_period_of_time(self, date1, date2, country):
        return (self.df_covid_data.filter(self.df_covid_data['location'] == country)
                .filter((self.df_covid_data['date'] <= date2) & (self.df_covid_data['date'] >= date1))
                .select('location', 'date', 'new_cases'))

    # Given a date the function returns the total number of new cases that day
    def get_new_world_cases_a_day(self, date):
        return (self.df_covid_data.filter(self.df_covid_data['date'] == date)
                .select('date', 'new_cases')
                .groupBy('date').agg({'new_cases': 'sum'}))

    # Given a month returns the cases each day in that month for each country
    def get_new_cases_a_month_per_country(self, this_month):
        df_aux = self.df_covid_data.select('location', 'date', 'new_cases', month('date').alias('month'))
        return (df_aux.filter(df_aux['month'] == this_month)
                .select('location', 'date', 'new_cases'))

    # Same that the previous one but returns data for only a specified country
    def get_new_cases_a_month_a_country(self, this_month, country):
        df = self.get_new_cases_a_month_per_country(this_month)
        return (df.filter(df['location'] == country))

    # Given a month returns the total number of cases that month in each country
    def get_total_cases_a_month_per_country(self, this_month):
        return (self.get_new_cases_a_month_per_country(this_month)
                .select('location', 'new_cases')
                .groupBy('location').agg({'new_cases': 'sum'}))

    # Returns the 'num_countries' countries with more new cases in the month 'this_month'
    def get_countries_with_more_cases_a_month(self, this_month, num_countries):
        df_aux = (self.get_total_cases_a_month_per_country(this_month)
                .sort(desc('sum(new_cases)')))
        return (df_aux.filter((df_aux['location'] != 'World') & (df_aux['location'] != 'International'))
                .limit(num_countries))

    # Returns the 'num_countries' countries with less new cases in the month 'this_month'    
    def get_countries_with_less_cases_a_month(self, this_month, num_countries):
        df_aux = (self.get_total_cases_a_month_per_country(this_month)
                .sort('sum(new_cases)'))
        return (df_aux.filter((df_aux['location'] != 'World') & (df_aux['location'] != 'International') & (df_aux['sum(new_cases)'].isNotNull()))
                .limit(num_countries))

    # Returns the avaerage cases per day in a month for each country
    def get_average_cases_per_day_a_month(self, this_month):
        df_aux = self.df_covid_data.select('location', 'new_cases', month('date').alias('month'))
        return (df_aux.filter(df_aux['month'] == this_month)
                .groupBy('month', 'location').agg({'new_cases': 'avg'}))
    # Returns the average cases per day each month for a specified country
    def get_average_cases_per_day_per_month_a_country(self, country):
        df_aux = self.df_covid_data.select('location', 'new_cases', month('date').alias('month'))
        return (df_aux.filter(df_aux['location'] == country)
                .groupBy('month', 'location').agg({'new_cases': 'avg'})
                .sort('month'))


    
