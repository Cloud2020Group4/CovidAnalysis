from pyspark.sql.functions import max, col, asc, desc, avg

from os.path import dirname, abspath

import covidData_graphs

class Physicians:
    # the function receives a SparkSession and initializes the dataframes needed
    #Probar->
    #import physicians
    #from pyspark.sql import SparkSession
    #spark = SparkSession.builder.appName('CovidAnalysis').master('local').getOrCreate()
    #phys = physicians.Physicians(spark)
    def __init__(self, sparkSes):
        self.spark = sparkSes
        self.df_physicians = self.spark.read.csv('medical_doctors_per_1000_people.csv', header = True, inferSchema=True)
        self.df_continents = self.spark.read.csv('countries.csv', header = True, inferSchema=True)
        self.dir = dirname(dirname(abspath(__file__)))
    #Returns the number of doctors per 1000 people given a country and year
    #Probar-> phys.get_doctors_country_and_year('1962','Albania').show()
    def get_doctors_country_and_year(self, year, country):
        df = (self.df_physicians.filter((self.df_physicians['country'] == country) & self.df_physicians[year].isNotNull()).select(year, 'country'))
        return df
    #Returns the top country considering the number of doctors given a year
    #Probar->phys.get_top_country(10,"2016",True).show()
    def get_top_country(self,num_countries, year, plot = False):
        df = (self.df_physicians.filter(self.df_physicians[year].isNotNull()).select(year,'country').sort(col(year).desc()).limit(num_countries))
        if plot:
            save_name = './graphs/'+'top_country_physicians.png'
            title = 'Top countries: Medical Doctors per 1000 people'
            covidData_graphs.plot_bars(df, 'country', year, title, save_name)
            #covidData_graphs.plot_pie(df, 'country', year, title, save_name)
        return df
    #Returns the bottom country considering the number of doctors given a year
    #Probar->phys.get_bottom_country(10,"2016",True).show()
    def get_bottom_country(self,num_countries, year, plot = False):
        df = (self.df_physicians.filter(self.df_physicians[year].isNotNull()).select(year,'country').sort(year).limit(num_countries))
        if plot:
            save_name = './graphs/'+'bottom_country_physicians.png'
            title = 'Bottom countries: Medical Doctors per 1000 people'
            covidData_graphs.plot_bars(df, 'country', year, title, save_name)
        return df
    #Returns the average doctors per 1000 people in the whole world given a year
    #Probar->phys.get_avg_year("2016").show()
    def get_avg_year(self, year):
        df = (self.df_physicians.filter(self.df_physicians[year].isNotNull()).select(year).agg(avg(year)))
        return df
    #Returns the continent of the country
    def get_continent(self, country):
        return self.df_continents.filter(self.df_continents['name'] == country).select('region')

    #Returns the bottom country of a region considering the number of doctors given a year
    #Probar->phys.get_bottom_country_continent(5,"2016",'Europe', True).show()
    def get_bottom_country_continent(self,num_countries, year, continent, plot = False):
        df = (self.df_physicians.filter(self.df_physicians[year].isNotNull()).join(self.df_continents, self.df_physicians.country == self.df_continents.name).filter(self.df_continents['region'] == continent).select(year,'country','region').sort(year).limit(num_countries))
        if plot:
            save_name = './graphs/'+'bottom_country_continent_physicians.png'
            title = 'Bottom countries in continent: Medical Doctors per 1000 people'
            covidData_graphs.plot_bars(df, 'country', year, title, save_name)
        return df
    
    #Returns the top country of a region considering the number of doctors given a year
    #phys.get_top_country_continent(5,"2016",'Europe', True).show()
    def get_top_country_continent(self,num_countries, year, continent, plot = False):
        df = (self.df_physicians.filter(self.df_physicians[year].isNotNull()).join(self.df_continents, self.df_physicians.country == self.df_continents.name).filter(self.df_continents['region'] == continent).select(year,'country','region').sort(col(year).desc()).limit(num_countries))
        if plot:
            save_name = './graphs/'+'top_country_continent_physicians.png'
            title = 'Top countries continent: Medical Doctors per 1000 people'
            covidData_graphs.plot_bars(df, 'country', year, title, save_name)
        return df

    #Returns the average doctors per 1000 people in a region
    def get_avg_year_continent(self, year, continent):
        df = (self.df_physicians.filter(self.df_physicians[year].isNotNull()).join(self.df_continents, self.df_physicians.country == self.df_continents.name).filter(self.df_continents['region'] == continent).select(year,'country','region').agg(avg(year)))
        return df
