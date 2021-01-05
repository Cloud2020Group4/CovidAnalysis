'''
Module for general Data Processing

Atributes:
   - spark: current SparkSession
   - df_covid_data: dataframe with data from the file 'owid-covid-data.csv'

Methods:
    Various methods that select some specific data and return them as a dataframe.
    Each method has a comment that explains its purpose.
'''
from pyspark.sql.functions import col, asc,desc, when, udf, min, max, avg, struct, row_number
from os.path import dirname, abspath
from pyspark.sql.window import Window
import covidData_graphs
import utils

class ProcessData:

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

    # Give the up-to-date indicator's value for all the countries
    def get_indicator_all_countries(self, indicator):
        return (self.df_covid_data.filter((self.df_covid_data['location'] != 'World') & (self.df_covid_data['location'] != 'International') & self.df_covid_data[indicator].isNotNull())
                .groupBy('location', indicator)
                .agg(max('date'))
                .select('location', indicator)
                .sort(col('location').asc()))

    # Returns the 'num_countries' countries with the highest up-to-date indicator's value
    def get_countries_with_highest_indicator(self, num_countries, indicator, plot = False):
        df = (self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull() & (self.df_covid_data['location'] != 'World') & (self.df_covid_data['location'] != 'International'))
                .groupBy('location', indicator)
                .agg(max('date'))
                .select('location', indicator)
                .sort(col(indicator).desc()).limit(num_countries))
        
        if plot:
            if num_countries > 15:
                print("To many countries to plot!")
            else:
                save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_countries_highest_' + indicator +  '.png'
                title = 'Countries with highest ' + utils.get_column_natural_name(indicator)
                covidData_graphs.plot_bars(df, 'location', indicator, title, save_name)

        return df
    
    # Returns the 'num_countries' countries with the lowest up-to-date indicator's value
    def get_countries_with_lowest_indicator(self, num_countries, indicator,plot = False):
        df = (self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull()  & (self.df_covid_data['location'] != 'World') & (self.df_covid_data['location'] != 'International'))
                .groupBy('location', indicator)
                .agg(max('date'))
                .select('location', indicator)
                .sort(col(indicator).asc()).limit(num_countries))

        if plot:
            if num_countries > 15:
                print("To many countries to plot!")
            else:
                save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_countries_lowest_' + indicator +  '.png'
                title = 'Countries with lowest ' + utils.get_column_natural_name(indicator)
                covidData_graphs.plot_bars(df, 'location', indicator, title, save_name)

        return df

    # Returns the up-to-date indicator's average, min and max value for each continent
    def get_indicator_by_continent(self, indicator,plot = False):
        df = self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull()) \
                 .filter(self.df_covid_data['continent'].isNotNull()) \
                 .groupBy('location','continent', indicator) \
                 .agg(max('date'))

        windowSpec = Window.partitionBy('continent').orderBy(desc(indicator))
        windowSpecAgg = Window.partitionBy('continent')
        df = df.withColumn("row",row_number().over(windowSpec)) \
                .withColumn('avg', avg(df[indicator]).over(windowSpecAgg)) \
                .withColumn('min', min(df[indicator]).over(windowSpecAgg)) \
                .withColumn('max', max(df[indicator]).over(windowSpecAgg)) \
                .where(col("row")==1).select('continent','avg','min','max')

        if plot:
                save_name = self.dir + '/graphs/'+ 'avg_min_max_' + indicator + '_per_continent.png'
                title = 'AVG, MIN and MAX ' + utils.get_column_natural_name(indicator) + ' per continent'
                covidData_graphs.plot_three_bars_continent(df, 'continent', 'avg', 'min', 'max', title, save_name, 'avg ' + indicator, 'min ' + indicator, 'max ' + indicator)

        return df
    
    # Returns the 'num_countries' countries with the highest up-to-date indicator's value for each continent
    def get_countries_with_highest_indicator_per_continent(self, num_countries, indicator, plot = False): 
        df = self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull()) \
                .filter(self.df_covid_data['continent'].isNotNull()) \
                .groupBy('continent', 'location', indicator) \
                .agg(max('date'))
        windowSpec = Window.partitionBy('continent').orderBy(desc(indicator))
        df = df.withColumn('row',row_number().over(windowSpec)) \
                .where(col('row') <= num_countries).select('continent','location',indicator)

        if plot:
            if num_countries > 15:
                print("To many countries to plot!")
            else:
                for continent in df.select('continent').distinct().collect():
                    save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_countries_highest_' + indicator  + '_in_ ' + continent[0] + '.png'
                    title = 'Countries with highest ' + utils.get_column_natural_name(indicator) + ' in ' + continent[0]
                    covidData_graphs.plot_bars(df.where(col('continent') == continent[0]), 'location', indicator, title, save_name)
        return df

    # Returns the 'num_countries' countries with the lowest up-to-date indicator's value for each continent
    def get_countries_with_lowest_indicator_per_continent(self, num_countries, indicator,plot = False): 
        df = self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull()) \
                .filter(self.df_covid_data['continent'].isNotNull()) \
                .groupBy('continent', 'location', indicator) \
                .agg(max('date'))
        windowSpec = Window.partitionBy('continent').orderBy(asc(indicator))
        df = df.withColumn('row',row_number().over(windowSpec)) \
                .where(col('row') <= num_countries).select('continent','location',indicator)

        if plot:
            if num_countries > 15:
                print("To many countries to plot!")
            else:
                for continent in df.select('continent').distinct().collect():
                    save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_countries_lowest_' + indicator  + '_in_ ' + continent[0] + '.png'
                    title = 'Countries with lowest ' + utils.get_column_natural_name(indicator) + ' in ' + continent[0]
                    covidData_graphs.plot_bars(df.where(col('continent') == continent[0]), 'location', indicator, title, save_name)
        return df