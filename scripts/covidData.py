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
from pyspark.sql.functions import month, desc
import covidData_graphs
import utils
from os.path import dirname, abspath

class CovidData:
    # the function receives a SparkSession and initializes the dataframes needed
    def __init__(self, sparkSes):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        self.df_covid_data = self.spark.read.csv(self.dir + '/datasets/owid-covid-data.csv', header = True, inferSchema=True)
        self.df_world_region = self.spark.read.csv(self.dir + '/datasets/countries.csv')

    # Given a date and a country returns the number of new cases and new deaths in that day and that country
    def get_data_a_country_a_date(self, date, country):
        df = self.df_covid_data.dropna(subset =('location', 'date', 'new_cases', 'new_deaths'))
        return (df.filter((df['date'] == date) & (df['location'] == country))
                .select('location', 'date','new_cases','new_dates'))
    
    # Given two dates and a country returns the daily cases and deaths during that period of time in
    # the given country
    def get_data_a_country_a_period_of_time(self, date1, date2, country, plot = False):
        df = self.df_covid_data.dropna(subset =('location', 'date', 'new_cases', 'new_deaths'))
        df = (df.filter(self.df_covid_data['location'] == country)
                .filter((df['date'] <= date2) & (df['date'] >= date1))
                .select('location', 'date', 'new_cases', 'new_deaths'))
        if plot:
            # Plot new cases
            save_name = self.dir +'/graphs/'+'new_cases_' + country + '_from_' + date1 + '_to_' + date2 + '.png'
            title = 'Daily Cases in ' + country + ' between ' + date1 + ' and ' + date2
            covidData_graphs.plot_dataframe_with_date(df, 'date', 'new_cases', title, save_name, ylabel = 'New Covid-19 Cases')

            # Plot new deaths
            save_name = self.dir + '/graphs/' +'new_deaths_' + country + '_from_' + date1 + '_to_' + date2 + '.png'
            title = 'Daily Deaths in ' + country + ' between ' + date1 + ' and ' + date2
            covidData_graphs.plot_dataframe_with_date(df, 'date', 'new_deaths', title, save_name, ylabel = 'New Deaths for Covid-19')

            # Plot both things

            save_name = self.dir + '/graphs/'+'new_deaths_and_cases_' + country + '_from_' + date1 + '_to_' + date2 + '.png'
            title = 'Daily Deaths and Cases in ' + country + ' between ' + date1 + ' and ' + date2
            covidData_graphs.plot_dataframe_with_date_double(df, 'date', 'new_cases', 'new_deaths', title, save_name, 'New Cases', 'New Deaths')

        return df

    # Given a month returns the cases and deaths each day in that month for each country
    def get_data_a_month_per_country(self, this_month):
        df = self.df_covid_data.dropna(subset = ('location', 'date', 'new_cases', 'new_deaths'))
        df = df.select('location', 'date', 'new_cases', 'new_deaths', month('date').alias('month'))
        return (df.filter(df['month'] == this_month)
                .select('location', 'date', 'new_cases', 'new_deaths'))

    # Same that the previous one but returns data for only a specified country
    def get_data_a_month_a_country(self, this_month, country, plot = False):
        df = self.get_data_a_month_per_country(this_month)
        df = df.filter(df['location'] == country)
        if plot:
            month_str = utils.month_string(this_month)
            # Plot new cases
            save_name = self.dir + '/graphs/'+'new_cases_' + country + '_' + month_str + '.png'
            title = 'Daily Cases in ' + country + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date(df, 'date', 'new_cases', title, save_name, ylabel = 'New Covid-19 Cases')

            # Plot new deaths
            save_name = self.dir + '/graphs/'+'new_deaths_' + country + '_' + month_str + '.png'
            title = 'Daily Deaths in ' + country + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date(df, 'date', 'new_deaths', title, save_name, ylabel = 'New Covid-19 Deaths')

            # Plot both things

            save_name = self.dir + '/graphs/'+'new_deaths_and_cases_' + country + '_' + month_str + '.png'
            title = 'Daily Deaths and Cases in ' + country + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date_double(df, 'date', 'new_cases', 'new_deaths', title, save_name, 'New Cases', 'New Deaths')
        
        return df

    # Given a month returns the total number of cases that month in each country
    def get_data_totals_a_month_per_country(self, this_month):
        return (self.get_data_a_month_per_country(this_month)
                .select('location', 'new_cases', 'new_deaths')
                .groupBy('location').agg({'new_cases': 'sum', 'new_deaths' : 'sum'}))

    # Returns the 'num_countries' countries with more new cases in the month 'this_month'
    def get_countries_with_more_cases_a_month(self, this_month, num_countries, plot = False):
        df = (self.get_data_totals_a_month_per_country(this_month)
                    .select('location', 'sum(new_cases)')
                    .sort(desc('sum(new_cases)')))
        df = (df.filter((df['location'] != 'World') & (df['location'] != 'International'))
                    .limit(num_countries))
        if plot:
            if num_countries > 15:
                print("To many countries to print!")
            else:
                month_str = utils.month_string(this_month)
                save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_more_cases_' + month_str + '.png'
                title = 'Countries with more cases in ' + month_str
                covidData_graphs.plot_bars(df, 'location', 'sum(new_cases)', title, save_name)

        return df

    # Returns the 'num_countries' countries with less new cases in the month 'this_month'    
    def get_countries_with_less_cases_a_month(self, this_month, num_countries):
        df = (self.get_data_totals_a_month_per_country(this_month)
                    .select('location', 'sum(new_cases)')
                    .sort(('sum(new_cases)')))
        df = (df.filter((df['location'] != 'World') & (df['location'] != 'International'))
                    .limit(num_countries))
        if plot:
            if num_countries > 15:
                print("To many countries to plot!")
            else:
                month_str = utils.month_string(this_month)
                save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_less_cases_' + month_str + '.png'
                title = 'Countries with less cases in ' + month_str
                covidData_graphs.plot_bars(df, 'location', 'sum(new_cases)', title, save_name)

        return df

    # Returns the 'num_countries' countries with more new deaths in the month 'this_month'
    def get_countries_with_more_deaths_a_month(self, this_month, num_countries, plot = False):
        df = (self.get_data_totals_a_month_per_country(this_month)
                    .select('location', 'sum(new_deaths)')
                    .sort(desc('sum(new_deaths)')))
        df = (df.filter((df['location'] != 'World') & (df['location'] != 'International'))
                    .limit(num_countries))
        if plot:
            if num_countries > 15:
                print("To many countries to plot!")
            else:
                month_str = utils.month_string(this_month)
                save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_more_deaths_' + month_str + '.png'
                title = 'Countries with more deaths in ' + month_str
                covidData_graphs.plot_bars(df, 'location', 'sum(new_deaths)', title, save_name)

        return df

    # Returns the 'num_countries' countries with less new deaths in the month 'this_month'    
    def get_countries_with_less_deaths_a_month(self, this_month, num_countries):
        df = (self.get_data_totals_a_month_per_country(this_month)
                    .select('location', 'sum(new_deaths)')
                    .sort(('sum(new_deaths)')))
        df = (df.filter((df['location'] != 'World') & (df['location'] != 'International'))
                    .limit(num_countries))
        if plot:
            if num_countries > 15:
                print("To many countries to plot!")
            else:
                month_str = utils.month_string(this_month)
                save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_less_deaths_' + month_str + '.png'
                title = 'Countries with less detahs in ' + month_str
                covidData_graphs.plot_bars(df, 'location', 'sum(new_deaths)', title, save_name)

        return df

    # Returns the avaerage cases per day in a month for each country
    def get_average_data_per_day_a_month_per_country(self, this_month):
        df_aux = self.df_covid_data.select('location', 'new_cases', 'new_deaths', month('date').alias('month'))
        return (df_aux.filter(df_aux['month'] == this_month)
                .groupBy('month', 'location').agg({'new_cases': 'avg', 'new_deaths': 'avg'}))


    # Returns the average cases per day each month for a specified country
    def get_average_data_per_day_per_month_a_country(self, country, plot = False):
        df = self.df_covid_data.dropna(subset = ('location', 'date', 'new_cases', 'new_deaths'))
        df = df.select('location', 'new_cases', 'new_deaths', month('date').alias('month'))
        df = (df.filter(df['location'] == country)
                .groupBy('month', 'location').agg({'new_cases': 'avg', 'new_deaths': 'avg'})
                .sort('month'))

        if plot:

            # Plot new cases average per month
            save_name = self.dir + '/graphs/'+'avg_cases_per_month_'+ country + '.png'
            title = 'Average daily cases per month in ' + country
            covidData_graphs.plot_bars_months(df, 'month', 'avg(new_cases)', title, save_name)

            # Plot new deaths average per month
            save_name = self.dir + '/graphs/'+'avg_deaths_per_month_'+ country + '.png'
            title = 'Average daily deaths per month in ' + country
            covidData_graphs.plot_bars_months(df, 'month', 'avg(new_deaths)', title, save_name)

            # Plot both things

            save_name = self.dir + '/graphs/'+'avg_deaths_and_cases_per_month_'+ country + '.png'
            title = 'Average daily deaths and cases per month in ' + country
            covidData_graphs.plot_bars_months_double(df, 'month', 'avg(new_cases)','avg(new_deaths)', title, save_name, 'Average cases per day', 'Average deaths per day')
        
        return df


    
