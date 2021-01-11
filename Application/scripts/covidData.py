'''
Module for general Covid Data Processing

Atributes:
   - spark: current SparkSession
   - df_covid_data: dataframe with data from the file 'owid-covid-data.csv'
   - dir: parent of the directory where the script is saved 

Methods:
    Various methods that select some specific data and return them as a dataframe.
    Each method has a comment that explains its purpose.

    Functions summary:

    1. Data in a specific date
        1.1. For all countries: get_data_a_date_all_countries
        1.2. For only a country: get_data_a_date_a_country
    2. Data during each day in a period of time
        2.1. For only a country during any period of time: get_data_a_country_a_period_of_time [INCLUDES PLOT: Line]
        2.2. For all countries during each day in a month: get_data_a_month_daily_all_countries
        2.3. For only a country during each day in a month: get_data_a_month_daily_a_country [INCLUDES PLOT: Line]
    3. Total data during a period of time
        3.1. Aggregated total data during a month for all countries: get_data_a_month_total_all_countries
            3.1.1. Top countries with more cases: get_countries_with_more_cases_a_month    [INCLUDES PLOT: Bars]
            3.1.2. Top countries with less cases: get_countries_with_less_cases_a_month    [INCLUDES PLOT: Bars]
            3.1.3. Top countries with more deaths: get_countries_with_more_deaths_a_month  [INCLUDES PLOT: Bars]
            3.1.4. Top countries with less deaths: get_countries_with_less_deaths_a_month  [INCLUDES PLOT: Bars]
        3.2. Total data until a specific date
            3.2.1. Top countries with more cases until a date: get_countries_with_more_cases_until_a_date   [INCLUDES PLOT: Bars]
            3.2.2. Top countries with less cases until a date: get_countries_with_less_cases_until_a_date   [INCLUDES PLOT: Bars]
            3.2.3. Top countries with more deaths until a date: get_countries_with_more_deaths_until_a_date [INCLUDES PLOT: Bars]
            3.2.4. Top countries with less deaths until a date: get_countries_with_less_deaths_until_a_date [INCLUDES PLOT: Bars]
    4. Aggregated data per month:
        4.1. For a country: get_data_aggregate_a_country_all_months [INCLUDES PLOT: Bars]
    5. Data per continent:
        5.1. Total data during a month: get_total_data_a_month_per_continent    [INCLUDES PLOT: Pie]
        5.2. Total data until a date: get_total_data_until_a_date_per_continent [INCLUDES PLOT: Pie]
    6. Compare data for two countries
        6.1. Compare data during any period of time for each day [INCLUDES PLOT: Line]
        6.2. Compare data during ecah day of a month [INCLUDES PLOT: Line]
        6.3. Compare aggregated data for each month [INCLUDES PLOT: Bars]

    AUXILIARY FUNCTIONS
        1. Get top countries: get_top_countries
        2. Combine two dataframes into one
'''
from pyspark.sql.functions import month, desc, asc
import pyspark.sql.functions as f
import covidData_graphs
import utils

# For being able to use external python libraries, set [export PYSPARK_PYTHON='/usr/bin/python']
# Also you have to install the library before (pip install <library>)
from os.path import dirname, abspath

class CovidData:
    # [Constructor] the function receives a SparkSession and initializes the dataframe needed
    def __init__(self, sparkSes, mode):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        if mode == 'local':
            self.data_dir = self.dir + "/datasets/owid-covid-data.csv"
        elif mode == 'hadoop':
            self.data_dir = "owid-covid-data.csv"
            
        self.df_covid_data = self.spark.read.csv(self.data_dir, header = True, inferSchema=True)

    # [1.1] Given a date returns the data in that day for all countries (including World)

    def get_data_a_date_all_countries(self, date, smoothed = False, totals = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(smoothed, totals, relative)
        df = self.df_covid_data.dropna(subset =('location', 'date', cases, deaths))
        return (df.filter(df['date'] == date).select('location', 'date', cases, deaths))

    # [1.2] Given a date and a country returns the data in that day for that country (country can be World)

    def get_data_a_date_a_country(self, date, country, smoothed = False, totals = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(smoothed, totals, relative)
        df = self.get_data_a_date_all_countries(date, smoothed, totals, relative)
        return df.filter(df['location'] == country)

    
    # [2.1] Given a country returns the data for all dates included in the dataset, with the option of limiting the period of time considered 
    # by providing a loewr-bound date, an upper bound one or bith of them
    def get_data_a_country_a_period_of_time(self, country, date_ini = None, date_fin = None, plot = False, smoothed = False, totals = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(smoothed, totals, relative)
        df = self.df_covid_data.dropna(subset =('location', 'date', cases, deaths))
        df = df.filter(df['location'] == country).select('location', 'date', cases, deaths)
        end_text_file = ''
        end_text = ''
        if (date_ini != None):
            df = df.filter(df['date'] >= date_ini)
            end_text_file = end_text_file + '_from_' + date_ini
            end_text = end_text + ' from ' + date_ini
        if(date_fin != None):
            df = df.filter((df['date'] <= date_fin))
            end_text_file = end_text_file + '_to_' + date_fin
            end_text = end_text + ' to ' + date_fin
            
        if plot:
            # Plot new cases
            save_name = self.dir +'/graphs/'+ cases + '_' + country + '_all_dates' + end_text_file + '.png'
            title = 'Daily ' + cases_text + ' in ' + country + end_text
            covidData_graphs.plot_dataframe_with_date(df, 'date', cases, title, save_name, ylabel = cases_text)

            # Plot new deaths
            save_name = self.dir + '/graphs/' + deaths + '_' + country + '_all_dates' + end_text_file + '.png'
            title = 'Daily ' + deaths_text + ' in ' + country + end_text
            covidData_graphs.plot_dataframe_with_date(df, 'date', deaths, title, save_name, ylabel = deaths_text)

            # Plot both things

            save_name = self.dir + '/graphs/'+ cases + '_and_' + deaths+ '_in_' + country + '_all_dates' + end_text_file + '.png'
            title = 'Daily '+ cases_text+ ' and ' + deaths_text + ' in ' + country + end_text
            covidData_graphs.plot_dataframe_with_date_double(df, 'date', cases, deaths, title, save_name, cases_text, deaths_text)

        return df

    # [2.2] Given a month returns the cases and deaths each day in that month for each country
    def get_data_a_month_daily_all_countries(self, this_month, smoothed = False, totals = False, relative = False):
        cases, deaths, _ , _ = utils.get_correct_columns(smoothed, totals, relative)
        df = self.df_covid_data.dropna(subset = ('location', 'date', cases, deaths))
        df = df.select('location', 'date', cases, deaths, month('date').alias('month'))
        return (df.filter(df['month'] == this_month).select('location', 'date', cases, deaths))

    # [2.3] Same that the previous one but returns data for only a specified country
    def get_data_a_month_daily_a_country(self, this_month, country, plot = False, smoothed = False, totals = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(smoothed, totals, relative)
        df = self.get_data_a_month_daily_all_countries(this_month, smoothed = smoothed, totals = totals, relative = relative)
        df = df.filter(df['location'] == country)
        if plot:
            month_str = utils.month_string(this_month)
            # Plot new cases
            save_name = self.dir + '/graphs/'+ cases + '_' + country + '_' + month_str + '.png'
            title = 'Daily ' + cases_text + ' in ' + country + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date(df, 'date', cases, title, save_name, ylabel = cases_text)

            # Plot new deaths
            save_name = self.dir + '/graphs/' + deaths +'_' + country + '_' + month_str + '.png'
            title = 'Daily ' + deaths_text + ' in '  + country + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date(df, 'date', deaths, title, save_name, ylabel = deaths_text)

            # Plot both things
            save_name = self.dir + '/graphs/'+ cases + '_and_' + deaths + '_' + country + '_' + month_str + '.png'
            title = 'Daily ' + cases_text + ' and ' + deaths_text + ' in ' + country + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date_double(df, 'date', cases, deaths, title, save_name, cases_text, deaths_text)
        
        return df

    # [3.1] Given a month returns the total number of cases that month in each country
    def get_data_a_month_total_all_countries(self, this_month, avg = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, False, relative)
        aggregate, _, _ = utils.get_correct_names_aggregate(avg)
        return (self.get_data_a_month_daily_all_countries(this_month, relative=relative)
                .select('location', cases, deaths)
                .groupBy('location').agg({cases: aggregate, deaths : aggregate}))


    # [3.1.1] Returns the 'num_countries' countries with more new cases in the month 'this_month'
    def get_countries_with_more_cases_a_month(self, this_month, num_countries, plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, False, relative)
        sum_cases = 'sum(' + cases + ')'
        df = self.get_data_a_month_total_all_countries(this_month, relative = relative)
        month_str = utils.month_string(this_month)
        return self.get_top_countries(df, sum_cases, cases, cases_text, num_countries, desc, plot, month_str, 'in ' + month_str)

    # [3.1.2] Returns the 'num_countries' countries with less new cases in the month 'this_month'    
    def get_countries_with_less_cases_a_month(self, this_month, num_countries, plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, False, relative)
        sum_cases = 'sum(' + cases + ')'
        df = self.get_data_a_month_total_all_countries(this_month, relative = relative)
        month_str = utils.month_string(this_month)
        return self.get_top_countries(df, sum_cases, cases, cases_text, num_countries, asc, plot, month_str, 'in ' + month_str)

    # [3.1.3] Returns the 'num_countries' countries with more new deaths in the month 'this_month'
    def get_countries_with_more_deaths_a_month(self, this_month, num_countries, plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, False, relative)
        sum_deaths = 'sum(' + deaths + ')'
        df = self.get_data_a_month_total_all_countries(this_month, relative = relative)
        month_str = utils.month_string(this_month)
        return self.get_top_countries(df, sum_deaths, deaths, deaths_text, num_countries, desc, plot, month_str, 'in ' + month_str)

    # [3.1.4] Returns the 'num_countries' countries with less new deaths in the month 'this_month'    
    def get_countries_with_less_deaths_a_month(self, this_month, num_countries, plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, False, relative)
        sum_deaths = 'sum(' + deaths + ')'
        df = self.get_data_a_month_total_all_countries(this_month, relative = relative)
        month_str = utils.month_string(this_month)
        return self.get_top_countries(df, sum_deaths, deaths, deaths_text, num_countries, asc, plot, month_str, 'in ' + month_str)
    
    # [3.2] Returns the total cumulative data until a date for all countries
    def get_total_data_until_a_date_all_countries(self, date, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, True, relative)
        df = self.get_data_a_date_all_countries(date, totals = True, relative = relative)
        return df

    # [3.2.1] Returns the top 'num_countries' with more cumulative cases until a given date

    def get_countries_with_more_cases_until_a_date(self, date, num_countries,plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, True, relative)
        df = self.get_data_a_date_all_countries(date, totals = True, relative = relative)
        return self.get_top_countries(df, cases, cases, cases_text, num_countries, desc, plot, 'until_'+ date, 'until ' + date)

    # [3.2.2] Returns the top 'num_countries' with less cumulative cases until a given date

    def get_countries_with_less_cases_until_a_date(self, date, num_countries,plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, True, relative)
        df = self.get_data_a_date_all_countries(date, totals = True, relative = relative)
        return self.get_top_countries(df, cases, cases, cases_text, num_countries, asc, plot, 'until_'+ date, 'until ' + date)

    # [3.2.3] Returns the top 'num_countries' with more cumulative deaths until a given date

    def get_countries_with_more_deaths_until_a_date(self, date, num_countries,plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, True, relative)
        df = self.get_data_a_date_all_countries(date, totals = True, relative = relative)
        return self.get_top_countries(df, deaths, deaths, deaths_text, num_countries, desc, plot, 'until_'+ date, 'until ' + date)

    # [3.2.4] Returns the top 'num_countries' with less cumulative deaths until a given date

    def get_countries_with_less_deaths_until_a_date(self, date, num_countries,plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, True, relative)
        df = self.get_data_a_date_all_countries(date, totals = True, relative = relative)
        return self.get_top_countries(df, deaths, deaths, deaths_text, num_countries, asc, plot, 'until_'+ date, 'until ' + date)


    # [4.1] Returns the average cases per day or total cases each month for a specified country
    def get_data_aggregate_a_country_all_months(self, country, avg = False, plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, False, relative)
        aggregate, file_text, text = utils.get_correct_names_aggregate(avg)
        df = self.df_covid_data.dropna(subset = ('location', 'date', cases, deaths))
        
        df = (df.filter(df['location'] == country)
                .select('location', cases, deaths, month('date').alias('month'))
                .groupBy('location', 'month').agg({cases: aggregate, deaths: aggregate})
                .sort('month'))

        if plot:
            agg_cases = aggregate + '(' + cases + ')'
            agg_deaths = aggregate + '(' + deaths + ')'

            # Plot new cases average per month
            save_name = self.dir + '/graphs/'+ file_text + '_' + cases + '_per_month_'+ country + '.png'
            title = text + ' ' + cases_text + ' per month in ' + country
            covidData_graphs.plot_bars_months(df, 'month', agg_cases, title, save_name)

            # Plot new deaths average per month
            save_name = self.dir + '/graphs/'+ file_text + '_' + deaths + '_per_month_'+ country + '.png'
            title = text + ' '+ deaths_text + ' per month in ' + country
            covidData_graphs.plot_bars_months(df, 'month', agg_deaths, title, save_name)

            # Plot both things
            save_name = self.dir + '/graphs/'+ file_text + '_' + deaths + '_and_' + cases + '_per_month_'+ country + '.png'
            title = text + ' ' + cases_text + ' and ' + deaths_text + ' per month in ' + country
            covidData_graphs.plot_bars_months_double(df, 'month', agg_cases,agg_deaths, title, save_name, text + ' ' + cases_text , text + ' ' + deaths_text )
        
        return df

    # [5.1] Returns the total data a month per continent

    def get_total_data_a_month_per_continent(self, this_month, plot = False):
        df = self.df_covid_data.dropna(subset=('location', 'continent', 'date', 'new_cases', 'new_deaths'))
        df = df.select('continent','new_cases', 'new_deaths', month('date').alias('month'))
        df = (df.filter(df['month'] == this_month)
                .groupBy('continent').agg({'new_cases': 'sum', 'new_deaths': 'sum'}))

        if plot:

            month_str = utils.month_string(this_month)

            # Plot distribution of new cases in that month

            save_name = self.dir + '/graphs/'+'new_cases_per_continent_'+ month_str + '.png'
            title = 'Distribution of new Cases in ' + month_str
            covidData_graphs.plot_pie(df, 'continent', 'sum(new_cases)', title, save_name)

            # Plot distribution of new deaths in that month

            save_name = self.dir + '/graphs/'+'new_deaths_per_continent_'+ month_str + '.png'
            title = 'Distribution of new deaths in ' + month_str
            covidData_graphs.plot_pie(df, 'continent', 'sum(new_deaths)', title, save_name)

        return df



     # [5.2] Returns the total data until a date per continent

    def get_total_data_until_a_date_per_continent(self, date, plot = False):
        df = self.df_covid_data.dropna(subset=('location','continent', 'date', 'total_cases', 'total_deaths'))
        df = (df.filter(df['date'] == date)
                .select('continent', 'total_deaths', 'total_cases')
                .groupBy('continent').agg({'total_deaths': 'sum', 'total_cases': 'sum'}))

        if plot:
            # Plot distribution of new cases in until that date

            save_name = self.dir + '/graphs/'+'total_cases_per_continent_until_'+ date + '.png'
            title = 'Distribution of total cases until ' + date
            covidData_graphs.plot_pie(df, 'continent', 'sum(total_cases)', title, save_name)

            # Plot distribution of new deaths in that month

            save_name = self.dir + '/graphs/'+'total_deaths_per_continent_until'+ date + '.png'
            title = 'Distribution of total deaths until ' + date
            covidData_graphs.plot_pie(df, 'continent', 'sum(total_deaths)', title, save_name)

        return df

    def compare_two_countries_a_period_of_time(self, country1, country2, date_ini = None, date_fin = None, plot = False, smoothed = False, totals = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(smoothed, totals, relative)
        df1 = self.get_data_a_country_a_period_of_time(country1, date_ini=date_ini, date_fin = date_fin, smoothed = smoothed, totals = totals, relative = relative)
        df2 = self.get_data_a_country_a_period_of_time(country2, date_ini=date_ini, date_fin = date_fin, smoothed = smoothed, totals = totals, relative = relative)
        df, date_ini, date_fin = self.combine_dataframes(df1, df2, 'date', cases, deaths)
        if plot:
            date_ini_str = date_ini.strftime("%Y-%m-%d")
            date_fin_str = date_fin.strftime("%Y-%m-%d")
            # Plot cases
            save_name = self.dir + '/graphs/'+'compare_'+ cases + '_' + country1 + '_vs_' + country2 + '_from_' + date_ini_str + '_to_' + date_fin_str + '.png'
            title = cases_text+ ' ' + country1 + ' vs ' + country2 + ' from ' + date_ini_str + ' to ' + date_fin_str
            covidData_graphs.plot_dataframe_with_date_double(df, 'date_1', cases + '_1', cases + '_2', title, save_name, cases_text + ' in ' + country1, cases_text + ' in ' + country2)

            # Plot deaths
            save_name = self.dir + '/graphs/'+'compare_'+ deaths + '_' + country1 + '_vs_' + country2 + '_from_' + date_ini_str + '_to_' + date_fin_str + '.png'
            title = deaths_text + ' ' + country1 + ' vs ' + country2 + ' from ' + date_ini_str + ' to ' + date_fin_str
            covidData_graphs.plot_dataframe_with_date_double(df, 'date_1', deaths + '_1', deaths + '_2', title, save_name, deaths_text + ' in ' + country1, deaths_text + ' in ' + country2)
        return df

    def compare_two_countries_a_month_daily(self, this_month, country1, country2, plot = False, smoothed = False, totals = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(smoothed, totals, relative)
        df1 = self.get_data_a_month_daily_a_country(this_month, country1,smoothed = smoothed, totals = totals, relative =relative)
        df2 = self.get_data_a_month_daily_a_country(this_month, country2,smoothed = smoothed, totals = totals, relative =relative)
        df, _, _ = self.combine_dataframes(df1, df2, 'date', cases, deaths)
        if plot:
            month_str = utils.month_string(this_month)
            # Plot cases
            save_name = self.dir + '/graphs/' + 'compare_'+ cases + '_' + country1 + '_vs_' + country2 + '_' + month_str + '.png'
            title = cases_text + ' ' + country1 + ' vs ' + country2 + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date_double(df, 'date_1', cases + '_1', cases + '_2', title, save_name, cases_text + ' in ' + country1, cases_text + ' in ' + country2)

            # Plot deaths
            save_name = self.dir + '/graphs/'+'compare_'+ deaths + '_' + country1 + '_vs_' + country2 + '_' + month_str + '.png'
            title = deaths_text + ' ' + country1 + ' vs ' + country2 + ' in ' + month_str
            covidData_graphs.plot_dataframe_with_date_double(df, 'date_1', deaths + '_1', deaths + '_2', title, save_name, deaths_text + ' in ' + country1, deaths_text + ' in ' + country2)
        return df

    def compare_two_countries_all_months_aggregated(self, country1, country2, avg = False, plot = False, relative = False):
        cases, deaths, cases_text, deaths_text = utils.get_correct_columns(False, False, relative)
        aggregate, file_text, text = utils.get_correct_names_aggregate(avg)
        agg_cases = aggregate + '(' + cases + ')'
        agg_deaths = aggregate + '(' + deaths + ')'
        df1 = self.get_data_aggregate_a_country_all_months(country1, avg = avg, relative = relative)
        df2 = self.get_data_aggregate_a_country_all_months(country2, avg = avg, relative = relative)
        df, _, _ = self.combine_dataframes(df1, df2, 'month', agg_cases, agg_deaths)

        if plot:
            # Plot Cases
            save_name = self.dir + '/graphs/'+ 'compare_' + file_text + '_' + cases + '_per_month_'+ country1 + '_vs_' + country2 + '.png'
            title = text + ' ' + cases_text + ' ' + country1 + ' vs ' + country2
            covidData_graphs.plot_bars_months_double(df, 'month_1', agg_cases + '_1',agg_cases + '_2', title, save_name, text + ' ' + cases_text + ' in ' + country1, text + ' ' + cases_text + ' in ' + country2 )

            # Plot deaths
            # Plot Cases
            save_name = self.dir + '/graphs/'+ 'compare_' + file_text + '_' + deaths + '_per_month_'+ country1 + '_vs_' + country2 + '.png'
            title = text + ' ' + deaths_text + ' ' + country1 + ' vs ' + country2
            covidData_graphs.plot_bars_months_double(df, 'month_1', agg_deaths + '_1',agg_deaths + '_2', title, save_name, text + ' ' + deaths_text + ' in ' + country1, text + ' ' + deaths_text + ' in ' + country2 )
        return df


        
    # [AUX 1]
    def get_top_countries(self, df, indicator, indicator_file, indicator_text, num_countries ,ordering, plot, file_end_text, end_text):
        if ordering == asc:
            word= 'less'
        else:
            word = 'more'
        df = (df.select('location', indicator)
                .sort(ordering(indicator))
                .filter((df['location'] != 'World') & (df['location'] != 'International'))
                .limit(num_countries))
        if plot:
            if num_countries > 15:
                print("To many countries to plot!")

            else:
                save_name = self.dir + '/graphs/'+'top_'+ str(num_countries) + '_' + word + '_' + indicator_file + '_' + file_end_text + '.png'
                title = 'Countries with ' + word + ' ' + indicator_text + ' ' + end_text
                covidData_graphs.plot_bars(df, 'location', indicator, title, save_name)
        return df

    # [AUX 2]
    def combine_dataframes(self, df1, df2, time, cases, deaths):
        time_ini = max(df1.select(time).agg(f.min(time)).collect()[0][0], df2.select(time).agg(f.min(time)).collect()[0][0])
        time_fin = min(df1.select(time).agg(f.max(time)).collect()[0][0], df2.select(time).agg(f.max(time)).collect()[0][0])
        df1 = df1.filter((df1[time] <= time_fin) & (df1[time] >= time_ini))
        df2 = df2.filter((df2[time] <= time_fin) & (df2[time] >= time_ini))
        df1 = (df1.withColumnRenamed('location', 'location_1')
            .withColumnRenamed(time, time + '_1')
            .withColumnRenamed(cases, cases + '_1')
            .withColumnRenamed(deaths, deaths + '_1'))
        df2 = (df2.withColumnRenamed('location', 'location_2')
            .withColumnRenamed(time, time + '_2')
            .withColumnRenamed(cases, cases + '_2')
            .withColumnRenamed(deaths, deaths + '_2'))
        df = df1.join(df2, df1[time + '_1'] == df2[time + '_2'])
        return df, time_ini, time_fin


            






    
