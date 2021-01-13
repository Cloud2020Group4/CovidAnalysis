import os
from os.path import dirname, abspath
import urllib.request
import utils
from datetime import datetime

dir = dirname(dirname(abspath(__file__)))

def download_datasets(mode):
    print("Downloading dataset...")
    urllib.request.urlretrieve("https://covid.ourworldindata.org/data/owid-covid-data.csv", dir + "/datasets/owid-covid-data.csv")
    if mode == 'hadoop':
        os.system("hadoop fs -put -f "+  dir + "/datasets/owid-covid-data.csv")


def write_executable(data_type, to_execute, mode, num_threads_local):
    file = open(dir + '/scripts/execute.py','w')
    file.write('import covidData, economicData,populationData, processData, vaccinesData,physiciansData, machineLearning, vaccinesData\n')
    file.write('import shutil\n')
    file.write('from pyspark.sql import SparkSession\n')
    if mode == 'local':
        if num_threads_local == 0:
            str_num_threads_local = '*'
        else:
            str_num_treads_local = str(num_threads_local)
        file.write("spark = SparkSession.builder.appName('CovidAnalysis').master('local[" + str_num_threads_local + "]').getOrCreate()\n")
    elif mode == 'hadoop':
         file.write("spark = SparkSession.builder.appName('CovidAnalysis').getOrCreate()\n")
    if data_type=='covid':
        file.write("data = covidData.CovidData(spark, '" + mode + "')\n")

    elif data_type=='economy':
        file.write("data = processData.ProcessData(spark, '" + mode + "')\n")
        
    elif data_type=='population':
        file.write("data = processData.ProcessData(spark, '" + mode + "')\n")
        
    elif data_type=='health':
        file.write("data = processData.ProcessData(spark, '" + mode + "')\n")

    elif data_type=='vaccines':
        file.write("data = vaccinesData.VaccinesData(spark, '" + mode + "')\n")
    
    elif data_type=='physicians':
        file.write("data = physiciansData.PhysiciansData(spark, '" + mode + "')\n")

    elif data_type=='machineLearning':
        file.write("data = machineLearning.MachineLearning(spark, '" + mode + "')\n")

    file.write('df =' + to_execute + '\n')

    # Show the dataframe
    file.write('df.show()' + '\n')
    
    # Print dataframe to file
    if mode == 'local':
        file.write("shutil.rmtree('" + dir  + "/output', ignore_errors = True, onerror = None)\n")
        file.write("df.coalesce(1).write.format('csv').options(header=True).save('" + dir + "/output')\n")
    elif mode == 'hadoop':
        file.write("os.system('hadoop fs -rm -r output')\n")
        file.write("df.coalesce(1).write.format('csv').options(header=True).save('output')\n")

    file.close() 

def enter_integer(text):
    val = -1
    while True:
        try:
            val = int(input(text))       
        except ValueError:
            print("Input value is not an integer!")
            continue
        else:
            if val >= 0:
                break 
            else:
                print("Input is not a possitive integer!")
    return val

def enter_date():
    while(True):
        date = input("Enter a date in format yyyy-mm-dd: ")
        split_date = date.split("-")
        if len(split_date) != 3:
            print("Please enter date in the correct format!")
        else:
            correct_year = False
            if len(split_date[0]) != 4 :
                print("Please enter year in format yyyy")
            else:
                try:
                    year = int(split_date[0])
                except ValueError:
                    print("Year must be an integer!")
                    continue
                else:
                    if year > 0:
                        correct_year = True 
                    else:
                        print("Year must be a possitive integer!")
            if correct_year:
                correct_month = False
                if len (split_date[1])  != 2 :
                    print("Please enter month in format mm")
                else:
                    try:
                        month= int(split_date[1])
                    except ValueError:
                        print("Month must be an integer!")
                        continue
                    else:
                        if month in [i for i in range(1,13)]:
                            correct_month = True 
                        else:
                            print("Month must be a possitive integer between 1 and 12!")
                    if correct_month:
                        correct_day = False
                        if len(split_date[2]) != 2:
                            print("Please enter day in format dd")
                        else:
                            try:
                                day = int(split_date[2])
                            except ValueError:
                                print("Day must be an integer!")
                                continue
                            else:
                                if day in [i for i in range(1, utils.num_days_a_month(month))]:
                                    correct_day = True
                                    break
                                else:
                                    print("Day must be an integer between 1 and " + str(utils.num_days_a_month(month)))            
    return date

def enter_month():
     while(True):
        this_month = enter_integer("Enter a month number (from 1 to 12): ")
        if this_month in [i for i in range(1, 13)]:
            break
        else:
            print("Month number must be between 1 and 12")

def enter_aggregate_option():    
    while(True):       
        print("////////////////////////////////////////")
        print("What type of aggegated data do you want?")
        print("1.Total sum of data for ecah month's day ")
        print("2.Average daily data for that month")
        print("////////////////////////////////////////")
        avg_option = enter_integer("Enter an option: ")
        if avg_option == 1:
            ret = ''
            break
        elif avg_option == 2:
            ret = ", avg=True"
            break
        else:
            print("Wrong option")
    return ret

def date_limit_options():
    while(True):
        print("/////////////////////////////////////////////////////////")
        print("By default, data is given for all available dates.")
        print("Choose one of the following option")
        print("1.Continue with all available dates for the given country")
        print("2.Enter only a start date")
        print("3.Enter only an end date")
        print("4.Enter a start date and an end date")
        print("/////////////////////////////////////////////////////////")
        dates_option = enter_integer("Enter an option: ")
        if dates_option == 1:
            ret = ''
            break
        elif dates_option == 2:
            date_ini = enter_date()
            ret = ", date_ini='" + date_ini + "'"
            break
        elif dates_option == 3:
            date_fin = enter_date()
            ret = ", date_fin='" + date_fin + "'"
            break
        elif dates_option == 4:
            date_ini = enter_date()
            date_fin = enter_date()
            ret = ", date_ini='" + date_ini + "', date_fin='" + date_fin + "'"
            break
        else:
            print("Wrong answer")
    return ret


def ask_yes_no_option_covid_data(text):
    while(True):
        ans = input(text)
        if ans == 'y':
            return True
        elif ans == 'n':
            return False
        else:
            print("Wrong answer")

def ask_options_covid_data(has_smoothed, has_totals, has_relative, has_plot):
    text = ''
    if has_smoothed:
        smoothed = ask_yes_no_option_covid_data("Do you want smoothed data?[y/n]: ")
        text = text + ', smoothed=' + str(smoothed)
    if has_totals:
        totals = ask_yes_no_option_covid_data("Do you want the cumulative total data ecah day instead of the new data each day?[y/n]: ")
        text = text + ', totals=' + str(totals)
    if has_relative:
        relative = ask_yes_no_option_covid_data("Do you want relative data per million people instead of absolute data?[y/n]: ")
        text = text + ', relative=' + str(relative)
    if has_plot:
        plot = ask_yes_no_option_covid_data("Do you want to plot the results?[y/n]: ")
        text = text + ', plot=' + str(plot)
    return text

def write_executable_covid_data(final_option, mode, num_threads_local):
    if final_option == '1.1':
        date = enter_date()
        options_text = ask_options_covid_data(True, True, True, False)
        func = "data.get_data_a_date_all_countries('" + date + "'" + options_text + ")"
    elif final_option == '1.2': 
        date = enter_date()
        country = input("Enter a country name: ")
        options_text = ask_options_covid_data(True, True, True, False)
        func = "data.get_data_a_date_a_country('" + date + "', '"+ country + "'" + options_text + ")"
    elif final_option == '2.1':
        country = input("Enter a country name: ")
        func = "data.get_data_a_country_a_period_of_time('" + country + "'"
        date_options = date_limit_options()
        options_text = ask_options_covid_data(True, True, True, True)
        func = "data.get_data_a_country_a_period_of_time('" + country + "'" + date_options + options_text + ")"

    elif final_option == '2.2':
        this_month = enter_month()
        options_text = ask_options_covid_data(True, True, True, False)
        func = "data.get_data_a_month_daily_all_countries(" + str(this_month) + options_text + ")"
    
    elif final_option == '2.3':
        this_month = enter_month()
        country = input("Enter a country name: ")
        options_text = ask_options_covid_data(True, True, True, True)
        func = "data.get_data_a_month_daily_a_country(" + str(this_month) + ", '" + country + "'" + options_text + ")"

    elif final_option == '3.1':
        this_month = enter_month()    
        agg_option = enter_aggregate_option()
        options_text = ask_options_covid_data(False, False, True, False)
        func = "data.get_data_a_month_total_all_countries(" + str(this_month) + agg_option + options_text + ")"

    elif final_option == '3.1.1':
        this_month = enter_month()
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_more_cases_a_month(" + str(this_month) + ", " + str(num_countries) + options_text + ")"
    
    elif final_option == '3.1.2':
        this_month = enter_month()
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_less_cases_a_month(" + str(this_month) + ", " + str(num_countries) + options_text + ")"

    elif final_option == '3.1.3':
        this_month = enter_month()
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_more_deaths_a_month(" + str(this_month) + ", " + str(num_countries) + options_text + ")"
    
    elif final_option == '3.1.4':
        this_month = enter_month
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_less_deaths_a_month(" + str(this_month) + ", " + str(num_countries) + options_text + ")"

    elif final_option == '3.2':
        date = enter_date()
        options_text = ask_options_covid_data(False, False, True, False)
        func = "data.get_total_data_until_a_date_all_countries('" + date + "'" + options_text + ")"
    
    elif final_option == '3.2.1':
        date = enter_date()
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_more_cases_until_a_date('" + date + "', " + str(num_countries) + options_text + ")"

    elif final_option == '3.2.2':
        date = enter_date()
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_less_cases_until_a_date('" + date + "', " + str(num_countries) + options_text + ")"
    
    elif final_option == '3.2.3':
        date = enter_date()
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_more_deaths_until_a_date('" + date + "', " + str(num_countries) + options_text + ")"

    elif final_option == '3.2.4':
        date = enter_date()
        num_countries = enter_integer("Enter the number of countries that will appear in the top: ")
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_countries_with_less_deaths_until_a_date('" + date + "', " + str(num_countries) + options_text + ")"

    elif final_option == '4.1':
        country = input("Enter a country name: ")        
        agg_option = enter_aggregate_option()        
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.get_data_aggregate_a_country_all_months('" + country + "'" + agg_option + options_text + ")"

    elif final_option == '5.1':
        this_month = enter_month()
        options_text = ask_options_covid_data(False, False, False, True)
        func = "data.get_total_data_a_month_per_continent(" + str(this_month) + options_text + ")"

    elif final_option == '5.2':
        date = enter_date()
        options_text = ask_options_covid_data(False, False, False, True)
        func = "data.get_total_data_until_a_date_per_continent('" + date + "'" + options_text + ")"

    elif final_option == '6.1':
        country1 = input("Enter a country name: ")
        country2 = input("Enter another country name: ")        
        date_options = date_limit_options()
        options_text = ask_options_covid_data(True, True, True, True)
        func = "data.compare_two_countries_a_period_of_time('" + country1 + "', '" + country2 + "'"+ date_options + options_text + ")"

    elif final_option == '6.2':
        this_month = enter_month()
        country1 = input("Enter a country name: ")
        country2 = input("Enter another country name: ")        
        options_text = ask_options_covid_data(True, True, True, True)
        func = "data.compare_two_countries_a_month_daily(" + str(this_month) + ", '" + country1 + "', '" + country2 + "'" + options_text + ")"

    elif final_option == '6.3':
        country1 = input("Enter a country name: ")
        country2 = input("Enter another country name: ")       
        agg_option = enter_aggregate_option()
        options_text = ask_options_covid_data(False, False, True, True)
        func = "data.compare_two_countries_all_months_aggregated('" + country1 + "', '" + country2 + "'" + agg_option + options_text + ")"
    
    write_executable('covid', func, mode, num_threads_local)

def aux_menu_vaccines(data_type, mode, num_threads_local):
    while(True):
        print("//////////////////////")
        print("What kind of information do you want?")
        print("1.Opinion about vaccines importance given a country")
        print("2.Opinion about vaccines safety given a country")
        print("3.Opinion about vaccines effectiveness given a country")
        print("//////////////////////")
        option=enter_integer("Enter your choice: ")
        if option==1:
            country=input("Enter the name of a country: ")
            write_executable(data_type, "data.get_vaccines_importance_data_per_country('" + country + "')\n", mode, num_threads_local)
            break
        elif option==2:
            country=input("Enter the name of a country: ")
            write_executable(data_type, "data.get_vaccines_safety_data_per_country('" + country + "')\n", mode, num_threads_local)
            break
        elif option==3:
            country=input("Enter the name of a country: ")
            write_executable(data_type, "data.get_vaccines_effectiveness_data_per_country('" + country + "')\n", mode, num_threads_local)
            break
        else:
            print("Wrong Choice")

def aux_menu_physicians(data_type, mode, num_threads_local):
    while(True):
        print("//////////////////////")
        print("What kind of information do you want?")
        print("1.Number of doctors per 1000 people given a country and year")
        print("2.Top with the countries with the highest number of doctors per 1000 people in the world given a year")
        print("3.Top with the countries with the lowest number of doctors per 1000 people in the world given a year")
        print("4.Average number of doctors per 1000 people in the world given a year")
        print("5.Top with the countries with the highest number of doctors per 1000 people given a year and a continent")
        print("6.Top with the countries with the lowest number of doctors per 1000 people given a year and a continent")
        print("7.Average number of doctors per 1000 people given a year and a continent")
        print("//////////////////////")
        option=enter_integer("Enter your choice: ")
        if option==1:
            country=input("Enter the name of a country: ")
            year=enter_integer("Enter a year (from 1960 to 2018): ")
            write_executable(data_type, "data.get_doctors_country_and_year('" + str(year) + "','" + country + "')\n", mode, num_threads_local)
            break
        elif option==2:
            year=enter_integer("Enter a year (from 1960 to 2018): ")
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(data_type, "data.get_top_country(" + str(num_countries) + ",'" + str(year) + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(data_type, "data.get_top_country(" + str(num_countries) + ",'" + str(year) + "')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
            break
        elif option==3:
            year=enter_integer("Enter a year (from 1960 to 2018): ")
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(data_type, "data.get_bottom_country(" + str(num_countries) + ",'" + str(year) + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(data_type, "data.get_bottom_country(" + str(num_countries) + ",'" + str(year) + "')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
            break
        elif option==4:
            year=enter_integer("Enter a year (from 1960 to 2018): ")
            write_executable(data_type, "data.get_avg_year('" + str(year)+ "')\n", mode, num_threads_local)
            break
        elif option==5:
            continent=input("Enter the name of a continent: ")
            year=enter_integer("Enter a year (from 1960 to 2018): ")
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(data_type, "data.get_top_country_continent(" + str(num_countries) +",'" + str(year) +"','" + continent + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(data_type, "data.get_top_country_continent(" + str(num_countries) +",'" + str(year) +"','" + continent +"')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
            break
        elif option==6:
            continent=input("Enter the name of a continent: ")
            year=input("Enter a year (from 1960 to 2018): ")
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(data_type, "data.get_bottom_country_continent(" + str(num_countries) +",'" + str(year) +"','" + continent + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(data_type, "data.get_bottom_country_continent(" + str(num_countries) +",'" + str(year) +"','" + continent +"')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
            break
        elif option==7:
            continent=input("Enter the name of a continent: ")
            year=input("Enter a year (from 1960 to 2018): ")
            write_executable(data_type, "data.get_avg_year_continent('"+ str(year) + "','" + continent + "')\n", mode, num_threads_local)
            break
        else:
            print("Wrong Choice")

def aux_menu(indicator, dataType, mode, num_threads_local):
    while(True):
        print("//////////////////////")
        print("What kind of information do you want?")
        print("1.Given a country see the indicator's value")
        print("2.See the indicator's value for all countries")
        print("3.See the top with the countries with the highest value for that indicator")
        print("4.See the top with the countries with the lowest value for that indicator")
        print("5.See the average, minimum and maximum value for each continent")
        print("6.See the top with the countries with the highest value for that indicator per continent")
        print("7.See the top with the countries with the lowest value for that indicator per continent")
        print("//////////////////////")
        option=enter_integer("Enter your choice: ")
        if option==1:
            country=input("Enter the name of a country: ")
            write_executable(dataType, "data.get_indicator_per_country('" + country + "','" + indicator + "')\n", mode, num_threads_local)
            break
        elif option==2:
            write_executable(dataType, "data.get_indicator_all_countries('" + indicator + "')\n", mode, num_threads_local)
            break
        elif option==3:
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator(" + str(num_countries) + ",'" + indicator + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator(" + str(num_countries) + ",'" + indicator + "')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
        elif option==4:
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator(" + str(num_countries) + ",'" + indicator + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator(" + str(num_countries) + ",'" + indicator + "')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
        elif option==5:
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_indicator_by_continent('" +  indicator + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_indicator_by_continent('" + indicator + "')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
        elif option==6:
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator_per_continent(" + str(num_countries) + ",'" + indicator + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator_per_continent(" + str(num_countries) + ",'" + indicator + "')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
        elif option==7:
            num_countries=enter_integer("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator_per_continent(" + str(num_countries) + ",'" + indicator + "',plot=True)\n", mode, num_threads_local)
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator_per_continent(" + str(num_countries) + ",'" + indicator + "')\n", mode, num_threads_local)
                    break
                else:
                    print("Wrong answer")
            break
        else:
            print("Wrong Choice")

def execute_spark_submit(mode, num_executors, executor_cores):
    if mode == 'local' :
        os.system("spark-submit " + dir + "/scripts/execute.py")
    elif mode == 'hadoop':
        command = "spark-submit"
        if num_executors > 0:
            command = command + " --num-executors " + str(num_executors)
            print('Runing spark with ' + str(num_executors) + ' working nodes')
        if executor_cores > 0:
            command = command + " --executor-cores " + str(executor_cores)
            print('Runing spark with ' + str(executor_cores) + ' cores per node')
        command = command + " " + dir + "/scripts/execute.py"
        os.system(command)

    
def main():
    num_executors = -1
    executor_cores = -1
    num_threads_local = -1
    while(True):
        print("**********************")
        print("Where are you executing the application?")
        print("1.Hadoop Cluster")
        print("2.Local mode")
        print("**********************")
        mode_op = enter_integer("Enter an option: ")
        if mode_op == 1:
            mode = 'hadoop'
            print("Configure the system parallelism when running spark-submit")
            num_executors = enter_integer("Select the value for the flag --num-executors (0 if you don't want to set it): ")
            executor_cores = enter_integer("Select the value for the flag --executor-cores (0 if you don't want to set it): ")
            
            # Load datasets to HDFS
            if ask_yes_no_option_covid_data("Do you want to upload the datasets to Hadoop File System? (you must do it the first time you run the application)[y/n]: "):
                print("Updating datasets to Hadoop File System...")
                os.system("hadoop fs -put -f "+  dir + "/datasets/owid-covid-data.csv")
                os.system("hadoop fs -put -f "+  dir + "/datasets/vaccine.csv")
                os.system("hadoop fs -put -f "+  dir + "/datasets/medical_doctors_per_1000_people.csv")
                os.system("hadoop fs -put -f "+  dir + "/datasets/countries.csv")
                print("Updating ended...")
            break
        elif mode_op == 2:
            mode = 'local'
            print("Select desired level of system parallelism, that is, the number of threads that you want to create")
            print("Enter 0 if you want to run spark with as many worker threads as logical cores on your machine")
            num_threads_local = enter_integer("Enter a number: ")
            break
        else:
            print('Wrong answer')
        
    while True:
        print("**********************")
        print("Menu")
        print("1.DOWNLOAD THE NEWEST DATASETS- Please execute this option in the first place before doing anything else")
        print("2.Covid-19 data")
        print("3.Economic data")
        print("4.Populational data")    
        print("5.Health data")
        print("6.Machine Learning")
        print("7.Exit")
        print("**********************")
        choice=enter_integer("Enter your choice: ")
        if choice== 1:
            download_datasets(mode)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            print("Now you are working with data downloaded on date: " + dt_string)
        elif choice==2:
            while(True):
                print("----------------------")
                print("Covid-19 data menu")
                print("1.Data in a specific date")
                print("2.Data during each day in a period of time")
                print("3.Total data during a period of time")
                print("4.Aggregated data per month")
                print("5.Data per continent")
                print("6.Compare data for two countries")            
                print("----------------------")
                option1 = enter_integer("Enter an option: ")
                if option1 == 1 :
                    while(True):
                        specific_country1 = input("Do you want the data for a specific country?[y/n]: ")
                        if specific_country1 == 'y' :
                            final_option = '1.2'
                            break
                        elif specific_country1 == 'n':
                            final_option = '1.1'
                            break
                        else:
                             print('Wrong Answer')
                    break
                elif option1 == 2:
                    while(True):
                        print("//////////////////////")
                        print("What data do you want?")
                        print("1.For only a country during any period of time")
                        print("2.For all countries during each day in a month")
                        print("3.For only a country during each day in a month")
                        print("//////////////////////")
                        suboption2 = enter_integer("Enter an option: ")
                        if(suboption2 in [1,2,3]):
                            final_option = '2.' + str(suboption2)
                            break
                        else:
                            print('Wrong Answer')
                    break
                elif option1 == 3:
                    while(True):
                        print("//////////////////////")
                        print("What data do you want?")
                        print("1.Aggregated total data during a month")
                        print("2.Total data until a specific date")
                        print("//////////////////////")
                        suboption3 = enter_integer("Enter an option: ")
                        if(suboption3 in [1,2]):
                            final_option = '3.' + str(suboption3)
                            break
                        else: 
                            print("Wrong answer")
                    while(True):
                        tops3 = input('Do you want to make a ranking with the countries with the best or the worst data?[y/n]: ')
                        if tops3 == 'y' :
                            while(True):
                                print("//////////////////////")
                                print("Select the top that you prefer")
                                print("1.Countries with more Covid-19 cases")
                                print("2.Countries with less Covid-19 cases")
                                print("3.Countries with more Covid-19 deaths")
                                print("4.Countries with less Covid-19 deaths")
                                print("//////////////////////")
                                tops3_option = enter_integer("Enter an option: ")
                                if tops3_option in [1,2,3,4]:
                                    final_option = final_option + '.' + str(tops3_option)
                                    break
                                else:
                                    print("Wrong answer")
                            break
                        elif tops3 == 'n':
                            break
                        else:
                            print("Wrong answer")
                    break
                elif option1 == 4:
                    final_option = '4.1'
                    break
                elif option1 == 5:
                    while(True):
                        print("///////////////////////////")
                        print("What data do you want?")
                        print("1.Total data during a month")
                        print("2.Total data until a date")
                        print("///////////////////////////")
                        suboption5 = enter_integer("Enter an option: ")
                        if suboption5 in [1,2]:
                            final_option = '5.' + str(suboption5)
                            break
                        else:
                            print("Wrong answer")
                    break
                elif option1 == 6:
                    while(True):
                        print("/////////////////////////////////////////////////////")
                        print("What data do you want to compare?")
                        print("1.Compare data during any period of time for each day")
                        print("2.Compare data during ecah day of a month")
                        print("3.Compare aggregated data for each month")
                        print("/////////////////////////////////////////////////////")
                        suboption6 = enter_integer("Enter an option: ")
                        if suboption6 in [1,2,3]:
                            final_option = '6.' + str(suboption6)
                            break
                        else:
                            print("Wrong answer")
                    break
            write_executable_covid_data(final_option, mode, num_threads_local)

            execute_spark_submit(mode, num_executors, executor_cores)
        
        elif choice==3:
            while(True):
                print("----------------------")
                print("Economic data menu")
                print("1.GDP per capita")
                print("2.Extreme poverty rate")
                print("3.Human development index")
                print("----------------------")
                option2=enter_integer("Enter the indicator: ")
                if option2==1:
                    aux_menu('gdp_per_capita', 'economy', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option2==2:
                    aux_menu('extreme_poverty', 'economy', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option2==3:
                    aux_menu('human_development_index', 'economy', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                else:
                    print("Wrong Choice")
            

        elif choice==4:
            while(True):
                print("----------------------")
                print("Population data menu")
                print("1.Population")
                print("2.Population density")
                print("3.Median age")
                print("4.Population older than 65")
                print("5.Population older than 70")
                print("6.Life expectancy")
                print("----------------------")
                option3=enter_integer("Enter the indicator: ")
                if option3==1:
                    aux_menu('population', 'population', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option3==2:
                    aux_menu('population_density', 'population', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option3==3:
                    aux_menu('median_age', 'population', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option3==4:
                    aux_menu('aged_65_older', 'population', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option3==5:
                    aux_menu('aged_70_older', 'population', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option3==6:
                    aux_menu('life_expectancy', 'population', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                else:
                    print("Wrong Choice")

        elif choice==5:
            while(True):
                print("----------------------")
                print("Health data menu")
                print("1.Cardiovacular death rate")
                print("2.Diabetes prevalence")
                print("3.Female smokers")
                print("4.Male smokers")
                print("5.Handwashing facilities")
                print("6.Hospital beds per thousand")
                print("7.Opion about Vaccines")
                print("8.Physicians")
                print("----------------------")
                option4=enter_integer("Enter the indicator: ")
                if option4==1:
                    aux_menu('cardiovasc_death_rate', 'health', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option4==2:
                    aux_menu('diabetes_prevalence', 'health', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option4==3:
                    aux_menu('female_smokers', 'health', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option4==4:
                    aux_menu('male_smokers', 'health', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option4==5:
                    aux_menu('handwashing_facilities', 'health', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option4==6:
                    aux_menu('hospital_beds_per_thousand', 'health', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option4==7:
                    aux_menu_vaccines('vaccines', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option4==8:
                    aux_menu_physicians('physicians', mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                else:
                    print("Wrong Choice")
        elif choice==6:
            while(True):
                print("----------------------")
                print("Machine Learning Menu")
                print("1. Total deaths per million, Total cases per million, GDP per capita, Hospital bed per thousand")
                print("2. Total deaths per million, Total cases per million, Vaccine importance, Vaccine safety")
                print("----------------------")
                option6=enter_integer("Choose the indicators you want to use when clustering: ")
                if option6==1:
                    write_executable('machineLearning', "data.ml_covid_data(['total_deaths_per_million', 'total_cases_per_million', 'gdp_per_capita', 'hospital_beds_per_thousand'])", mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break
                elif option6 == 2:
                    write_executable('machineLearning', "data.ml_vaccines_data(['total_deaths_per_million', 'total_cases_per_million'], ['Vaccine importance, Strongly agree (%)', 'Vaccine safety,Strongly agree (%)'])", mode, num_threads_local)
                    execute_spark_submit(mode, num_executors, executor_cores)
                    break

                else: 
                    print("Wrong Choice")
        elif choice==7:
            break
        else:
            print("Wrong Choice")

if __name__ == "__main__":
    main()
