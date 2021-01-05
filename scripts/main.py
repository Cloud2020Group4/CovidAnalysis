# Example of main. 
# It only tries some implemented functions in order to see if they work properly

#import economicData, populationData, processData
#import covidData, covidData_graphs, os, sys
#from pyspark.sql import SparkSession
import os
from os.path import dirname, abspath
#spark = SparkSession.builder.appName('CovidAnalysis').master('local').getOrCreate()
#data = covidData.CovidData(spark)
#spark = SparkSession.builder.appName('CovidAnalysis').master('local').getOrCreate()
#data = covidData.CovidData(spark)
'''
data.get_cases_per_country_and_date('2020-02-24', 'Afghanistan').show()
'''
#data.get_cases_a_country_and_period_of_time('2020-02-24', '2020-05-25','Spain', plot = True).show()
'''
data.get_new_world_cases_a_day('2020-12-28').show()
data.get_new_cases_a_month_per_country('07').show()
data.get_new_cases_a_month_a_country(7, 'Spain').show()
data.get_total_cases_a_month_per_country(7).show(300)
data.get_countries_with_more_cases_a_month(7, 10).show()
data.get_countries_with_less_cases_a_month(7, 10).show()
data.get_average_cases_per_day_a_month(7).show()
data.get_average_cases_per_day_per_month_a_country('Spain').show()
'''
#covidData_graphs.plot_cases_a_country_a_mont(spark)
#print(os.path.dirname(os.path.realpath(sys.argv[0])))

#data.get_new_world_cases_a_day('2020-07-07').show()
#data.get_new_world_cases_a_day_1('2020-07-07').show()

#data.get_countries_with_more_cases_a_month(7, 15, plot = True).show()
#data.get_countries_with_more_deaths_a_month(7, 15, plot = True).show()
#data.get_data_a_month_a_country(7, 'Spain', plot = True).show()

#data.get_average_data_per_day_per_month_a_country('Spain', plot = True).show()


#processData = processData.ProcessData(spark)
#df = get_economic_data_per_country('Spain')
#df = get_economic_indicator_per_country('Spain', 'gdp_per_capita')
#df = get_countries_with_highest_economic_indicator(10, 'gdp_per_capita')
#df = get_countries_with_lowest_economic_indicator(10, 'gdp_per_capita')
#df = get_economic_indicator_by_continent('gdp_per_capita')
#df = get_economic_data_by_continent()

#df = processData.get_countries_with_highest_indicator_per_continent(10,'gdp_per_capita', plot=True)
#df.coalesce(1).write.save(path='./output', format='csv', mode='append', sep='\t')

#data.compare_two_countries_a_period_of_time('Spain', 'Portugal', plot = True, relative = True, smoothed=True).show()
#data.compare_two_countries_a_month_daily(11, 'Italy', 'United Kingdom', plot = True)
#data.compare_two_countries_all_months_aggregated('Canada', 'South Korea', relative = True, plot = True).show()
dir = dirname(dirname(abspath(__file__)))

def write_executable(data_type, to_execute):
    file = open(dir + '/scripts/execute.py','w')
    file.write('import covidData, economicData,populationData, processData\n')
    file.write('from pyspark.sql import SparkSession\n')
    file.write("spark = SparkSession.builder.appName('CovidAnalysis').master('local').getOrCreate()\n")
    if data_type=='covid':
        file.write('data = covidData.CovidData(spark)\n')
    elif data_type=='economy':
        file.write('data = processData.ProcessData(spark)\n')
        
    elif data_type=='population':
        file.write('data = processData.ProcessData(spark)\n')
        
    elif data_type=='health':
        file.write('data = processData.ProcessData(spark)\n')
    
    file.write(to_execute)
    file.close() 

def aux_menu(indicator, dataType):
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
        option=int(input("Enter your choice: "))
        if option==1:
            #TODO: make sure that the country is correct
            country=input("Enter the name of a country: ")
            write_executable(dataType, "data.get_indicator_per_country(" + country + ",'" + indicator + "')\n")
            break
        elif option==2:
            write_executable(dataType, "data.get_indicator_all_countries('" + indicator + "')\n")
            break
        elif option==3:
            #TODO: make sure that it is a number
            num_countries=input("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator(" + num_countries + ",'" + indicator + "',plot=True)\n")
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator(" + num_countries + ",'" + indicator + "')\n")
                    break
                else:
                    print("Wrong answer")
            break
        elif option==4:
            #TODO: make sure that it is a number
            num_countries=input("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator(" + num_countries + ",'" + indicator + "',plot=True)\n")
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator(" + num_countries + ",'" + indicator + "')\n")
                    break
                else:
                    print("Wrong answer")
            break
        elif option==5:
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_indicator_by_continent('" +  indicator + "',plot=True)\n")
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_indicator_by_continent('" + indicator + "')\n")
                    break
                else:
                    print("Wrong answer")
            break
        elif option==6:
            #TODO: make sure that it is a number
            num_countries=input("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator_per_continent(" + num_countries + ",'" + indicator + "',plot=True)\n")
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_highest_indicator_per_continent(" + num_countries + ",'" + indicator + "')\n")
                    break
                else:
                    print("Wrong answer")
            break
        elif option==7:
            #TODO: make sure that it is a number
            num_countries=input("Enter the number of countries you want on your top: ")
            while(True):
                plot=input("Do you want to plot the results[y/n]?:")
                if(plot=='y'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator_per_continent(" + num_countries + ",'" + indicator + "',plot=True)\n")
                    break
                elif(plot=='n'):
                    write_executable(dataType, "data.get_countries_with_lowest_indicator_per_continent(" + num_countries + ",'" + indicator + "')\n")
                    break
                else:
                    print("Wrong answer")
            break
        else:
            print("Wrong Choice")
    
def main():
    while True:
        print("**********************")
        print("Menu")
        print("1.Covid-19 data")
        print("2.Economic data")
        print("3.Populational data")    
        print("4.Health data")
        print("5.Exit")
        print("**********************")
        choice=int(input("Enter your choice: "))
        if choice==1:
            print("----------------------")
            print("Covid-19 data menu")
            
            print("----------------------")
            write_executable_covid_data(option)
            os.system("spark-submit scripts/execute.py")
        
        elif choice==2:
            while(True):
                print("----------------------")
                print("Economic data menu")
                print("1.GDP per capita")
                print("2.Extreme poverty rate")
                print("3.Human development index")
                print("----------------------")
                option2=int(input("Enter the indicator: "))
                if option2==1:
                    aux_menu('gdp_per_capita', 'economy')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option2==2:
                    aux_menu('extreme_poverty', 'economy')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option2==3:
                    aux_menu('human_development_index', 'economy')
                    os.system("spark-submit scripts/execute.py")
                    break
                else:
                    print("Wrong Choice")
            

        elif choice==3:
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
                option3=int(input("Enter the indicator: "))
                if option3==1:
                    aux_menu('population', 'population')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option3==2:
                    aux_menu('population_density', 'population')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option3==3:
                    aux_menu('median_age', 'population')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option3==4:
                    aux_menu('aged_65_older', 'population')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option3==5:
                    aux_menu('aged_70_older', 'population')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option3==6:
                    aux_menu('life_expectancy', 'population')
                    os.system("spark-submit scripts/execute.py")
                    break
                else:
                    print("Wrong Choice")

        elif choice==4:
            while(True):
                print("----------------------")
                print("Health data menu")
                print("1.Cardiovacular death rate")
                print("2.Diabetes prevalence")
                print("3.Female smokers")
                print("4.Male smokers")
                print("5.Handwashing facilities")
                print("6.Hospital beds per thousand")
                print("----------------------")
                option4=int(input("Enter the indicator: "))
                if option4==1:
                    aux_menu('cardiovasc_death_rate', 'health')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option4==2:
                    aux_menu('diabetes_prevalence', 'health')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option4==3:
                    aux_menu('female_smokers', 'health')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option4==4:
                    aux_menu('male_smokers', 'health')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option4==5:
                    aux_menu('handwashing_facilities', 'health')
                    os.system("spark-submit scripts/execute.py")
                    break
                elif option4==6:
                    aux_menu('hospital_beds_per_thousand', 'health')
                    os.system("spark-submit scripts/execute.py")
                    break
                else:
                    print("Wrong Choice")

        elif choice==5:
            break
        else:
            print("Wrong Choice")

if __name__ == "__main__":
    main()