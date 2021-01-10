from pyspark.sql.functions import col, asc,desc, when, udf, max, avg
from os.path import dirname, abspath


import covidData_graphs
import utils



class vaccinesData:

	def __init__(self, sparkSes):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        self.df_covid_data = self.spark.read.csv(self.dir + '/datasets/vaccine.csv', header = True, inferSchema=True)

    #Show the rate of how the people agree with the importance of the vaccines in the country chosen. 
    def  get_vaccines_importance_data_per_country(self, country):
     	return(self.df_covid_data.filter(self.df_covid_data['name']== country)
     			.groupBy('name', 'Vaccine importance, Strongly agree (%)','Vaccine importance,Tend to agree (%)', 'Vaccine importance,Tend to disagree (%)',
     			 		'Vaccine importance,Strongly disagree (%)', 'Vaccine importance,Agree (%)' )
     			.agg(max('year'))
     			.select('name', 'Vaccine importance, Strongly agree (%)','Vaccine importance,Tend to agree (%)', 'Vaccine importance,Tend to disagree (%)',
     			 		'Vaccine importance,Strongly disagree (%)', 'Vaccine importance,Agree (%)' ) )

    #Show the rate of how the people agree with how safety are the vaccines in the country chosen. 
    def  get_vaccines_safety_data_per_country(self, country):
     	return(self.df_covid_data.filter(self.df_covid_data['name']== country)
     			.groupBy('name', 'Vaccine safety, Strongly agree (%)','Vaccine safety,Tend to agree (%)', 'Vaccine safety,Tend to disagree (%)',
     			 		'Vaccine safety,Strongly disagree (%)', 'Vaccine safety,Agree (%)' )
     			.agg(max('year'))
     			.select('name', 'Vaccine safety, Strongly agree (%)','Vaccine safety,Tend to agree (%)', 'Vaccine safety,Tend to disagree (%)',
     			 		'Vaccine safety,Strongly disagree (%)', 'Vaccine safety,Agree (%)' ) )

    #Show the rate of how the people agree with the effectiveness of the vaccines in the country chosen. 
    def  get_vaccines_effectiveness_data_per_country(self, country):
     	return(self.df_covid_data.filter(self.df_covid_data['name']== country)
     			.groupBy('name', 'Vaccine effectiveness, Strongly agree (%)','Vaccine effectiveness,Tend to agree (%)', 'Vaccine effectiveness,Tend to disagree (%)',
     			 		'Vaccine effectiveness,Strongly disagree (%)', 'Vaccine effectiveness,Agree (%)' )
     			.agg(max('year'))
     			.select('name', 'Vaccine effectiveness, Strongly agree (%)','Vaccine effectiveness,Tend to agree (%)', 'Vaccine effectiveness,Tend to disagree (%)',
     			 		'Vaccine effectiveness,Strongly disagree (%)', 'Vaccine effectiveness,Agree (%)' ) )

    #Return the num_countries countries with the highest indicator
    def get_countries_vaccines_with_highest_indicator(self, num_countries, indicator):
        return (self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull())
                .groupBy('name', indicator)
                .agg(max('year'))
                .select('name', indicator)
                .sort(col(indicator).desc()).limit(num_countries))
  
    #Return the num_countries countries with the lowest indicator
    def get_countries_vaccines_with_lowest_indicator(self, num_countries, indicator):
        return (self.df_covid_data.filter(self.df_covid_data[indicator].isNotNull())
                .groupBy('name', indicator)
                .agg(max('year'))
                .select('name', indicator)
                .sort(col(indicator).asc()).limit(num_countries))









