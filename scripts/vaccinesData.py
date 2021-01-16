from pyspark.sql.functions import col, asc,desc, when, udf, max, avg
from os.path import dirname, abspath

import covidData_graphs
import utils

class VaccinesData:

    def __init__(self, sparkSes, mode):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        if mode == 'local':
            self.data_dir = self.dir + "/datasets/vaccine.csv"
        elif mode == 'hadoop':
            self.data_dir = "vaccine.csv"

        self.df_covid_data = self.spark.read.csv(self.data_dir, header = True, inferSchema=True)

    #Show the rate of how the people agree with the importance of the vaccines in the country chosen. 
    def  get_vaccines_importance_data_per_country(self, country):
     	return(self.df_covid_data.filter(self.df_covid_data['name']== country)
     			.groupBy('name', 'Vaccine importance, Strongly agree (%)','Vaccine importance,Tend to agree (%)', 'Vaccine importance,Tend to disagree (%)',
     			 		'Vaccine importance,Strongly disagree (%)', 'Vaccine importance,Agree (%)' )
     			.agg(max('year')))

    #Show the rate of how the people agree with how safety are the vaccines in the country chosen. 
    def  get_vaccines_safety_data_per_country(self, country):
     	return(self.df_covid_data.filter(self.df_covid_data['name']== country)
     			.groupBy('name', 'vaccine safety,Strongly agree (%)','vaccine safety,Tend to agree(%)', 'vaccine safety,Tend to disagree(%)',
     			 		'vaccine safety,Strongly disagree(%)', 'vaccine safety,Agree(%)' )
     			.agg(max('year')))

    #Show the rate of how the people agree with the effectiveness of the vaccines in the country chosen. 
    def  get_vaccines_effectiveness_data_per_country(self, country):
     	return(self.df_covid_data.filter(self.df_covid_data['name']== country)
     			.groupBy('name', 'vaccine effectiveness,Tend to agree (%)', 'vaccine effectiveness,Tend to disagree (%)',
     			 		'vaccine effectiveness,Strongly disagree (%)', 'vaccine effectiveness,Agree (%)' )
     			.agg(max('year')))