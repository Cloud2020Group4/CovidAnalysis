from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from os.path import dirname, abspath
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
import matplotlib.pyplot as plt
import numpy as np

class MachineLearning:
    # [Constructor] the function receives a SparkSession and initializes the dataframe needed
    def __init__(self, sparkSes, mode):
        self.spark = sparkSes
        self.dir = dirname(dirname(abspath(__file__)))
        if mode == 'local':
            self.data_dir = self.dir + "/datasets/owid-covid-data.csv"
        elif mode == 'hadoop':
            self.data_dir = "owid-covid-data.csv"
        self.df_covid_data = self.spark.read.csv(self.data_dir, header = True, inferSchema=True)
    
        #Drop the rows that aren't related to a country
        self.df_covid_data = self.df_covid_data.where((col("location") != "World") &  (col("location") != "International"))
        # Create an identifier for each country
        window = Window.orderBy('location')
        self.df_country_id = (self.df_covid_data.select('location')).withColumn('id', row_number().over(window))
        self.df_covid_data = self.df_covid_data.join(self.df_country_id , 'location')
        # Delete the columns that contain strings
        self.df_covid_data = self.df_covid_data.drop('iso_code','continent','location', 'date')


    def main(self): 
        df = self.df_covid_data.na.drop()
        FEATURES_COL = ['total_cases', 'new_cases']
        vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
        df = vecAssembler.transform(df).select('id', 'features')
        # Trains a k-means model.
        # Find the best k 
        cost = np.zeros(20)
        for k in range(2,20):
            kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
            model = kmeans.fit(df.sample(False,0.1, seed=42))
            cost[k] = model.computeCost(df)

        fig, ax = plt.subplots(1,1, figsize =(8,6))
        ax.plot(range(2,20),cost[2:20])
        ax.set_xlabel('k')
        ax.set_ylabel('cost')
        plt.savefig(dir + "best_k.png")

        # Make predictions
        #predictions = model.transform(df)

        # Evaluate clustering by computing Silhouette score
        #evaluator = ClusteringEvaluator()

        #silhouette = evaluator.evaluate(predictions)
        #print("Silhouette with squared euclidean distance = " + str(silhouette))

        # Shows the result.
        #centers = model.clusterCenters()
        #print("Cluster Centers: ")
        #for center in centers:
        #    print(center)