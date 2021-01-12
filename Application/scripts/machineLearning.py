from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from os.path import dirname, abspath
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, max, asc
import matplotlib.pyplot as plt
import numpy as np
import shutil

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
    
        
    def transform_dataframe(self, features):
        location_date_features = features + ['location', 'date']
        id_features = features + ['id']
        # Drop the rows that aren't related to a country
        self.df_covid_data = self.df_covid_data.where((col("location") != "World") &  (col("location") != "International"))
        # Select 'location', 'date' and features
        self.df_covid_data = self.df_covid_data.select(location_date_features)
        # Delete the rows that have a null value
        self.df_covid_data = self.df_covid_data.na.drop()
        # For each country get the latest data
        df_country_date = self.df_covid_data.groupBy('location').agg(max('date').alias('date')).select('location', 'date')
        self.df_covid_data = df_country_date.join(self.df_covid_data, ['date', 'location'])
        # Create an id per country
        window = Window.orderBy(asc('location'))
        self.df_covid_data = self.df_covid_data.withColumn('id', row_number().over(window))
        self.df_location_id = self.df_covid_data.select('id', 'location')
        # Select 'id' and features
        self.df_covid_data = self.df_covid_data.select(id_features)


    def main(self, features): 
        self.transform_dataframe(features)
        df = self.df_covid_data
        for col in df.columns:
            if col in features:
                df = df.withColumn(col,df[col].cast('float'))
        vecAssembler = VectorAssembler(inputCols=features, outputCol="features")
        df = vecAssembler.transform(df).select('id', 'features')

        # Calculate the best value for k 
        best_k = -1
        best_silhouette = -1
        for k in range(2, 10):
            
            kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
            model = kmeans.fit(df)

            # Make predictions
            predictions = model.transform(df)
            # Evaluate clustering by computing Silhouette score
            evaluator = ClusteringEvaluator()
            silhouette = evaluator.evaluate(predictions)
            if (best_silhouette < silhouette):
                best_silhouette = silhouette
                best_k = k
            
        kmeans = KMeans().setK(best_k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df)
        predictions = model.transform(df)
        rows = predictions.select('id', 'prediction').collect()
        df_pred = self.spark.createDataFrame(rows)

        # Join the predictions with the complete dataframe
        df_final = df_pred.join(self.df_covid_data, 'id')
        df_final = df_final.join(self.df_location_id,'id').orderBy(asc('id'))

        # Create graph
        '''        
        threedee = plt.figure(figsize=(12,10)).gca(projection='3d')
        threedee.scatter(pddf_pred.x, pddf_pred.y, pddf_pred.z, c=pddf_pred.prediction)
        threedee.set_xlabel('x')
        threedee.set_ylabel('y')
        threedee.set_zlabel('z')
        plt.show()
        '''
        return df_final
