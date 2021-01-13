from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from os.path import dirname, abspath
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, max, asc
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import shutil
import os

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
        silhouette_values = []
        for k in range(2, 10):
            
            kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
            model = kmeans.fit(df)

            # Make predictions
            predictions = model.transform(df)
            # Evaluate clustering by computing Silhouette score
            evaluator = ClusteringEvaluator()
            silhouette = evaluator.evaluate(predictions)
            silhouette_values.append(silhouette)
            if (best_silhouette < silhouette):
                best_silhouette = silhouette
                best_k = k

        # Plot silhouette value for each k value
        sil_save_dir = self.dir + '/graphs/ml/shilouette'
        sil_title = 'Silhouette values. Features: '
        for f in features:
            sil_save_dir = sil_save_dir + '_' + f
            sil_title = sil_title + ' ' + f
        self.plot_shilhouette(np.arange(2, 10), silhouette_values, sil_save_dir, sil_title)
        

        # Predict with the chosen model
        kmeans = KMeans().setK(best_k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df)
        predictions = model.transform(df)
        rows = predictions.select('id', 'prediction').collect()
        df_pred = self.spark.createDataFrame(rows)

        # Join the predictions with the complete dataframe
        df_final = df_pred.join(self.df_covid_data, 'id')
        df_final = df_final.join(self.df_location_id,'id').orderBy(asc('id'))

        # Create 2d graphs

        for i in range(0, len(features) - 1):
            for j in range (i+1, len(features)):
                x = df_final.select(features[i]).collect()
                y = df_final.select(features[j]).collect()
                c = df_final.select('prediction').collect()
                label_x = features[i]
                label_y = features[j]
                save_dir = self.dir + '/graphs/ml/clustering_2d_' + label_x + '_' + label_y + '.png'
                name = 'Clustering with variables ' + label_x + ', ' + label_y
                self.plot_cluster_2d(x, y, label_x, label_y, c, save_dir, name)

        # Create 3d graphs

        for i in range(0, len(features)-2):
            for j in range(i+1, len(features) -1):
                for k in range(j+1, len(features)):
                    x = df_final.select(features[i]).collect()
                    y = df_final.select(features[j]).collect()
                    z = df_final.select(features[k]).collect()
                    c = df_final.select('prediction').collect()
                    label_x = features[i]
                    label_y = features[j]
                    label_z = features[k]
                    save_dir = self.dir + '/graphs/ml/clustering_3d_' + label_x + '_' + label_y + '_' + label_z + '.png'
                    name = 'Clustering with variables ' + label_x + ', ' + label_y + ', ' + label_z
                    self.plot_cluster_3d(x, y, z, label_x, label_y, label_z, c, save_dir, name)
        
        return df_final

    def plot_cluster_3d(self, x, y, z, label_x, label_y, label_z, c, save_name, title):
        plt.close('all')
        threedee = plt.figure(figsize=(12,10)).gca(projection='3d')
        threedee.scatter(x, y, z, c = c)
        threedee.set_xlabel(label_x)
        threedee.set_ylabel(label_y)
        threedee.set_zlabel(label_z)
        plt.title(title, loc='center', wrap=True)
        if os.path.exists(save_name):
            os.remove(save_name)
        plt.savefig(save_name)
        print('Graph saved at ' + save_name)

    def plot_cluster_2d(self, x, y, label_x, label_y, c, save_name, title):
        plt.close('all')
        plt.scatter(x, y, c = c)
        plt.xlabel(label_x)
        plt.ylabel(label_y)
        plt.title(title, loc='center', wrap=True)
        if os.path.exists(save_name):
            os.remove(save_name)
        plt.savefig(save_name)
        print('Graph saved at ' + save_name)

    def plot_shilhouette(self, k_values, sil_values, save_name, title):
        plt.close('all')
        fig, ax = plt.subplots()
        ax.plot(k_values, sil_values)
        ax.set_xlabel('k value')
        ax.set_ylabel('Silhouette')
        ax.set_title(title, loc='center', wrap=True)
        if os.path.exists(save_name):
            os.remove(save_name)
        plt.savefig(save_name)
        print('Graph saved at ' + save_name)
 
