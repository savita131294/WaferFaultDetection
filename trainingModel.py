"""
This is the Entry point for Training the Machine Learning Model.

Written By: iNeuron Intelligence
Version: 1.0
Revisions: None

"""


# Doing the necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger
from MongoDB.MongoDBOperation import MongoDBOperation
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
from application_logging.loggerDB import App_LoggerDB
#Creating the common Logging object


class trainModel:

    def __init__(self,execution_id):
        #self.log_writer = logger.App_Logger()
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.log_database="wafer_training_log"
        self.log_collection="model_training_tog"
        self.execution_id=execution_id
        #self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')

    def trainingModel(self):
        # Logging the start of Training
        #self.log_writer.log(self.file_object, 'Start of Training')
        self.log_db_writer.log(self.log_database,self.log_collection,"Start of Training")
        try:
            # Getting the data from the source
            #data_getter=data_loader.Data_Getter(self.file_object,self.log_writer)
            data_getter=data_loader.Data_Getter(self.log_database,self.log_collection,self.execution_id)
            data=data_getter.get_data()

            if data.__len__()==0:
                self.log_db_writer.log(self.log_database,self.log_collection,"No record found to train model")
                return 0


            """doing the data preprocessing"""

            #preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
            preprocessor=preprocessing.Preprocessor(self.log_database,self.log_collection,self.execution_id)
            data=preprocessor.remove_columns(data,['Wafer']) # remove the unnamed column as it doesn't contribute to prediction.

            # create separate features and labels
            #to be continue from here evening
            X,Y=preprocessor.separate_label_feature(data,label_column_name='Output')

            # check if missing values are present in the dataset
            is_null_present=preprocessor.is_null_present(X)

            # if missing values are there, replace them appropriately.
            if(is_null_present):
                X=preprocessor.impute_missing_values(X) # missing value imputation

            # check further which columns do not contribute to predictions
            # if the standard deviation for a column is zero, it means that the column has constant values
            # and they are giving the same output both for good and bad sensors
            # prepare the list of such columns to drop
            cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(X)

            # drop the columns obtained above
            X=preprocessor.remove_columns(X,cols_to_drop)

            """ Applying the clustering approach"""

            kmeans=clustering.KMeansClustering(execution_id=self.execution_id) # object initialization.
            number_of_clusters=kmeans.elbow_plot(X)  #  using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X=kmeans.create_clusters(X,number_of_clusters)

            #create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels']=Y

            # getting the unique clusters from our dataset
            list_of_clusters=X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i] # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label= cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=355)

                model_finder=tuner.Model_Finder(self.log_database,self.log_collection,self.execution_id) # object initialization

                #getting the best model for each of the clusters
                best_model_name,best_model=model_finder.get_best_model(x_train,y_train,x_test,y_test)

                #saving the best model to the directory.
                file_op = file_methods.File_Operation(self.log_database,self.log_collection,self.execution_id)
                save_model=file_op.save_model(best_model,best_model_name+str(i))

            # logging the successful Training
            #self.log_writer.log(self.file_object, 'Successful End of Training')
            #self.file_object.close()
            self.log_db_writer.log(self.log_database,self.log_collection,'Successful End of Training')

        except Exception:
            # logging the unsuccessful Training
            #self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            #self.file_object.close()
            self.log_db_writer.log(self.log_database,self.log_collection, 'Unsuccessful End of Training')
            #self.file_object.close()
            raise Exception