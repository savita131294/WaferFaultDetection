import pandas
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
from application_logging.loggerDB import App_LoggerDB
from MongoDB.MongoDBOperation import MongoDBOperation

class prediction:

    def __init__(self,path,execution_id):
        self.execution_id=execution_id

        #self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        #self.log_writer = logger.App_Logger()
        self.log_database="wafer_prediction_log"
        self.log_collection="prediction_log"
        self.log_db_writer=App_LoggerDB(execution_id)
        self.az_blob_mgt=AzureBlobManagement()
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(path,execution_id)

    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            #self.log_writer.log(self.file_object,'Start of Prediction')
            self.log_db_writer.log(self.log_database,self.log_collection,'Start of Prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.log_database,self.log_collection,self.execution_id)
            data=data_getter.get_data()
            path=""
            if data.__len__()==0:
                self.log_db_writer.log(self.log_database,self.log_collection,"No data was present to perform prediction existing prediction method")
                return path,"No data was present to perform prediction"
            #code change
            # wafer_names=data['Wafer']
            # data=data.drop(labels=['Wafer'],axis=1)

            #preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
            preprocessor = preprocessing.Preprocessor(self.log_database, self.log_collection, self.execution_id)

            is_null_present=preprocessor.is_null_present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data)

            cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(data)
            data=preprocessor.remove_columns(data,cols_to_drop)
            #data=data.to_numpy()
            file_loader=file_methods.File_Operation(self.log_database, self.log_collection, self.execution_id)
            kmeans=file_loader.load_model('KMeans')

            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            clusters=kmeans.predict(data.drop(['Wafer'],axis=1))#drops the first column for cluster prediction
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            for i in clusters:
                cluster_data= data[data['clusters']==i]
                wafer_names = list(cluster_data['Wafer'])
                cluster_data=data.drop(labels=['Wafer'],axis=1)
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result=list(model.predict(cluster_data))
                result = pandas.DataFrame(list(zip(wafer_names,result)),columns=['Wafer','Prediction'])
                #path="Prediction_Output_File/Predictions.csv"
                path="prediction-output-file"
                self.az_blob_mgt.saveDataFrameTocsv(path,"prediction.csv",result,header=True,mode="a+")
                #result.to_csv("Prediction_Output_File/Predictions.csv",header=True,mode='a+') #appends result to prediction file
            #self.log_writer.log(self.file_object,'End of Prediction')
            self.log_db_writer.log(self.log_database,self.log_collection,'End of prediction')
        except Exception as ex:
            #self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            self.log_db_writer.log(self.log_database,self.log_collection,'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path, result.head().to_json(orient="records")




