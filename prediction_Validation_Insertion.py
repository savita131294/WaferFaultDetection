from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import DbOperationMongoDB
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from application_logging import logger

from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
from MongoDB.MongoDBOperation import MongoDBOperation
from application_logging.loggerDB import App_LoggerDB
class pred_validation:
    def __init__(self,path,execution_id):
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.log_database="wafer_prediction_log"
        self.log_collection="prediction_log"
        self.raw_data = Prediction_Data_validation(path,execution_id)
        self.dataTransform = dataTransformPredict(execution_id)
        #self.dBOperation = dBOperation()
        self.dBOperation=DbOperationMongoDB(execution_id=execution_id)
        #self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        #self.log_writer = logger.App_Logger()

    def prediction_validation(self):

        try:

            #self.log_writer.log(self.file_object,'Start of Validation on files for prediction!!')
            self.log_db_writer.log(self.log_database,self.log_collection, 'Start of Validation on files for prediction!!')

            #extracting values from prediction schema
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()
            #getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            #validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            #validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            #validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            #self.log_writer.log(self.file_object,"Raw Data Validation Complete!!")
            self.log_db_writer.log(self.log_database,self.log_collection,"Raw Data Validation Complete!!")
            #self.log_writer.log(self.file_object,("Starting Data Transforamtion!!"))
            self.log_db_writer.log(self.log_database,self.log_collection,"Starting Data Transforamtion!!")
            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            #self.log_writer.log(self.file_object,"DataTransformation Completed!!!")
            self.log_db_writer.log(self.log_database,self.log_collection,"DataTransformation Completed!!!")

            #self.log_writer.log(self.file_object,"Creating Prediction_Database and tables on the basis of given schema!!!")
            self.log_db_writer.log(self.log_database,self.log_collection,
                                "Creating Prediction_Database and tables on the basis of given schema!!!")
            #create database with given name, if present open the connection! Create table with columns given in schema
            #self.dBOperation.createTableDb('Prediction',column_names)
            #self.log_writer.log(self.file_object, "Table creation Completed!!")
            #self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            #self.log_db_writer.log(self.log_database,self.log_collection,"Table creation Completed!!")

            self.log_db_writer.log(self.log_database,self.log_collection, "Creating database and collection if not exist then insert record")
            #insert csv files in the table
            self.dBOperation.insertIntoTableGoodData(column_names)
            #self.log_writer.log(self.file_object,"Insertion in Table completed!!!")
            #self.log_writer.log(self.file_object,"Deleting Good Data Folder!!!")
            self.log_db_writer.log(self.log_database,self.log_collection, "Insertion in Table completed!!!")
            self.log_db_writer.log(self.log_database,self.log_collection, "Deleting Good Data Folder!!!")
            #Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            #self.log_writer.log(self.file_object,"Good_Data folder deleted!!!")
            #self.log_writer.log(self.file_object,"Moving bad files to Archive and deleting Bad_Data folder!!!")
            self.log_db_writer.log(self.log_database,self.log_collection,"Good_Data folder deleted!!!")
            self.log_db_writer.log(self.log_database,self.log_collection, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            #Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            #self.log_writer.log(self.file_object,"Bad files moved to archive!! Bad folder Deleted!!")
            #self.log_writer.log(self.file_object,"Validation Operation completed!!")
            #self.log_writer.log(self.file_object,"Extracting csv file from table")
            self.log_db_writer.log(self.log_database,self.log_collection, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_db_writer.log(self.log_database,self.log_collection, "Validation Operation completed!!")
            self.log_db_writer.log(self.log_database,self.log_collection, "Extracting csv file from table")
            #export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv()

        except Exception as e:
            raise e









