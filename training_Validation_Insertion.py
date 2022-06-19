from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation,DbOperationMongoDB
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger
from application_logging.loggerDB import App_LoggerDB
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
class train_validation:
    def __init__(self,path,execution_id):
        self.raw_data = Raw_Data_validation(path,execution_id)
        self.dataTransform = dataTransform(execution_id)
        #self.dBOperation = dBOperation() # code commented by avnish yadav
        self.dBOperationMongoDB=DbOperationMongoDB(execution_id)# code added by avnish yadav
        #self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+') # code commented by avnish yadav
        self.log_database="wafer_training_log" #code added by Avnish Yadav
        self.log_collection="training_main_log" #code added by Avnish Yadav
        #self.log_writer = logger.App_Logger() # code commented by Avnish Yadav
        self.execution_id=execution_id #code added by Avnish Yadav
        self.logDB_write=App_LoggerDB(execution_id=execution_id)  #code Added by Avnish Yadav
        self.az_blob_mgt=AzureBlobManagement() #code Added by Avnish Yadav

    def train_validation(self):
        try:
            #self.log_writer.log(self.file_object, 'Start of Validation on files!!') ##code Added by Avnish Yadav
            self.logDB_write.log(self.log_database,self.log_collection,'Start of Validation on files!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            #self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Raw Data Validation Complete!!")

            #self.log_writer.log(self.file_object, "Starting Data Transforamtion!!")
            self.logDB_write.log(self.log_database, self.log_collection, "Starting Data Transforamtion!!")

            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            #self.log_writer.log(self.file_object, "DataTransformation Completed!!!")
            self.logDB_write.log(self.log_database,self.log_collection,"DataTransformation Completed!!!")


            #self.log_writer.log(self.file_object,
             #                   "Creating Training_Database and tables on the basis of given schema!!!")

            # create database with given name, if present open the connection! Create table with columns given in schema

            ##self.dBOperation.createTableDb('Training', column_names)
            #self.log_writer.log(self.file_object, "Table creation Completed!!")
            #self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.logDB_write.log(self.log_database,self.log_collection,"Creating database and collection if not exist then insert record")
            #self.dBOperation.insertIntoTableGoodData('Training')
            self.dBOperationMongoDB.insertIntoTableGoodData(column_names)

            #self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            #self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Insertion in Table completed!!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table

            self.raw_data.deleteExistingGoodDataTrainingFolder()
            #self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Good_Data folder deleted!!!")
            #self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()

            #self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Bad files moved to archive!! Bad folder Deleted!!")
            #self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Validation Operation completed!!")
            #self.log_writer.log(self.file_object, "Extracting csv file from table")
            self.logDB_write.log(self.log_database,self.log_collection,"Extracting csv file from table")
            # export data in table to csvfile
            self.dBOperationMongoDB.selectingDatafromtableintocsv()
            #self.dBOperation.selectingDatafromtableintocsv('Training')
            #self.file_object.close()

        except Exception as e:
            raise e









