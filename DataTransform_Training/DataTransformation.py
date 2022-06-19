from datetime import datetime
from os import listdir
import pandas
from application_logging.logger import App_Logger

from MongoDB.MongoDBOperation import MongoDBOperation
from application_logging.loggerDB import  App_LoggerDB
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
class dataTransform:

     """
               This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

     def __init__(self,execution_id):
          #self.goodDataPath = "Training_Raw_files_validated/Good_Raw" #code commented by avnish yadav
          self.goodDataPath="good-raw-file-train-validated"
          self.execution_id=execution_id
          #self.logger = App_Logger()#code commented by avnish yadav
          self.logger_db_writer=App_LoggerDB(execution_id)
          self.az_blob_mgt=AzureBlobManagement()


     def replaceMissingWithNull(self):
          """
                                           Method Name: replaceMissingWithNull
                                           Description: This method replaces the missing values in columns with "NULL" to
                                                        store in the table. We are using substring in the first column to
                                                        keep only "Integer" data for ease up the loading.
                                                        This column is anyways going to be removed during training.

                                            Written By: iNeuron Intelligence
                                           Version: 1.0
                                           Revisions: None

                                                   """
          log_collection="data_transform_log"
          log_database="wafer_training_log"
          #log_file = open("Training_Logs/dataTransformLog.txt", 'a+')
          try:

               #onlyfiles = [f for f in listdir(self.goodDataPath)]
               onlyfiles=self.az_blob_mgt.getAllFileNameFromDirectory(self.goodDataPath)
               for file in onlyfiles:
                    #csv = pandas.read_csv(self.goodDataPath+"/" + file)
                    csv=self.az_blob_mgt.readCsvFileFromDirectory(self.goodDataPath,file_name=file)
                    csv.fillna('NULL',inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Wafer'] = csv['Wafer'].str[6:]
                    #csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    self.az_blob_mgt.saveDataFrameTocsv(self.goodDataPath,file,csv,index=None,header=True)
                    #self.logger.log(log_file," %s: File Transformed successfully!!" % file)
                    self.logger_db_writer.log(log_database,log_collection,"File {0} Transformed successfully!!".format(file))
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
          except Exception as e:
               msg="Error occured in class:dataTransform method:replaceMissingWithNull error:Data Transformation failed because:"+str(e)
               self.logger_db_writer.log(log_database,log_collection,msg)
               raise e
               #self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               #log_file.close()
          #log_file.close()
