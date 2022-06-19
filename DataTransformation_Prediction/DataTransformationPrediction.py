from datetime import datetime
from os import listdir
import pandas
from application_logging.logger import App_Logger

from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
from application_logging.loggerDB import App_LoggerDB
from MongoDB.MongoDBOperation import MongoDBOperation
class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

                  Written By: iNeuron Intelligence
                  Version: 1.0
                  Revisions: None

                  """

     def __init__(self,execution_id):
          self.execution_id=execution_id
          #self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"
          self.goodDataPath="good-raw-file-prediction-validated"
          #self.logger = App_Logger()
          self.log_db_writer=App_LoggerDB(execution_id=execution_id)
          self.log_database="wafer_prediction_log"
          self.az_blob_mgt=AzureBlobManagement()

     def replaceMissingWithNull(self):

          """
                                  Method Name: replaceMissingWithNull
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                                          """

          try:
               log_collection="data_transform_log"
               #log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
               #onlyfiles = [f for f in listdir(self.goodDataPath)]
               onlyfiles=self.az_blob_mgt.getAllFileNameFromDirectory(self.goodDataPath)
               for file in onlyfiles:
                    #csv = pandas.read_csv(self.goodDataPath+"/" + file)
                    csv=self.az_blob_mgt.readCsvFileFromDirectory(self.goodDataPath,file)
                    csv.fillna('NULL',inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Wafer'] = csv['Wafer'].str[6:]
                    #csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    self.az_blob_mgt.saveDataFrameTocsv(self.goodDataPath,file,csv,idex=None,header=True)
                    #self.logger.log(log_file," %s: File Transformed successfully!!" % file)
                    self.log_db_writer.log(self.log_database,log_collection,"File {0} transformed successfully".format(file))
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")

          except Exception as e:
               #self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               #log_file.close()
               self.log_db_writer.log(self.log_database,log_collection,'Data Transformation failed because:'+str(e))
               raise e
          #log_file.close()
