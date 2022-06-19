import pandas as pd

from application_logging.loggerDB import App_LoggerDB
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
class Data_Getter:
    """
    This class shall  be used for obtaining the data from the source for training.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None

    """
    """
    def __init__(self, file_object, logger_object):
        self.training_file='Training_FileFromDB/InputFile.csv'
        self.file_object=file_object
        self.logger_object=logger_object
    """

    def __init__(self,log_database,log_collection,execution_id):
        self.log_database=log_database
        self.log_collection=log_collection
        self.training_directory="training-file-from-db"
        self.filename="InputFile.csv"
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.az_blob_mgt=AzureBlobManagement()


    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None

        """
        #self.logger_object.log(self.file_object,'Entered the get_data method of the Data_Getter class')
        self.log_db_writer.log(self.log_database,self.log_collection,"Entered the get_data method of the Data_Getter class")
        try:

            #self.data= pd.read_csv(self.training_file) # reading the data file
            self.data=self.az_blob_mgt.readCsvFileFromDirectory(self.training_directory,self.filename)
            #self.logger_object.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            self.log_db_writer.log(self.log_database,self.log_collection,"Data Load Successful.Exited the get_data method of the Data_Getter class")
            return self.data
        except Exception as e:
            #self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            #self.logger_object.log(self.file_object,
              #                     'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            self.log_db_writer.log(self.log_database,self.log_collection,"Exception occured in get_data method of the Data_Getter class. Exception message:Data Load Unsuccessful.Exited the get_data method of the Data_Getter class ")
            raise Exception()


