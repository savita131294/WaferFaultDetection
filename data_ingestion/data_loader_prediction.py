import pandas as pd
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
from application_logging.loggerDB import App_LoggerDB
class Data_Getter_Pred:
    """
    This class shall  be used for obtaining the data from the source for prediction.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None

    """
    """
    def __init__(self, file_object, logger_object):
        self.prediction_file='Prediction_FileFromDB/InputFile.csv'
        self.file_object=file_object
        self.logger_object=logger_object
    """
    def __init__(self,log_database,log_collection,execution_id):
        self.log_database=log_database
        self.log_collection=log_collection
        self.prediction_directory="prediction-file-from-db"
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
        # self.logger_object.log(self.file_object,'Entered the get_data method of the Data_Getter class')
        self.log_db_writer.log(self.log_database, self.log_collection,
                               "Entered the get_data method of the Data_Getter class")
        try:

            # self.data= pd.read_csv(self.prediction_file) # reading the data file
            self.data = self.az_blob_mgt.readCsvFileFromDirectory(self.prediction_directory, self.filename)
            # self.logger_object.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            self.log_db_writer.log(self.log_database, self.log_collection,
                                   "Data Load Successful.Exited the get_data method of the Data_Getter class")
            return self.data
        except Exception as e:
            # self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            # self.logger_object.log(self.file_object,
            #                     'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            self.log_db_writer.log(self.log_database, self.log_collection,
                                   "Exception occured in get_data method of the Data_Getter class. Exception message:Data Load Unsuccessful.Exited the get_data method of the Data_Getter class ")
            raise Exception()



