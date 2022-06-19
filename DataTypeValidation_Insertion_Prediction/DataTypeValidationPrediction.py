import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from application_logging.logger import App_Logger

from MongoDB.MongoDBOperation import MongoDBOperation
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
from application_logging.loggerDB import App_LoggerDB

class DbOperationMongoDB:
    def __init__(self,execution_id):
        self.mongodb=MongoDBOperation()
        self.az_blob_mgt=AzureBlobManagement()
        self.logger_db_writer=App_LoggerDB(execution_id=execution_id)
        self.good_file_path="good-raw-file-prediction-validated"
        self.bad_file_path="bad-raw-file-prediction-validated"


    def insertIntoTableGoodData(self,column_names):
        """
        Description: Load all csv file into mongo db database "training_database" ,collection:"Good_Raw_Data"


        :return:
        """
        try:
            prediction_database="prediction_database"
            prediction_collection="Good_Raw_Data"
            database_name = "wafer_prediction_log"
            collection_name = "db_insert_log"
            self.mongodb.dropCollection(prediction_database,prediction_collection)
            self.logger_db_writer.log(database_name,collection_name,"Droping collection:"+prediction_collection+" from database:"+prediction_database)
            self.logger_db_writer.log(database_name, collection_name,"Starting loading of good files in database:training_database and collection: Good_Raw_Data")
            files = self.az_blob_mgt.getAllFileNameFromDirectory(self.good_file_path)
            self.logger_db_writer.log(database_name, collection_name,"No of file found in good-raw-file-train-validated " + str(len(files)))
            for file in files:
                try:
                    self.logger_db_writer.log(database_name, collection_name,
                                              "Insertion of file +" + file + " started...")
                    df = self.az_blob_mgt.readCsvFileFromDirectory(self.good_file_path, file)
                    df.columns=column_names
                    self.mongodb.insertDataFrame(prediction_database, prediction_collection, df)
                    self.logger_db_writer.log(database_name, collection_name,
                                              "File: {0} loaded successfully".format(file))
                except Exception as e:
                    self.logger_db_writer.log(database_name, collection_name, str(e))
                    self.az_blob_mgt.moveFileInDirectory(self.good_file_path, self.bad_file_path, file)
                    self.logger_db_writer.log(database_name, collection_name,
                                              "File: " + file + " was not loaded successfully hence moved tp dir:" + self.bad_file_path)

        except Exception as e:
            error_message = "Error occured in class:DbOperationMongoDB method:insertIntoTableGoodData error:" + str(e)
            self.logger_db_writer.log(database_name, collection_name, error_message)

    def selectingDatafromtableintocsv(self,):
        """

        :return:
        """
        try:
            directory_name="prediction-file-from-db"
            file_name="InputFile.csv"
            database_name = "wafer_prediction_log"
            collection_name = "export_to_csv"
            prediction_database="prediction_database"
            prediction_collection="Good_Raw_Data"
            msg="starting of loading of database:"+prediction_database+",collection:"+prediction_collection+" records into file:"+file_name
            self.logger_db_writer.log(database_name,collection_name,msg)
            df=self.mongodb.getDataFrameofCollection(prediction_database,prediction_collection)
            msg="Good_Raw_data has been loaded into pandas dataframe"
            self.logger_db_writer.log(database_name,collection_name,msg)
            self.az_blob_mgt.saveDataFrameTocsv(directory_name,file_name,df)
            msg = "InputFile.csv created successfully in directory"+directory_name
            self.logger_db_writer.log(database_name, collection_name, msg)
        except Exception as e:
            msg="Error occured in class:DbOperationMongoDB method:insertIntoTableGoodData error:"+str(e)
            self.logger_db_writer.log(database_name,collection_name,msg)









class dBOperation:
    """
          This class shall be used for handling all the SQL operations.

          Written By: iNeuron Intelligence
          Version: 1.0
          Revisions: None

          """

    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()


    def dataBaseConnection(self,DatabaseName):

        """
                        Method Name: dataBaseConnection
                        Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
                        Output: Connection to the DB
                        On Failure: Raise ConnectionError

                         Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                        """
        try:
            conn = sqlite3.connect(self.path+DatabaseName+'.db')

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createTableDb(self,DatabaseName,column_names):

        """
           Method Name: createTableDb
           Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
           Output: None
           On Failure: Raise Exception

            Written By: iNeuron Intelligence
           Version: 1.0
           Revisions: None

        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]

                # we will remove the column of string datatype before loading as it is not needed for training
                #in try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create the table
                try:
                    #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                except:
                    conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

            conn.close()

            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e


    def insertIntoTableGoodData(self,Database):

        """
                                       Method Name: insertIntoTableGoodData
                                       Description: This method inserts the Good data files from the Good_Raw folder into the
                                                    above created table.
                                       Output: None
                                       On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                       Version: 1.0
                                       Revisions: None

                """

        conn = self.dataBaseConnection(Database)
        goodFilePath= self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:

                with open(goodFilePath+'/'+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()
                raise e

        conn.close()
        log_file.close()


    def selectingDatafromtableintocsv(self,Database):

        """
                                       Method Name: selectingDatafromtableintocsv
                                       Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                                    above created .
                                       Output: None
                                       On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                       Version: 1.0
                                       Revisions: None

                """

        self.fileFromDb = 'Prediction_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()

            #Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            raise e





