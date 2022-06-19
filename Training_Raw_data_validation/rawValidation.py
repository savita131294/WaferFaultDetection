import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger
from application_logging.loggerDB import App_LoggerDB
from MongoDB.MongoDBOperation import MongoDBOperation
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement


class Raw_Data_validation:

    """
             This class shall be used for handling all the validation done on the Raw Training Data!!.

             Written By: iNeuron Intelligence
             Version: 1.0
             Revisions: None

             """

    def __init__(self,path,execution_id):
        self.Batch_Directory = path
        self.exexcution_id=execution_id
        #self.schema_path = 'schema_training.json'
        self.collection_name="schema_training" #code added by Avnish yadav
        self.database_name="wafer_sys"#code added by Avnish yadav
        #self.logger = App_Logger()
        self.logger_db_writer=App_LoggerDB(execution_id=execution_id) #code added by Avnish yadav
        self.mongdb=MongoDBOperation()
        self.az_blob_mgt=AzureBlobManagement()
        self.good_directory_path="good-raw-file-train-validated"
        self.bad_directory_path="bad-raw-file-train-validated"


    def valuesFromSchema(self):
        """
                        Method Name: valuesFromSchema
                        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception

                         Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                                """
        try:
            """code commented by Avnish Yadav
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            """
            #code started by Avnish Yadav
            log_database="wafer_training_log"
            log_collection="values_from_schema_validation"

            df_schema_training=self.mongdb.getDataFrameofCollection(self.database_name,self.collection_name)
            dic={}
            [dic.update({i: df_schema_training.loc[0, i]}) for i in df_schema_training.columns]
            del df_schema_training
            #code ended by Avnish Yadav

            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            #file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            #self.logger.log(file,message) code commented by Avnish Yadav
            self.logger_db_writer.log(log_database,log_collection,message)


            #file.close()



        except ValueError:
            #file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            #self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            #file.close()
            self.logger_db_writer.log(log_database,log_collection,"Error occured in class:Raw_Data_validation method: valuesFromSchema  ValueError:Value not found inside collection schema_training")
            raise ValueError

        except KeyError:
            #file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            #self.logger.log(file, "KeyError:Key value error incorrect key passed")
            #file.close()
            self.logger_db_writer.log(log_database, log_collection,
                                      "Error occured in class:Raw_Data_validation method: valuesFromSchema KeyError:Key value error incorrect key passed")

            raise KeyError

        except Exception as e:
            #file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            #self.logger.log(file, str(e))
            #file.close()
            self.logger_db_writer.log(log_database,log_collection,"Error occured in class:Raw_Data_validation method: valuesFromSchema error"+str(e))
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None

                                 Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                                        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: createDirectoryForGoodBadRawData
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data.

                                      Output: None
                                      On Failure: OSError

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """



        """
        try:
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError
        """
        try:
            log_database="wafer_training_log"
            log_collection="general_log"
            self.az_blob_mgt.createDirectory(self.good_directory_path,is_replace=True)
            self.az_blob_mgt.createDirectory(self.bad_directory_path,is_replace=True)
            msg=self.good_directory_path+" and "+self.bad_directory_path+" created successfully."
            self.logger_db_writer.log(log_database,log_collection,msg)
        except Exception as e:
            msg ="Error Occured in class Raw_Data_validation method:createDirectoryForGoodBadRawData error: Failed to create directory " +self.good_directory_path + " and " + self.bad_directory_path
            self.logger_db_writer.log(log_database,log_collection,msg)
            raise e

    def deleteExistingGoodDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made  to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            """
            path = 'Training_Raw_files_validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
            """
            log_database="wafer_training_log"
            log_collection="general_log"
            self.az_blob_mgt.deleteDirectory(self.good_directory_path)
            self.logger_db_writer.log(log_database,log_collection,self.good_directory_path+" deleted successfully!!")
            """
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError
            """
        except Exception as e:
            msg="Error Occured in class Raw_Data_validation method:deleteExistingGoodDataTrainingFolder Error occured while deleting :"+self.good_directory_path
            self.logger_db_writer.log(log_database,log_collection,msg)
            raise e



    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            """
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError"""
            log_database = "wafer_training_log"
            log_collection = "general_log"
            self.az_blob_mgt.deleteDirectory(self.bad_directory_path)
            self.logger_db_writer.log(log_database, log_collection,
                                      self.bad_directory_path + " deleted successfully!!")

        except Exception as e:
            msg = "Error Occured in class Raw_Data_validation method:deleteExistingGoodDataTrainingFolder Error occured while deleting :" + self.good_directory_path
            self.logger_db_writer.log(log_database, log_collection, msg)
            raise e

    def moveBadFilesToArchiveBad(self):

        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            log_database="wafer_training_log"
            log_collection="general_log"

            #source = 'Training_Raw_files_validated/Bad_Raw/'
            source=self.bad_directory_path
            destination="lat-" + self.exexcution_id
            self.logger_db_writer.log(log_database,log_collection,"Started moving bad raw data..")
            for file in self.az_blob_mgt.getAllFileNameFromDirectory(source):

                self.az_blob_mgt.moveFileInDirectory(source,destination,file)
                self.logger_db_writer.log(log_database,log_collection,"File:"+file+" moved to directory:"+destination+" successfully.")

            self.logger_db_writer.log(log_database,log_collection,"All bad raw file moved to directory:"+destination)

            self.az_blob_mgt.deleteDirectory(source)
            self.logger_db_writer.log(log_database,log_collection,"Deleting bad raw directory:"+source)

            """
            if os.path.isdir(source):

                path = "TrainingArchiveBadData"

                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
                """
        except Exception as e:
            """
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            """
            self.logger_db_writer.log(log_database,log_collection,"class Raw_Data_validation method:moveBadFilesToArchiveBad Error while moving bad files to archive:"+str(e))
            raise e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

                """

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        """
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        """
        self.createDirectoryForGoodBadRawData()
        #onlyfiles = [f for f in listdir(self.Batch_Directory)]
        onlyfiles=self.az_blob_mgt.getAllFileNameFromDirectory(self.Batch_Directory)
        try:
            log_database="wafer_training_log"
            log_collection="name_validation_log"
            #f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            """
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
"""
                            self.az_blob_mgt.copyFileInDirectory(self.Batch_Directory,self.good_directory_path,filename)
                            self.logger_db_writer.log(log_database,log_collection,"Valid File name!! File moved to "+self.good_directory_path +filename)

                        else:
                            """
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                            """
                            self.az_blob_mgt.copyFileInDirectory(self.Batch_Directory, self.bad_directory_path,
                                                                 filename)
                            msg="Invalid File Name !! File moved to "+self.bad_directory_path+filename
                            self.logger_db_writer.log(log_database,log_collection,msg)
                    else:
                        """
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                        """
                        self.az_blob_mgt.copyFileInDirectory(self.Batch_Directory, self.bad_directory_path,
                                                             filename)
                        msg="Invalid File Name !! File moved to "+self.bad_directory_path+filename
                        self.logger_db_writer.log(log_database, log_collection, msg)

                else:
                    """
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    """
                    self.az_blob_mgt.copyFileInDirectory(self.Batch_Directory, self.bad_directory_path,
                                                         filename)
                    msg = "Invalid File Name !! File moved to " + self.bad_directory_path + filename
                    self.logger_db_writer.log(log_database, log_collection, msg)


            #f.close()

        except Exception as e:
            """
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            """
            msg = "Error occured while validating FileName " + str(e)
            self.logger_db_writer.log(log_database, log_collection, msg)
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
                          Method Name: validateColumnLength
                          Description: This function validates the number of columns in the csv files.
                                       It is should be same as given in the schema file.
                                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.
                                      The csv file is missing the first column name, this function changes the missing name to "Wafer".
                          Output: None
                          On Failure: Exception

                           Written By: iNeuron Intelligence
                          Version: 1.0
                          Revisions: None

                      """
        try:
            log_collection="column_validation_log"
            log_database="wafer_training_log"
            #f = open("Training_Logs/columnValidationLog.txt", 'a+')
            #self.logger.log(f,"Column Length Validation Started!!")
            self.logger_db_writer.log(log_database,log_collection,"Column Length Validation Started!!")
            #for file in listdir('Training_Raw_files_validated/Good_Raw/'):
            for file in self.az_blob_mgt.getAllFileNameFromDirectory(self.good_directory_path):
                #csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                csv=self.az_blob_mgt.readCsvFileFromDirectory(self.good_directory_path,file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    """
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            """
                    self.az_blob_mgt.moveFileInDirectory(self.good_directory_path,self.bad_directory_path,file)
                    msg="Invalid Column Length for the file!! File moved to "+self.bad_directory_path+"file:"+file
                    self.logger_db_writer.log(log_database,log_collection,msg)
            #self.logger.log(f, "Column Length Validation Completed!!")
            self.logger_db_writer.log(log_database,log_collection,"Column Length Validation Completed!!")
            """ except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
            """
        except Exception as e:
            """
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            """
            self.logger_db_writer.log(log_database,log_collection,"Error Occured in class Raw_Data_validation method: validateColumnLength error:"+str(e) )

            raise e
        #f.close()

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            log_database="wafer_training_log"
            log_collection="missing_values_in_column"
            #f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            #self.logger.log(f,"Missing Values Validation Started!!")
            self.logger_db_writer.log(log_database,log_collection,"Missing Values Validation Started!!")

            #for file in listdir('Training_Raw_files_validated/Good_Raw/'):
            for file in self.az_blob_mgt.getAllFileNameFromDirectory(self.good_directory_path):
                """
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                """
                csv=self.az_blob_mgt.readCsvFileFromDirectory(self.good_directory_path,file,)
                count=0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        """
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        """

                        self.az_blob_mgt.moveFileInDirectory(self.good_directory_path,self.bad_directory_path,file)
                        msg = "Invalid Column Length for the file!! File moved to "+self.bad_directory_path+":: %s" % file
                        self.logger_db_writer.log(log_database,log_collection,msg)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 1": "Wafer"}, inplace=True)
                    self.az_blob_mgt.saveDataFrameTocsv(self.good_directory_path,file,csv,index=None,header=True)

                    #csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)

                """
        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
            """
        except Exception as e:
            #f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            #self.logger.log(f, "Error Occured:: %s" % e)
            #f.close()
            self.logger_db_writer.log(log_database,log_collection,"Error Occured class:Raw_Data_validation method:validateMissingValuesInWholeColumn error:"+str(e))
            raise e
        #f.close()












