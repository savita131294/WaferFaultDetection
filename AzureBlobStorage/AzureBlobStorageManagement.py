import os, uuid
import dill
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import pandas as pd
from io import StringIO
import pickle


class AzureBlobManagement:
    __connection_string = "DefaultEndpointsProtocol=https;AccountName=storageaccountrgai197de;AccountKey=o56dC8V4h6GmP1bin7a9NO0e6pUWTJrf/lzO3ogX1vFOTnodTCKbZp5VsRHfCSSHDLF8XqMKP0wDVi82eiaT7Q==;EndpointSuffix=core.windows.net"

    def __init__(self,connection_string=None):
        try:
            if connection_string is not None:
                self.__connection_string=connection_string
            else:
                self.__connection_string = AzureBlobManagement.__connection_string

            self.blob_service_client = BlobServiceClient.from_connection_string(self.__connection_string)
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
        except Exception as e:
            raise Exception("Error occured in class: AzureBlobManagement method:__init__ error: " + str(e))

    def getAllFileNameFromDirectory(self, directory_name):
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            directory_name = directory_name.lower()
            if directory_name in self.dir_list:
                container_client = self.blob_service_client.get_container_client(directory_name)
                filenames = [files.name for files in container_client.list_blobs()]
                return filenames


            else:
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:getAllFileNameFromDirectory error: Direcory not found")
        except Exception as e:
            raise Exception(
                "Error occured in class: AzureBlobManagement method:getAllFileNameFromDirectory error:" + str(e))

    def createDirectory(self, directory_name, is_replace=False):
        """
        directory_name: Name of floder (name should be in lower case)
        =======================================
        return True if direcory will be created
        """
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]

            container_name = directory_name.lower()

            if container_name not in self.dir_list:
                self.blob_service_client.create_container(container_name)
            elif is_replace and container_name in self.dir_list:
                for file in self.getAllFileNameFromDirectory(container_name):
                    self.moveFileInDirectory(container_name,"recycle-bin",file)
            else:
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:createDirectory error: Directory alredy exists try to user is replace paremeter to true to delete and recreate directory")
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            return True
        except Exception as e:
            raise Exception("Error occured in class: AzureBlobManagement method:createDirectory error: " + str(e))
    def deleteDirectory(self,directory_name):
        """

        :param directory_name: directory to be deleted
        :return: True if dirctory is deleted.
        """
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            directory_name=directory_name.lower()
            if directory_name not in self.dir_list:
                return True
            else:
                self.blob_service_client.delete_container(container=directory_name)
                return True
        except Exception as e:
            raise Exception("Error Occured in class: AzureBlobManagement method:deleteDirectory error:"+str(e))


    def deleteFileFromDirectory(self,directory_name,file_name):
        """

        :param directory_name: directory to be deleted
        :return: True if dirctory is deleted.
        """
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            directory_name=directory_name.lower()
            if directory_name not in self.dir_list:
                raise Exception("Error in class AzureBlobManagement method deleteFileFromDirectory Specified Directory not found")

            if file_name in self.getAllFileNameFromDirectory(directory_name):
                blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
                blob_client.delete_blob()
            else:
                raise Exception("Error in class AzureBlobManagement method deleteFileFromDirectory :File" +file_name+ " not exists")
            return True
        except Exception as e:
            raise Exception("Error Occured in class: AzureBlobManagement method:deleteFileFromDirectory error:"+str(e))


    def readCsvFileFromDirectory(self, directory_name, file_name,drop_unnamed_col=True):
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            directory_name = directory_name.lower()
            if directory_name not in self.dir_list:
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:readCsvFileFromDirectory error:Directory {0} not found".format(
                        directory_name))
            if file_name not in self.getAllFileNameFromDirectory(directory_name):
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:readCsvFileFromDirectory error:File {0} not found".format(
                        file_name))

            blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
            df = pd.read_csv(StringIO(blob_client.download_blob().readall().decode()))
            if drop_unnamed_col is False:
                return df.copy()
            elif "Unnamed: 0" in df.columns.to_list():
                df.drop(columns=["Unnamed: 0"], axis=1, inplace=True)
                return df.copy()
            else:
                return df.copy()
        except Exception as e:
            raise Exception(
                "Error occured in class: AzureBlobManagement method:readCsvFileFromDirectory error:" + str(e))

    def saveObject(self, directory_name, file_name, object_name):
        """
        directory_name: Directory Name
        file_name: File Name
        object: object to save  eg: any model or any class object can be saved
        -------------------------------------
        return True if Model is created
        """
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            directory_name = directory_name.lower()
            if directory_name not in self.dir_list:
                self.createDirectory(directory_name)
            if file_name in self.getAllFileNameFromDirectory(directory_name):
                blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
                blob_client.delete_blob()
            blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
            blob_client.upload_blob(dill.dumps(object_name))
            return True

        except Exception as e:
            raise Exception(
                "Error occured in class: AzureBlobManagement method:createFileForModel error:" + str(e))

    def loadObject(self, directory_name, file_name):
        """

        :param directory_name: Directory name
        :param file_name: file name
        :return: object
        """
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            directory_name = directory_name.lower()
            if directory_name not in self.dir_list:
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:loadModel error:Directory {0} not found".format(
                        directory_name))
            if file_name not in self.getAllFileNameFromDirectory(directory_name):
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:loadModel error:File {0} not found".format(
                        file_name))

            blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
            model = dill.loads(blob_client.download_blob().readall())
            return model

        except Exception as e:
            raise Exception(
                "Error occured in class: AzureBlobManagement method:loadModel error:" + str(e))

    def saveDataFrameTocsv(self, directory_name, file_name, data_frame,**kwargs):
        """
        :param directory_name: source directory name
        :param file_name: file name in which datafarme need to be save
        :param data_frame: pandas dataframe object
        :return: True if dataframe saved into azure blob
        """

        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            allowed_keys=['index','header','mode']
            self.__dict__.update((k, v) for k, v in kwargs.items() if k in allowed_keys)

            directory_name=directory_name.lower()
            if file_name.split(".")[-1] != "csv":
                file_name = file_name + ".csv"
            if directory_name not in self.dir_list:
                self.createDirectory(directory_name)
            if file_name in self.getAllFileNameFromDirectory(directory_name) and 'mode' in self.__dict__.keys():
                if self.mode == 'a+':
                    df=self.readCsvFileFromDirectory(directory_name=directory_name,file_name=file_name)
                    data_frame=df.append(data_frame)
                    if 'Unnamed: 0' in data_frame.columns:
                        data_frame=data_frame.drop(columns=['Unnamed: 0'], axis=1)

            if file_name in self.getAllFileNameFromDirectory(directory_name):
                blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
                blob_client.delete_blob()

            blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
            if "index" in self.__dict__.keys() and "header" in self.__dict__.keys():
                output = data_frame.to_csv(encoding="utf-8",index=self.index,header=self.header)
            elif "header" in self.__dict__.keys() and 'mode' in self.__dict__.keys():
                output = data_frame.to_csv(encoding="utf-8",header=self.header)
            else:
                output = data_frame.to_csv(encoding="utf-8")
            blob_client.upload_blob(output)
            return True
        except Exception as e:
            raise Exception("Error occured in class: AzureBlobManagement method:saveDataFrameTocsv error:" + str(e))

    def moveFileInDirectory(self, source_directory, target_directory, file_name):
        """

        :param source_directory: source directory name
        :param target_directory: target directory name
        :param file_name:
        :return: True if file will be move successfully
        """
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            source_directory = source_directory.lower()
            target_directory = target_directory.lower()

            if source_directory not in self.dir_list:
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:moveFileInDirectory error:Source Directory not found"+str(source_directory))
            if file_name not in self.getAllFileNameFromDirectory(source_directory):
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:moveFileInDirectory error:Source File not found"+str(file_name))

            if target_directory not in self.dir_list:
                self.createDirectory(target_directory)

            if file_name in self.getAllFileNameFromDirectory(target_directory):
                blob_client = self.blob_service_client.get_blob_client(container=target_directory, blob=file_name)
                blob_client.delete_blob()

            account_name = self.blob_service_client.account_name
            source_blob = (f"https://{account_name}.blob.core.windows.net/{source_directory}/{file_name}")
            copied_blob = self.blob_service_client.get_blob_client(target_directory, file_name)
            copied_blob.start_copy_from_url(source_blob)
            remove_blob = self.blob_service_client.get_blob_client(source_directory, file_name)
            remove_blob.delete_blob()
            return True

        except Exception as e:
            raise Exception("Error occured in class: AzureBlobManagement method:moveFileInDirectory error:" + str(e))

    def copyFileInDirectory(self, source_directory, target_directory, file_name):
        """


        :param source_directory: source directory name
        :param target_directory: target directory name
        :param file_name: file name to be copied
        :return: True if file will be copied successfully
        """
        try:
            self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
            source_directory = source_directory.lower()
            target_directory = target_directory.lower()

            if source_directory not in self.dir_list:
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:copyFileInDirectory error:Source Directory not found"+str(source_directory))
            if file_name not in self.getAllFileNameFromDirectory(source_directory):
                raise Exception(
                    "Error occured in class: AzureBlobManagement method:copyFileInDirectory error:Source File not found"+str(file_name))

            if target_directory not in self.dir_list:
                self.createDirectory(target_directory)

            if file_name in self.getAllFileNameFromDirectory(target_directory):
                blob_client = self.blob_service_client.get_blob_client(container=target_directory, blob=file_name)
                blob_client.delete_blob()
            account_name = self.blob_service_client.account_name
            source_blob = (f"https://{account_name}.blob.core.windows.net/{source_directory}/{file_name}")
            copied_blob = self.blob_service_client.get_blob_client(target_directory, file_name)
            copied_blob.start_copy_from_url(source_blob)
            return True

        except Exception as e:
            raise Exception("Error occured in class: AzureBlobManagement method:copyFileInDirectory error:" + str(e))



















