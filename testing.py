from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
import flask_monitoringdashboard as dashboard
from Email.EmailHandling import EmailSender
from predictFromModel import prediction
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
import json
import re
import uuid


def trainingTest():
    try:
        az_blb_mgt = AzureBlobManagement()
        execution_id = str(uuid.uuid4())
        path = 'training-batch-files'
        train_valObj = train_validation(path, execution_id)  # object initialization
        train_valObj.train_validation()  # calling the training_validation function
        trainModelObj = trainModel(execution_id)  # object initialization
        trainModelObj.trainingModel()  # training the model for the files in the table
        bad_data_archived = "lat-" + execution_id
        directory = [container_name.name for container_name in az_blb_mgt.blob_service_client.list_containers()]
        for dir in directory:
            if re.search('^' + bad_data_archived, dir):
                bad_data_archived = dir

        file_names = az_blb_mgt.getAllFileNameFromDirectory(bad_data_archived)

        message = "Hi Team,\n\n We have listed file name which was failed to process due to validation"
        i = 0
        for file in file_names:
            i = i + 1
            message = message + "\n" + str(i) + ") " + file
        message =message+ "\n Thanks & regards\n Avnish Yadav"
        emailSender = EmailSender()
        emailSender.sendEmail(message, "Trainning failed file")
        print("Traing Completed")
    except Exception as e:
        print(str(e))


def predictionTest(path=None):
    try:
        az_blb_mgt=AzureBlobManagement()
        execution_id = str(uuid.uuid4())
        if path is None:
            path = 'prediction-batch-files'
        else:
            path=path
        pred_val = pred_validation(path, execution_id)  # object initialization

        pred_val.prediction_validation()  # calling the prediction_validation function

        pred = prediction(path, execution_id)  # object initialization

        # predicting for dataset present in database
        path, json_predictions = pred.predictionFromModel()
        prediction_location="prediction-output-file"
        file_list="prediction-output-file"
        #selecting all failed file name
        bad_data_archived="lap-"+execution_id
        directory=[container_name.name for container_name in az_blb_mgt.blob_service_client.list_containers()]
        for dir in directory:
            if re.search('^'+bad_data_archived,dir):
                bad_data_archived=dir

        file_names=az_blb_mgt.getAllFileNameFromDirectory(bad_data_archived)

        message="Hi Team,\n\n We have listed file name which was failed to process due to validation"
        i=0
        for file in file_names:
            i=i+1
            message=message+"\n"+str(i)+") "+file
        message=message +"\n Thanks & regards\n Avnish Yadav"
        emailSender=EmailSender()
        emailSender.sendEmail(message,"Prediction failed file")
        print(path,json_predictions)
    except Exception as e:
        print(str(e))


if __name__=="__main__":
    try:
        trainingTest()
        predictionTest()
    except Exception as e:
        print(str(e))