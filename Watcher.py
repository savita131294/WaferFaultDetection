import asyncio
import json
import re
import nest_asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement
import pandas
import testing
connection_string="DefaultEndpointsProtocol=https;AccountName=predictionlocation;AccountKey=V9VbOgYmDmbYH5ACyNy5vIACtfPXX4P5D4u+nkUnjWpOO32SP4wwRcsfRUefq3Rv94PmSTOZugT7ahtzyyV9Pw==;EndpointSuffix=core.windows.net"

def getEventAndSubject(data):
    event_type = None
    container = None
    if 'eventType' in data.keys():
        event_type = data['eventType']
    if 'subject' in data.keys():
        start_index = data['subject'].index('containers') + len('containers') + 1
        stop_index = data['subject'].index('/blobs/', start_index, )
        container = data['subject'][start_index:stop_index]
    if container=='avnish-yadav':
        if event_type=='Microsoft.Storage.BlobCreated':
            azm=AzureBlobManagement(connection_string)
            azm_processing_dir=AzureBlobManagement()
            file_names=azm.getAllFileNameFromDirectory(directory_name=container)
            file_names=list(filter(lambda filename: filename.split(".")[-1] == 'csv', file_names))
            if len(file_names) > 0:
                is_created=azm_processing_dir.createDirectory("received-prediction",is_replace=True)
                if is_created==True:
                    for file in file_names:
                        df=azm.readCsvFileFromDirectory(container,file)
                        azm_processing_dir.saveDataFrameTocsv("received-prediction",file,df)
                    for file in file_names:
                        azm.moveFileInDirectory(container,"recycle-bin",file)
                    testing.predictionTest("received-prediction")
                    print(event_type, container)


def updateSingleQuote(text):
    p = re.compile('(?<!\\\\)\'')
    text = p.sub('\"', text)
    return text

def messgae(data):
    data = list(json.loads(updateSingleQuote(data)))
    print("--------------------------------------")
    if len(data) > 0:
        getEventAndSubject(data[0])


async def on_event(partition_context, event):
    # Print the event data.
    event_data = event.body_as_str(encoding='UTF-8')
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event_data,
                                                                                 partition_context.partition_id))
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    messgae(event_data)
    await partition_context.update_checkpoint(event)


async def main():
    connection_str = "Endpoint=sb://avnish327030.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=BwK0zozJe+Oz46F5BgpHUtq5zkzqMKCzPdc2kOg2WW8="
    event_hubname = "waferpredictionevent"

    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=predictionlocation;AccountKey=V9VbOgYmDmbYH5ACyNy5vIACtfPXX4P5D4u+nkUnjWpOO32SP4wwRcsfRUefq3Rv94PmSTOZugT7ahtzyyV9Pw==;EndpointSuffix=core.windows.net",
        "avnish")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group="$Default",
                                                           eventhub_name=event_hubname,
                                                           checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event, starting_position="-1")


if __name__ == '__main__':
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


