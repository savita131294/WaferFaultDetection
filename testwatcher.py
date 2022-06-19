import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore



async def on_event(partition_context, event):
    # Print the event data.
    data_event=event.body_as_str(encoding='UTF-8')
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(data_event,
                                                                                 partition_context.partition_id))
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.

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
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())


# In[2]:
