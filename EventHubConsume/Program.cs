using System;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using System.Linq;
namespace EventHubConsume
{
    class Program
    {


        private const string EventHubConnectionString = "";
        private const string EventHubName = "";
        private const string StorageContainerName = "";
        private const string StorageAccountName = "";
        private const string StorageAccountKey = "";

        private static readonly string StorageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", StorageAccountName, StorageAccountKey);

        static void Main(string[] args)
        {
            Console.WriteLine("Start");
            MainAsync(args).GetAwaiter().GetResult();
            Console.WriteLine("Finished");
        }

        private static async Task MainAsync(string[] args)
        {
            Console.WriteLine("Registering EventProcessor...");

            var eventProcessorHost = new EventProcessorHost(
                EventHubName,
                PartitionReceiver.DefaultConsumerGroupName,
                EventHubConnectionString,
                StorageConnectionString,
                StorageContainerName);

            // Registers the Event Processor Host and starts receiving messages
            await eventProcessorHost.RegisterEventProcessorAsync<SimpleEventProcessor>();

            Console.WriteLine("Receiving. Press ENTER to stop worker.");
            Console.ReadLine();

            // Disposes of the Event Processor Host
            await eventProcessorHost.UnregisterEventProcessorAsync();
        }
    }

    public class SimpleEventProcessor: IEventProcessor
    {
        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"Processor Shutting Down. Partition '{context.PartitionId}', Reason: '{reason}'.");
            return Task.CompletedTask;
        }

        public Task OpenAsync(PartitionContext context)
        {
            Console.WriteLine($"SimpleEventProcessor initialized. Partition: '{context.PartitionId}'");
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Console.WriteLine($"Error on Partition: {context.PartitionId}, Error: {error.Message}");
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                var data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                if (data.ToLower().Contains("eventtype") && data.ToLower().Contains("id"))
                {
                    var temp = JsonConvert.DeserializeObject<List<DataObj>>(data);

                    foreach (var item in temp)
                    {
                        Console.WriteLine($"Message received. Partition: '{context.PartitionId}', Data: EventType : '{item.eventType}' , Message : {item.data}");
                        // Console.WriteLine($"Message received. Partition: '{context.PartitionId}', Data: '{data}'");
                    }
                }
                else
                {
                    Console.WriteLine($"Message received. Partition: '{context.PartitionId}', Data: : '{data}'");
                }

                
            }

            return context.CheckpointAsync();
        }
    }

    class DataObj
    {
        [JsonProperty("eventType")]
        public string eventType { get; set; }
        [JsonProperty("data")]
        public string data { get; set; }
    }
}
