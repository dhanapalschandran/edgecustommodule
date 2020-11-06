namespace IotEdgeCustomModule
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Extensions.Logging;
    using Microsoft.WindowsAzure.Storage.Blob.Protocol;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    class Program
    {
        static int counter;

        static void Main(string[] args)
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

            var messageBody = JsonConvert.DeserializeObject<InputPayload>(messageString);


            if (!string.IsNullOrEmpty(messageString))
            {
                var iotHubData = new payload()
                {
                    AssetId = messageBody.AssetId,
                    AssetType = "EdgeDevice10192010",
                    TimeStamp = DateTime.UtcNow.ToString("yyyy-MM-dd-HH:mm:ss"),
                    Parameters = new List<parameter>()
                    {
                        new parameter()
                        {
                            Parameter = $"{messageBody.AsaSource}min",
                            Rawvalue  = Math.Round((double)messageBody.MinValue, 2)
                        },
                        new parameter()
                        {
                            Parameter = $"{messageBody.AsaSource}max",
                            Rawvalue  = Math.Round((double)messageBody.MaxValue, 2)
                        },
                        new parameter()
                        {
                            Parameter = $"{messageBody.AsaSource}avg",
                            Rawvalue  = Math.Round((double)messageBody.AvgValue, 2)
                        },
                    }
                };

                //Append Alert When there it is above threshold
                if(messageBody.AvgValue > 35)
                {
                    iotHubData.Parameters.Add(new parameter()
                    {
                        Parameter = $"{messageBody.AsaSource}Alert",
                        Rawvalue = Math.Round((double)messageBody.AvgValue, 2)
                    });
                }

              
                string dataBuffer = JsonConvert.SerializeObject(iotHubData);
                var eventMessage = new Message(Encoding.UTF8.GetBytes(dataBuffer));

                eventMessage.ContentEncoding = "utf-8";
                eventMessage.ContentType = "application/json";

                try
                {
                    using (var client = DeviceClient.CreateFromConnectionString("HostName=RAMIoTHub.azure-devices.net;DeviceId=ramdevice;SharedAccessKey=oZLH7RtxUWQ39CQV/o8IEskyDQxOZ1n+KdG4Eh4O24I="))
                    {
                        await client.SendEventAsync(eventMessage);
                    }
                }catch(Exception ex)
                {
                    Console.WriteLine("Exception:", ex);
                }

                Console.WriteLine($"\t{DateTime.Now.ToLocalTime()}> Sending message: {eventMessage}, Body: [{dataBuffer}]");
                await moduleClient.SendEventAsync("output1", eventMessage);
  
            }
            return MessageResponse.Completed;
        }
    }

    public class InputPayload
    {
        public double? MinValue { get; set; }
        public double? MaxValue { get; set; }
        public double? AvgValue { get; set; }
        public string AsaSource { get; set; }
        public string AssetId { get; set; }
    }
    public class payload
    {
        [JsonProperty("TIME_STAMP")]
        public string TimeStamp { get; set; }
        [JsonProperty("ASSET_ID")]
        public string AssetId { get; set; }
        [JsonProperty("ASSETtype")]
        public string AssetType { get; set; }
        public List<parameter> Parameters { get; set; }
    }
    public class parameter
    {
        [JsonProperty("parameter")]
        public string Parameter { get; set; }
        [JsonProperty("rawvalue")]
        public double? Rawvalue { get; set; }
    }
}