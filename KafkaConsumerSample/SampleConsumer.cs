using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumerSample
{
    public class SampleConsumer
    {
        protected string strServer;
        protected string strGroupId;
        public SampleConsumer(string ip,string groupId)
        {
            strServer = ip;
            strGroupId = groupId;
        }
        public Task SubscribeAsync(string topic, Action<string> message)
        {
            var consumerConfig = new ConsumerConfig
            {
                GroupId = strGroupId,
                BootstrapServers = strServer,
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                //var totalCount = 0;
                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        //totalCount += JObject.Parse(cr.Message.Value).Value<int>("count");
                        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss")}: Consumed record with key {cr.Message.Key} and value {cr.Message.Value}");
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C was pressed.
                }
                finally
                {
                    consumer.Close();
                }
            }

            return Task.CompletedTask;
        }
    }
}
