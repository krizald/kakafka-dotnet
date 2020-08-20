using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TestKafka
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            //await SampleProducer("192.168.1.17:9092", "DevOps", "test12345");
            //await SampleConsumer("192.168.1.17:9092", "DevOps");
            // await SampleProducer("localhost:9092", "DevOps", "test12345");
            // await SampleConsumer("localhost:9092", "DevOps");
            await SampleProducer("kafka:9092", "DevOps", "test1234545");
            await SampleConsumer("kafka:9092", "DevOps");
        }

        private static async Task SampleProducer(string ip, string topic, string message)
        {
            var config = new ProducerConfig { BootstrapServers = ip };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var dr = await p.ProduceAsync(topic, new Message<Null, string> { Value = message });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

        private static async Task SampleConsumer(string ip, string topic)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = ip,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
