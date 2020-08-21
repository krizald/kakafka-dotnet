using System;
using System.Threading.Tasks;

namespace KafkaConsumerSample
{
    class Program
    {
        private const string topic = "DevOps";
        public static async Task Main(string[] args)
        {
            Console.Title = "Kafka Sample Consumer";
            Console.WriteLine("Kafka Sample Consumer");
            SampleConsumer consumer = new SampleConsumer("kafka:9092", "test-group");
            await consumer.SubscribeAsync(topic, Console.WriteLine);
        }
    }
}
