namespace ReliableKafkaProducer
{
    using System;
    using System.Threading.Tasks;
    using Confluent.Kafka;

    public static class Program
    {
        public static async Task Main()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
            };

            const string Topic = "resilience-tests";

            var producer = new ProducerBuilder<string, string>(config)
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(Serializers.Utf8)
                .Build();

            for (var index = 0; index < 10000; ++index)
            {
                var message = new Message<string, string>
                {
                    Key = index.ToString(),
                    Value = Guid.NewGuid().ToString(),
                };

                // TODO: create some sort of reliable scenario
                await producer.ProduceAsync(Topic, message);
                Console.WriteLine($"Message {index} produced");
            }
        }
    }
}
