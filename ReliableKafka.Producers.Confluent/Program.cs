namespace ReliableKafka.Producers.Confluent
{
    using System;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;

    public static class Program
    {
        public static async Task Main()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                // In order to ensure idempotence, this setting will change the defaults of other
                // settings as well, like Acks.All and maxRetries = int.MaxValue.
                EnableIdempotence = true,
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

                await producer.ProduceAsync(Topic, message);
                Console.WriteLine($"Message {index} produced");
            }
        }
    }
}
