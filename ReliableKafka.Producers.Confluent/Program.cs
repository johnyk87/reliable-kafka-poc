namespace ReliableKafka.Producers.Confluent
{
    using System;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using ReliableKafka.Shared;

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

            var producer = new ProducerBuilder<int, MyMessage>(config)
                .SetValueSerializer(new JsonSerializer<MyMessage>())
                .Build();

            for (var index = 0; index < 10000; ++index)
            {
                var message = new Message<int, MyMessage>
                {
                    Key = index,
                    Value = new MyMessage
                    {
                        Value = Guid.NewGuid().ToString(),
                    },
                };

                await producer.ProduceAsync(Topic, message);
                Console.WriteLine($"Message {index} produced");
            }
        }
    }
}
