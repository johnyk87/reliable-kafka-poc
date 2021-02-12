namespace ReliableKafkaConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;

    public static class Program
    {
        private static readonly Random Random = new Random();
        private static int totalMessages;
        private static readonly StringRepository Repository = new StringRepository();

        public static async Task Main()
        {
            try
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092",
                    GroupId = Guid.NewGuid().ToString(),
                    EnableAutoCommit = false,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                };

                var consumer = new ConsumerBuilder<string, string>(config)
                    .SetKeyDeserializer(Deserializers.Utf8)
                    .SetValueDeserializer(Deserializers.Utf8)
                    .SetPartitionsAssignedHandler(OnPartitionAssigned)
                    .SetPartitionsRevokedHandler(OnPartitionRevoked)
                    .Build();

                consumer.Subscribe("resilience-tests");

                while (true)
                {
                    await SafeConsumeAsync(consumer);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static void OnPartitionAssigned(IConsumer<string, string> arg1, List<TopicPartition> arg2)
        {
            Console.WriteLine("Partitions assigned: " + string.Join(',', arg2));
        }

        private static void OnPartitionRevoked(IConsumer<string, string> arg1, List<TopicPartitionOffset> arg2)
        {
            Console.WriteLine("Partitions revoked: " + string.Join(',', arg2));
        }

        private static async Task SafeConsumeAsync(IConsumer<string, string> consumer)
        {
            try
            {
                var consumeResult = consumer.Consume();
                if (consumeResult.Message == null)
                {
                    return;
                }

                await ReliableConsumeMessageAsync(consumeResult.Message);

                consumer.Commit(consumeResult);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static async Task ReliableConsumeMessageAsync(Message<string, string> message)
        {
            bool isSuccess;

            do
            {
                try
                {
                    await IdempotentConsumeMessageAsync(message);

                    isSuccess = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);

                    isSuccess = false;
                }
            }
            while (!isSuccess);
        }

        private static async Task IdempotentConsumeMessageAsync(Message<string, string> message)
        {
            // 1% change of failure.
            if (Random.Next(0, 100) == 0)
            {
                throw new Exception("puff before");
            }

            var currentValue = await Repository.GetAsync(message.Key);
            if (currentValue == null)
            {
                await Repository.UpsertAsync(message.Key, message.Value);
                Interlocked.Increment(ref totalMessages);
                Console.WriteLine($"Message \"{message.Key}\" received with value \"{message.Value}\" (total = {totalMessages}).");
            }

            // 1% change of failure.
            if (Random.Next(0, 100) == 0)
            {
                throw new Exception("puff after");
            }
        }
    }
}
