namespace ReliableKafka.Consumers.Confluent
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using Microsoft.Extensions.Hosting;
    using ReliableKafka.Shared;

    public class ConsumerService : BackgroundService
    {
        private readonly ICollection<string> bootstrapServers;
        private readonly MessageProcessor processor;

        public ConsumerService(ICollection<string> bootstrapServers, MessageProcessor processor)
        {
            this.bootstrapServers = bootstrapServers;
            this.processor = processor;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var kafkaConsumer = default(IConsumer<int, MyMessage>);

            try
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = string.Join(',', bootstrapServers),

                    // In order to guarantee only processed messages are committed, auto commit
                    // must be disabled.
                    EnableAutoCommit = false,

                    // The following settings are just for testing purposes to allow us to run the
                    // consumer multiple times without having to produce new messages every time.
                    GroupId = Guid.NewGuid().ToString(),
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                };

                kafkaConsumer = new ConsumerBuilder<int, MyMessage>(config)
                    .SetValueDeserializer(new JsonSerializer<MyMessage>())
                    .SetPartitionsAssignedHandler(OnPartitionAssigned)
                    .SetPartitionsRevokedHandler(OnPartitionRevoked)
                    .Build();

                kafkaConsumer.Subscribe("resilience-tests");

                var consumer = new MyConsumer(kafkaConsumer, this.processor);

                do
                {
                    stoppingToken.ThrowIfCancellationRequested();
                    await consumer.ConsumeAsync(stoppingToken);
                }
                while (true);
            }
            catch (OperationCanceledException)
            {
                try
                {
                    kafkaConsumer?.Close();
                }
                catch
                {
                    // Ignore close exceptions.
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void OnPartitionAssigned(IConsumer<int, MyMessage> arg1, List<TopicPartition> arg2)
        {
            Console.WriteLine("Partitions assigned: " + string.Join(',', arg2));
        }

        private static void OnPartitionRevoked(IConsumer<int, MyMessage> arg1, List<TopicPartitionOffset> arg2)
        {
            Console.WriteLine("Partitions revoked: " + string.Join(',', arg2));
        }
    }
}
