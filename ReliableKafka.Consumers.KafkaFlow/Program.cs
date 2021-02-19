namespace ReliableKafka.Consumers.KafkaFlow
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.DependencyInjection;
    using ReliableKafka.Shared;
    using global::KafkaFlow;
    using global::KafkaFlow.Serializer;
    using global::KafkaFlow.Serializer.NewtonsoftJson;
    using global::KafkaFlow.TypedHandler;

    public static class Program
    {
        public static async Task Main()
        {
            var bootstrapServers = new[] { "localhost:9092" };

            var serviceProvider = new ServiceCollection()
                .AddSingleton<StringRepository>()
                .AddSingleton<MessageProcessor>()
                .AddKafka(kafkaBuilder => kafkaBuilder
                    .UseLogHandler<SafeConsoleLog>()
                    .AddCluster(clusterBuilder => clusterBuilder
                        .WithBrokers(bootstrapServers)
                        .AddConsumer(consumerBuilder => consumerBuilder
                            .Topic("resilience-tests")
                            // The order of middlewares is important. With the current order, serialization errors will
                            // be ignored, and at-least-once will be enforced at the processing level only.
                            .AddMiddlewares(middlewareBuilder => middlewareBuilder
                                .AddSingleTypeSerializer<MyMessage, NewtonsoftJsonMessageSerializer>()
                                // This reusable middleware will ensure at-least-once semantics.
                                .Add<AtLeastOnceMiddleware>()
                                // Error handling (e.g.: logging, monitoring, etc.) should be independent from the other
                                // middlewares in order to make them more reusable.
                                .Add<ErrorHandlerMiddleware>()
                                .AddTypedHandlers(handlerBuilder => handlerBuilder
                                    .AddHandler<MyMessageHandler>()))
                            // The following settings are mandatory in KafkaFlow.
                            // A worker count of 1 will ensure that messages are processed in the same order as they
                            // exist in the Kafka partition.
                            .WithWorkersCount(1)
                            .WithBufferSize(100)
                            // The following settings are just for testing purposes to allow us to run the consumer
                            // multiple times without having to produce new messages every time.
                            .WithGroupId(Guid.NewGuid().ToString())
                            .WithAutoOffsetReset(AutoOffsetReset.Earliest))))
                .BuildServiceProvider();

            var kafkaBus = serviceProvider.CreateKafkaBus();

            var terminateSignal = new AutoResetEvent(false);
            Console.CancelKeyPress += (_, __) =>
            {
                Console.WriteLine("Ctrl+C received.");
                StopBusAsync(kafkaBus).GetAwaiter().GetResult();
                Console.WriteLine("Terminating application.");
                terminateSignal.Set();
            };

            Console.WriteLine("Press Ctrl+C to terminate...");

            await StartBusAsync(kafkaBus);

            terminateSignal.WaitOne();
        }

        private static async Task StartBusAsync(IKafkaBus bus)
        {
            Console.WriteLine("Starting Kafka bus.");
            await bus.StartAsync(default);
            Console.WriteLine("Kafka bus has been started.");
        }

        private static async Task StopBusAsync(IKafkaBus bus)
        {
            Console.WriteLine("Stopping Kakfa bus.");
            await bus.StopAsync();
            Console.WriteLine("Kafka bus has been stopped.");
        }
    }
}
