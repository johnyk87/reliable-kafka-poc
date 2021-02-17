namespace ReliableKafka.Consumers.Confluent
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Hosting;
    using ReliableKafka.Processor;

    public static class Program
    {
        public static async Task Main()
        {
            // This would usually be the DI container's job.
            var bootstrapServers = new[] { "localhost:9092" };
            var repository = new StringRepository();
            var processor = new MessageProcessor(repository);
            var consumerService = new ConsumerService(bootstrapServers, processor);
            var terminateSignal = new AutoResetEvent(false);

            Console.CancelKeyPress += (_, __) =>
            {
                Console.WriteLine("Ctrl+C received.");
                StopServiceAsync(consumerService).GetAwaiter().GetResult();
                Console.WriteLine("Terminating application.");
                terminateSignal.Set();
            };

            Console.WriteLine("Press Ctrl+C to terminate...");

            await StartServiceAsync(consumerService);

            terminateSignal.WaitOne();
        }

        private static async Task StartServiceAsync(IHostedService service)
        {
            Console.WriteLine($"Starting {service.GetType().Name}.");
            await service.StartAsync(default);
            Console.WriteLine($"{service.GetType().Name} has been started.");
        }

        private static async Task StopServiceAsync(IHostedService service)
        {
            Console.WriteLine($"Stopping {service.GetType().Name}.");
            await service.StopAsync(default);
            Console.WriteLine($"{service.GetType().Name} has been stopped.");
        }
    }
}
