namespace ReliableKafka.Processor
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class MessageProcessor
    {
        private static readonly Random Random = new Random();

        private readonly StringRepository repository;

        private int totalMessages;

        public MessageProcessor(StringRepository repository)
        {
            this.repository = repository;
            this.totalMessages = 0;
        }

        public async Task ProcessAsync(string id, string message, CancellationToken cancellationToken)
        {
            // Introduce 1% change of failure before processing.
            if (Random.Next(0, 100) == 0)
            {
                throw new Exception("Puff before");
            }

            // This is a simple way to demonstrate an exactly once processing. A real scenario may
            // require more complex management of processed messages.
            if (await this.IsProcessed(id))
            {
                Console.WriteLine($"Message \"{id}\" has already been processed (total = {totalMessages}).");
                return;
            }

            cancellationToken.ThrowIfCancellationRequested();

            await this.repository.UpsertAsync(id, message);
            Interlocked.Increment(ref totalMessages);
            Console.WriteLine($"Message \"{id}\" received with value \"{message}\" (total = {totalMessages}).");

            // Introduce 1% change of failure after processing.
            if (Random.Next(0, 100) == 0)
            {
                throw new Exception("Puff after");
            }
        }

        private async Task<bool> IsProcessed(string id)
        {
            var currentValue = await this.repository.GetAsync(id);

            return currentValue != null;
        }
    }
}
