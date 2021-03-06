﻿namespace ReliableKafka.Shared
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

        public async Task ProcessAsync(int id, MyMessage message, CancellationToken cancellationToken)
        {
            try
            {
                ThrowRandomError("1");

                // This is a simple way to demonstrate an exactly once processing. A real scenario may
                // require more complex management of processed messages.
                if (await this.IsProcessed(id))
                {
                    Console.WriteLine($"Message \"{id}\" has already been processed (total = {this.totalMessages}).");
                    return;
                }

                await this.ProcessNewAsync(id, message);
            }
            catch (ValidationException ex)
            {
                ThrowRandomError("on validaton exception", 0.5);

                Console.WriteLine("Validation error: " + ex.Message);
            }
        }

        private async Task<bool> IsProcessed(int id)
        {
            var currentValue = await this.repository.GetAsync(id);

            ThrowRandomError("2");

            return currentValue != null;
        }

        private async Task ProcessNewAsync(int id, MyMessage message)
        {
            ThrowRandomError("3");

            Validate(id);

            ThrowRandomError("4");

            await this.repository.UpsertAsync(id, message);
            Interlocked.Increment(ref this.totalMessages);
            Console.WriteLine($"Message \"{id}\" received with value \"{message}\" (total = {this.totalMessages}).");

            ThrowRandomError("5");
        }

        private static void Validate(int id)
        {
            if (id % 1000 == 0)
            {
                throw new ValidationException($"I don't like id {id}!");
            }
        }

        private static void ThrowRandomError(string info = "somewhere", double percentage = 0.001)
        {
            if (Random.NextDouble() < percentage)
            {
                throw new Exception("Puff " + info);
            }
        }
    }
}
