namespace ReliableKafka.Consumers.Confluent
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using ReliableKafka.Processor;

    public class MyConsumer : AtLeastOnceConsumer<string, string>
    {
        private readonly MessageProcessor processor;

        public MyConsumer(
            IConsumer<string, string> consumer,
            MessageProcessor processor)
            : base(consumer)
        {
            this.processor = processor;
        }

        protected override Task ProcessAsync(Message<string, string> message, CancellationToken cancellationToken)
        {
            return this.processor.ProcessAsync(message.Key, message.Value, cancellationToken);
        }

        protected override Task OnConsumeErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            Console.WriteLine("Consume error:" + exception);

            // Wait a bit before next attempt.
            return Task.Delay(100, cancellationToken);
        }

        protected override Task OnProcessErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            Console.WriteLine("Process error:" + exception);

            // Wait a bit before next attempt.
            return Task.Delay(100, cancellationToken);
        }
    }
}
