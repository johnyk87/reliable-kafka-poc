namespace ReliableKafka.Consumers.Confluent
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using ReliableKafka.Shared;

    public class MyConsumer : AtLeastOnceConsumer<int, MyMessage>
    {
        private readonly MessageProcessor processor;

        public MyConsumer(
            IConsumer<int, MyMessage> consumer,
            MessageProcessor processor)
            : base(consumer)
        {
            this.processor = processor;
        }

        protected override Task ProcessAsync(Message<int, MyMessage> message, CancellationToken cancellationToken)
        {
            return this.processor.ProcessAsync(message.Key, message.Value, cancellationToken);
        }

        protected override Task OnConsumeErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            Console.WriteLine("Consume error:" + exception);

            if (exception is ConsumeException consumeException)
            {
                if (consumeException.Error.Code == ErrorCode.Local_KeyDeserialization ||
                    consumeException.Error.Code == ErrorCode.Local_ValueDeserialization)
                {
                    // Now what?
                    Console.WriteLine("Deserialization error.");
                }
            }

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
