namespace ReliableKafka.Consumers.KafkaFlow
{
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using global::KafkaFlow;
    using global::KafkaFlow.TypedHandler;
    using ReliableKafka.Shared;

    public class MyMessageHandler : IMessageHandler<MyMessage>
    {
        private readonly MessageProcessor processor;

        public MyMessageHandler(
            MessageProcessor processor)
        {
            this.processor = processor;
        }

        public Task Handle(IMessageContext context, MyMessage message)
        {
            var messageId = Deserializers.Int32.Deserialize(
                context.PartitionKey,
                context.PartitionKey is null,
                SerializationContext.Empty);

            return processor.ProcessAsync(messageId, message, context.Consumer.WorkerStopped);
        }
    }
}
