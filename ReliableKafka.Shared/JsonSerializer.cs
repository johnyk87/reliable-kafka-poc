namespace ReliableKafka.Shared
{
    using System;
    using System.Threading;
    using Confluent.Kafka;
    using Newtonsoft.Json;

    public class JsonSerializer<T> : ISerializer<T>, IDeserializer<T>
    {
        private int messageCount;

        // ReSharper disable once StaticMemberInGenericType
        private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
        };

        public byte[] Serialize(T data, SerializationContext context)
        {
            // One in every 500 messages will have a serialization error.
            var messageString = Interlocked.Increment(ref messageCount) % 500 == 0
                ? "Bad dog!"
                : JsonConvert.SerializeObject(data, Settings);

            return Serializers.Utf8.Serialize(
                messageString,
                context);
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return JsonConvert.DeserializeObject<T>(
                Deserializers.Utf8.Deserialize(data, isNull, context),
                Settings);
        }
    }
}
