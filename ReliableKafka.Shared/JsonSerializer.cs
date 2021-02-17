namespace ReliableKafka.Shared
{
    using System;
    using Confluent.Kafka;
    using Newtonsoft.Json;

    public class JsonSerializer<T> : ISerializer<T>, IDeserializer<T>
    {
        // ReSharper disable once StaticMemberInGenericType
        private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
        };

        public byte[] Serialize(T data, SerializationContext context)
        {
            return Serializers.Utf8.Serialize(
                JsonConvert.SerializeObject(data, Settings),
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
