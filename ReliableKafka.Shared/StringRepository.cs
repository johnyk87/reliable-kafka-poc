namespace ReliableKafka.Shared
{
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    public class StringRepository
    {
        private readonly ConcurrentDictionary<int, MyMessage> values;

        public StringRepository()
        {
            this.values = new ConcurrentDictionary<int, MyMessage>();
        }

        public Task<MyMessage> GetAsync(int key)
        {
            return this.values.TryGetValue(key, out var value)
                ? Task.FromResult(value)
                : Task.FromResult<MyMessage>(null);
        }

        public Task UpsertAsync(int key, MyMessage value)
        {
            this.values.AddOrUpdate(key, value, (_, __) => value);

            return Task.CompletedTask;
        }
    }
}
