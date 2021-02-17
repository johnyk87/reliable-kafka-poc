namespace ReliableKafka.Processor
{
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    public class StringRepository
    {
        private readonly ConcurrentDictionary<string, string> values;

        public StringRepository()
        {
            this.values = new ConcurrentDictionary<string, string>();
        }

        public Task<string> GetAsync(string key)
        {
            return this.values.TryGetValue(key, out var value)
                ? Task.FromResult(value)
                : Task.FromResult<string>(null);
        }

        public Task UpsertAsync(string key, string value)
        {
            this.values.AddOrUpdate(key, value, (_, __) => value);

            return Task.CompletedTask;
        }
    }
}
