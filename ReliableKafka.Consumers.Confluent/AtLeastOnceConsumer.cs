namespace ReliableKafka.Consumers.Confluent
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;

    public abstract class AtLeastOnceConsumer<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> consumer;

        protected AtLeastOnceConsumer(
            IConsumer<TKey, TValue> consumer)
        {
            this.consumer = consumer;
        }

        public async Task ConsumeAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                // Consume is a blocking call, so to avoid blocking the current thread, control
                // will be explicitly yielded back to the caller.
                await Task.Yield();

                var consumeResult = this.consumer.Consume(cancellationToken);
                if (consumeResult.Message == null)
                {
                    return;
                }

                await this.SafeProcessAsync(consumeResult.Message, cancellationToken);

                this.consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                await SafeOnErrorAsync(ex, cancellationToken, OnConsumeErrorAsync);
            }
        }

        protected abstract Task ProcessAsync(Message<TKey, TValue> message, CancellationToken cancellationToken);

        protected virtual Task OnConsumeErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            return Task.Delay(100, cancellationToken);
        }

        protected virtual Task OnProcessErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            return Task.Delay(100, cancellationToken);
        }

        private async Task SafeProcessAsync(Message<TKey, TValue> message, CancellationToken cancellationToken)
        {
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await this.ProcessAsync(message, cancellationToken);

                    return;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    await SafeOnErrorAsync(ex, cancellationToken, OnProcessErrorAsync);
                }
            }
            while (true);
        }

        private static async Task SafeOnErrorAsync(
            Exception exception,
            CancellationToken cancellationToken,
            Func<Exception, CancellationToken, Task> errorHandler)
        {
            try
            {
                await errorHandler(exception, cancellationToken);
            }
            catch
            {
                // Ignore error handler exceptions.
            }
        }
    }
}
