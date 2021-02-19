namespace ReliableKafka.Consumers.KafkaFlow
{
    using System;
    using System.Threading.Tasks;
    using global::KafkaFlow;

    public class AtLeastOnceMiddleware : IMessageMiddleware
    {
        private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromMilliseconds(100);

        private readonly ILogHandler logHandler;
        private readonly TimeSpan retryDelay;

        public AtLeastOnceMiddleware(ILogHandler logHandler)
            : this(logHandler, DefaultRetryDelay)
        {
        }

        public AtLeastOnceMiddleware(ILogHandler logHandler, TimeSpan retryDelay)
        {
            this.logHandler = logHandler;
            this.retryDelay = retryDelay;
        }

        public async Task Invoke(IMessageContext context, MiddlewareDelegate next)
        {
            var cancellationToken = context.Consumer.WorkerStopped;

            context.Consumer.ShouldStoreOffset = false;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await next(context);

                    context.Consumer.ShouldStoreOffset = true;

                    return;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    this.SafeErrorHandler("Error processing message.", ex, context);

                    // Wait before next attempt.
                    await Task.Delay(this.retryDelay, cancellationToken);
                }
            }
            while (true);
        }

        private void SafeErrorHandler(string message, Exception exception, IMessageContext context)
        {
            try
            {
                this.logHandler.Error(message, exception, context);
            }
            catch
            {
                // Ignore logging errors.
            }
        }
    }
}
