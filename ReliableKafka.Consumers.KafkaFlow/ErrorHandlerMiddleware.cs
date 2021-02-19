namespace ReliableKafka.Consumers.KafkaFlow
{
    using System.Threading.Tasks;
    using global::KafkaFlow;

    public class ErrorHandlerMiddleware : IMessageMiddleware
    {
        public async Task Invoke(IMessageContext context, MiddlewareDelegate next)
        {
            try
            {
                await next(context);
            }
            catch
            {
                // Muttley, do something!

                throw;
            }
        }
    }
}
