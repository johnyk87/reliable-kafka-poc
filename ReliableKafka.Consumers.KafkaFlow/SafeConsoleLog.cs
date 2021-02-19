namespace ReliableKafka.Consumers.KafkaFlow
{
    using System;
    using global::KafkaFlow;
    using Newtonsoft.Json;

    public class SafeConsoleLog : ILogHandler
    {
        public void Error(string message, Exception ex, object data)
        {
            try
            {
                var str = JsonConvert.SerializeObject(new
                {
                    Type = ex.GetType().FullName,
                    ex.Message,
                    ex.StackTrace,
                });
                Print("\nKafkaFlow: " + message + " | Data: " + JsonConvert.SerializeObject(data) + " | Exception: " + str, ConsoleColor.Red);
            }
            catch
            {
                // Ignore logging errors.
            }
        }

        public void Info(string message, object data)
        {
            try
            {
                Print("\nKafkaFlow: " + message + " | Data: " + JsonConvert.SerializeObject(data), ConsoleColor.Green);
            }
            catch
            {
                // Ignore logging errors.
            }
        }

        public void Warning(string message, object data)
        {
            try
            {
                Print("\nKafkaFlow: " + message + " | Data: " + JsonConvert.SerializeObject(data), ConsoleColor.Yellow);
            }
            catch
            {
                // Ignore logging errors.
            }
        }

        private static void Print(string message, ConsoleColor color)
        {
            var foregroundColor = (int)Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(message);
            Console.ForegroundColor = (ConsoleColor)foregroundColor;
        }
    }
}
