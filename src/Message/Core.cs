using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Message
{
    public class Core
    {
        public static string ConnectionString = "";
        public static string AmqpConnectionString = $"{ConnectionString};TransportType=Amqp";
        public static string AmqpSuffix = "_amqp";

        public const int SendCount = 1000;

        public const string Basic = "Basic";
        public const string CreateQueueClientEachMessage = "CreateQueueClientEachSend";
        public const string AsyncSend = "AsyncSend";
        public const string ReceivePrefetch = "ReceivePrefetchExample";
        public const string PartitionedSend = "PartitionedSend";
        public const string AsyncPartitionedSend = "AsyncPartitionedSend";
        public const string ExpressSend = "ExpressSend";
        public const string AsyncTaskList = "AsyncTaskList";
        public const string MultipleQueueClients = "MultipleQueueClients";
        public const string SendBatching = "SendBatching";
        public const string ExpressSendBatching = "ExpressSendBatching";
        public const string PartitionedSendBatching = "PartitionedSendBatching";

        public static List<Tuple<string, QueueDescription>> Queues()
        {
            return new List<Tuple<string, QueueDescription>>
            {
                CreateQueue(Basic),
                CreateQueue(CreateQueueClientEachMessage),
                CreateQueue(AsyncSend),
                CreateQueue(PartitionedSend, o => o.EnablePartitioning = true),
                CreateQueue(AsyncPartitionedSend, o => o.EnablePartitioning = true),
                CreateQueue(ExpressSend, o => o.EnableExpress = true),
                CreateQueue(AsyncTaskList),
                CreateQueue(MultipleQueueClients),
                CreateQueue(SendBatching),
                CreateQueue(PartitionedSendBatching, o => o.EnablePartitioning = true),
                CreateQueue(ExpressSendBatching, o => o.EnableExpress = true)
            };
        }

        private static Tuple<string, QueueDescription> CreateQueue(string path, Action<QueueDescription> configure = null)
        {
            var queueDescription = DefaultQueueDescription(path);

            if (configure != null) configure(queueDescription);

            return new Tuple<string, QueueDescription>(path, queueDescription);
        }

        public static QueueDescription DefaultQueueDescription(string path) => new QueueDescription(path)
        {
            DefaultMessageTimeToLive = TimeSpan.FromMinutes(5)
        };
    }

    public class MyPoco
    {
        public Guid Id = Guid.NewGuid();
        public string Name = "MyMessageName";
        public DateTime Time = DateTime.Now;
    }

    public class Timer : IDisposable
    {
        private Stopwatch sw;
        private string _name;

        public Timer(string name)
        {
            _name = name;

            sw = new Stopwatch();
            Log($"Starting {_name}");
        }

        public async Task Time(Task action)
        {
            sw.Start();
            await action;
        }

        public void Time(Action act)
        {
            sw.Start();
            act();
        }

        public void Dispose()
        {
            sw.Stop();
            Log($"Finished {_name} - Total elapsed: {sw.Elapsed}.");
            sw.Reset();
        }

        private void Log(string message) => Console.WriteLine(message);
    }
}
