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

        public const int SendCount = 1000;

        public static bool UseAmqp = true;

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
                CreateQueue(Basic, UseAmqp),
                CreateQueue(CreateQueueClientEachMessage, UseAmqp),
                CreateQueue(AsyncSend, UseAmqp),
                CreateQueue(PartitionedSend, UseAmqp, o => o.EnablePartitioning = true),
                CreateQueue(AsyncPartitionedSend, UseAmqp, o => o.EnablePartitioning = true),
                CreateQueue(ExpressSend, UseAmqp, o => o.EnableExpress = true),
                CreateQueue(AsyncTaskList, UseAmqp),
                CreateQueue(MultipleQueueClients, UseAmqp),
                CreateQueue(SendBatching, UseAmqp),
                CreateQueue(PartitionedSendBatching, UseAmqp, o => o.EnablePartitioning = true),
                CreateQueue(ExpressSendBatching,  UseAmqp, o => o.EnableExpress = true)
            };
        }

        private static Tuple<string, QueueDescription> CreateQueue(string path, bool amqp, Action<QueueDescription> configure = null)
        {
            var name = amqp ? $"{path}_amqp" : path;

            var queueDescription = DefaultQueueDescription(name);

            if (configure != null) configure(queueDescription);

            return new Tuple<string, QueueDescription>(name, queueDescription);
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
