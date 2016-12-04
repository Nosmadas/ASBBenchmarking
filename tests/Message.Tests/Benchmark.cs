using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Message.Tests
{
    public interface IQueueBenchmark
    {
        Task CreateMessagingEntities();
        Task RunBenchmark();
    }

    public static class QueueHelper
    {
        public static async Task CreateQueue(QueueDescription desc)
        {
            var ns = NamespaceManager.CreateFromConnectionString(Core.ConnectionString);

            if (!await ns.QueueExistsAsync(desc.Path))
                await ns.CreateQueueAsync(desc);
        }
    }

    public class BasicQueueBenchmark : IQueueBenchmark
    {
        private const string Path = "Basic";

        public async Task CreateMessagingEntities()
        {
            QueueDescription queueDescription = new QueueDescription(Path)
            {
                DefaultMessageTimeToLive = TimeSpan.FromMinutes(5)
            };

            await QueueHelper.CreateQueue(queueDescription);
        }


    }

    public class Timer 
    {
        private Stopwatch sw = new Stopwatch();

        public async Task Time(Task action, string message)
        {
            Log.Write($"Starting {message}");
            sw.Start();
            await action;
            sw.Stop();
            Log.Write($"Finished {message} - Total elapsed: {sw.ElapsedMilliseconds}ms.");
        }

        public void Time(Action act, string message)
        {
            Log.Write(message);
            sw.Start();
            act();
            sw.Stop();
            Log.Write($"Finished {message} - Total elapsed: {sw.ElapsedMilliseconds}ms.");
        }

    }

    public static class Log
    {
        public static void Write(string message) => Debug.WriteLine(message);
    }

    public class Benchmark
    {
        const int SendCount = 100;

        const string Basic = "Basic";
        const string CreateQueueClientEachMessage = "CreateQueueClientEachMessage";
        const string Prefetch = "ReceivePrefetchExample";
        const string Partition = "Partition";

        [Fact]
        public async Task Run()
        {
            await BasicExample();
            await CreateEachMessageExample();
        }

        public async Task BasicInfrastructure()
        {

        }

        public async Task EachMessageInfrastructure()
        {
            QueueDescription queueDescription = new QueueDescription(CreateQueueClientEachMessage)
            {
                DefaultMessageTimeToLive = TimeSpan.FromMinutes(5)
            };

            await CreateQueue(queueDescription);
        }

        public async Task PrefetchInfrastructure()
        {
            QueueDescription queueDescription = new QueueDescription(Prefetch)
            {
                DefaultMessageTimeToLive = TimeSpan.FromMinutes(5),
            };

            await CreateQueue(queueDescription);
        }



        public async Task BasicExample()
        {

        }

        public async Task CreateEachMessageExample()
        {
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var receivedMessages = new List<BrokeredMessage>();
            var path = CreateQueueClientEachMessage;

            await EachMessageInfrastructure();

            var receiver = QueueClient.CreateFromConnectionString(Core.ConnectionString, path, ReceiveMode.PeekLock);

            Log($"Starting Create Queue Each Time Example: Sending {SendCount} messages");

            var timer = new Timer();

            timer.Time(() =>
            {
                receiver.OnMessage(o =>
                {
                    Log($"Received: {receivedMessages.Count}");
                    receivedMessages.Add(o);
                });

                for (int i = 0; i < SendCount; i++)
                {
                    var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, path, ReceiveMode.PeekLock);
                    sender.Send(new BrokeredMessage(new MyPoco()));
                }

                while (!cts.IsCancellationRequested)
                    if (receivedMessages.Count == SendCount) cts.Cancel();
            }, "");
        }

        public async Task PrefetchExample()
        {
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

            var receivedMessages = new List<BrokeredMessage>();

            var path = Basic;

            await BasicInfrastructure();

            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, path, ReceiveMode.PeekLock);
            var receiver = QueueClient.CreateFromConnectionString(Core.ConnectionString, path, ReceiveMode.PeekLock);

            Log($"Starting Basic Example: Sending {SendCount} messages");

            Timer.Time(() =>
            {
                receiver.OnMessage(o =>
                {
                    Log($"Received: {receivedMessages.Count}");
                    receivedMessages.Add(o);
                });

                for (int i = 0; i < SendCount; i++)
                    sender.Send(new BrokeredMessage(new MyPoco()));

                while (!cts.IsCancellationRequested)
                    if (receivedMessages.Count == SendCount) cts.Cancel();
            });
        }

        public void Log(string message) => Debug.WriteLine(message);
    }


}
