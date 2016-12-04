using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Message.Send
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Creating Messaging Entities...");
            CreateMessagingEntities().Wait();

            Console.WriteLine("Beginning Benchmark");
            
            // Singular Sends
            BasicSendBenchmark();
            CreateQueueClientEachMessageBenchmark();
            AsyncSendBenchmark().Wait();
            PartitionedSendBenchmark();
            PartitionedAsyncSendBenchmark().Wait();
            ExpressSendBenchmark();

            // Multiple Queue Client Sends
            MultipleQueueClientsBenchmark();

            // Concurrent Sends
            AsyncTaskListSendBenchmark().Wait();

            // Batched Sends
            BatchedSendBenchmark();
            PartitionedBatchedSendBenchmark();
            ExpressBatchedSendBenchmark();
            GranularBatchedSendBenchmark();

            Console.ReadLine();
        }

        private static async Task CreateMessagingEntities()
        {
            var ns = NamespaceManager.CreateFromConnectionString(Core.ConnectionString);

            foreach (var queue in Core.Queues())
            {
                if (!await ns.QueueExistsAsync(queue.Item1))
                    await ns.CreateQueueAsync(queue.Item2);
            }
        }

        public static void BasicSendBenchmark()
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.Basic, ReceiveMode.PeekLock);

            using (var timer = new Timer($"Send - Basic Example: {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    for (int i = 0; i < Core.SendCount; i++)
                        sender.Send(new BrokeredMessage(new MyPoco()));
                });
            }
        }

        public static void CreateQueueClientEachMessageBenchmark()
        {
            using (var timer = new Timer($"Send - Create Queue Client Each Send Example: {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    for (int i = 0; i < Core.SendCount; i++)
                    {
                        var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.CreateQueueClientEachMessage, ReceiveMode.PeekLock);

                        sender.Send(new BrokeredMessage(new MyPoco()));
                    }
                });
            }
        }

        public static async Task AsyncSendBenchmark()
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.AsyncSend);

            using (var timer = new Timer($"Send - Async Send Example: {Core.SendCount} messages."))
                await timer.Time(SendAsyncBenchmark(sender));
        }

        private static async Task SendAsyncBenchmark(QueueClient sender)
        {
            for (int i = 0; i < Core.SendCount; i++)
                await sender.SendAsync(new BrokeredMessage(new MyPoco()));
        }

        public static void PartitionedSendBenchmark()
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.PartitionedSend);

            using (var timer = new Timer($"Send - Partitioned Send Example: {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    for (int i = 0; i < Core.SendCount; i++)
                        sender.Send(new BrokeredMessage(new MyPoco()));
                });
            }
        }

        public static async Task PartitionedAsyncSendBenchmark()
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.AsyncPartitionedSend);

            using (var timer = new Timer($"Send - Async Partitioned Send Example: {Core.SendCount} messages."))
                await timer.Time(SendAsyncBenchmark(sender));
        }

        public static void ExpressSendBenchmark()
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.ExpressSend);

            using (var timer = new Timer($"Send - Express Send Example: {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    for (int i = 0; i < Core.SendCount; i++)
                        sender.Send(new BrokeredMessage(new MyPoco()));
                });
            }
        }

        public static async Task AsyncTaskListSendBenchmark()
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.AsyncTaskList);

            using (var timer = new Timer($"Send - Async Task List Send Example: {Core.SendCount} messages."))
            {
                await timer.Time(CreateTaskList(sender));
            }
        }

        private static Task CreateTaskList(QueueClient sender)
        {
            var tasks = new List<Task>();

            for (int i = 0; i < Core.SendCount; i++)
                tasks.Add(sender.SendAsync(new BrokeredMessage(new MyPoco())));

            return Task.WhenAll(tasks);
        }

        public static void MultipleQueueClientsBenchmark()
        {
            using (var timer = new Timer($"Send - Multiple Queue Client Send Example: {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    List<QueueClient> clients = new List<QueueClient>();

                    for (int i = 0; i < 10; i++)
                        clients.Add(QueueClient.CreateFromConnectionString(Core.ConnectionString, Core.MultipleQueueClients, ReceiveMode.PeekLock));

                    for (int i = 0; i < Core.SendCount; i++)
                    {
                        clients[i % 10].Send(new BrokeredMessage(new MyPoco()));
                    }
                });
            }
        }

        public static void BatchedSendBenchmark() => BatchedSend(100, Core.SendBatching, "Send Batching");

        public static void PartitionedBatchedSendBenchmark() => BatchedSend(100, Core.PartitionedSendBatching, "Partitioned Send Batching");

        public static void ExpressBatchedSendBenchmark() => BatchedSend(100, Core.ExpressSendBatching, "Express Send Batching");

        public static void GranularBatchedSendBenchmark() => BatchedSend(10, Core.SendBatching, "Granular Send Batching");

        private static void BatchedSend(int batchSize, string path, string name)
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, path);

            using (var timer = new Timer($"Send - {name} Example: {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    var batch = new List<BrokeredMessage>();

                    var batchIndex = 0;

                    for (int i = 0; i < Core.SendCount; i++)
                    {
                        batch.Add(new BrokeredMessage(new MyPoco()));
                        batchIndex++;

                        if (batchIndex == batchSize)
                        {
                            sender.SendBatch(batch);
                            batch = new List<BrokeredMessage>();
                            batchIndex = 0;
                        }
                    }
                });
            }
        }
    }
}
