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
            CreateNetMessagingEntities().Wait();
            CreateAmqpEntities().Wait();

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

        public static async Task CreateNetMessagingEntities() => await CreateQueues(Core.ConnectionString);

        public static async Task CreateAmqpEntities() => await CreateQueues(Core.AmqpConnectionString, Core.AmqpSuffix);

        private static async Task CreateQueues(string connectionString, string suffix = null)
        {
            var ns = NamespaceManager.CreateFromConnectionString(connectionString);

            foreach (var queue in Core.Queues())
            {
                if (!await ns.QueueExistsAsync(queue.Item1 + suffix))
                    await ns.CreateQueueAsync(queue.Item2 + suffix);
            }
        }

        public static void BasicSendBenchmark()
        {
            Benchmark("Basic Example", Core.Basic, sendClient =>
            {
                for (int i = 0; i < Core.SendCount; i++)
                    sendClient.Send(new BrokeredMessage(new MyPoco()));
            });
        }

        public static void Benchmark(string name, string path, Action<QueueClient> sendMessagesAction)
        {
            var sender = QueueClient.CreateFromConnectionString(Core.ConnectionString, path);

            using (var timer = new Timer($"Send - {name}: {Core.SendCount} messages."))
                timer.Time(() => sendMessagesAction(sender));

            var amqpSender = QueueClient.CreateFromConnectionString(Core.AmqpConnectionString, path + Core.AmqpSuffix);

            using (var timer = new Timer($"Send - AMQP {name}: {Core.SendCount} messages."))
                timer.Time(() => sendMessagesAction(sender));
        }

        public static async Task BenchmarkAsync(string name, string path, Func<QueueClient, Task> sendMessagesAction)
        {
            var sender = QueueClientFactory(Core.ConnectionString, path);

            using (var timer = new Timer($"Send - {name}: {Core.SendCount} messages."))
                await timer.Time(sendMessagesAction(sender));

            var amqpSender = QueueClientFactory(Core.AmqpConnectionString, path + Core.AmqpSuffix);

            using (var timer = new Timer($"Send - AMQP {name}: {Core.SendCount} messages."))
                await timer.Time(sendMessagesAction(sender));
        }

        private static QueueClient QueueClientFactory(string connectionString, string path) => QueueClient.CreateFromConnectionString(connectionString, path);

        public static void CreateQueueClientEachMessageBenchmark()
        {
            using (var timer = new Timer($"Send - Create Queue Client Each Send Example : {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    for (int i = 0; i < Core.SendCount; i++)
                    {
                        var sender = QueueClientFactory(Core.ConnectionString, Core.CreateQueueClientEachMessage);

                        sender.Send(new BrokeredMessage(new MyPoco()));
                    }
                });
            }

            using (var timer = new Timer($"Send - AMQP Create Queue Client Each Send Example : {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    for (int i = 0; i < Core.SendCount; i++)
                    {
                        var sender = QueueClientFactory(Core.AmqpConnectionString, Core.CreateQueueClientEachMessage + Core.AmqpSuffix);

                        sender.Send(new BrokeredMessage(new MyPoco()));
                    }
                });
            }
        }

        public static async Task AsyncSendBenchmark() => await BenchmarkAsync("Async Send Example", Core.AsyncSend, SendAsyncAction);

        private static async Task SendAsyncAction(QueueClient sender)
        {
            for (int i = 0; i < Core.SendCount; i++)
                await sender.SendAsync(new BrokeredMessage(new MyPoco()));
        }

        public static void PartitionedSendBenchmark()
        {
            Benchmark("Partitioned Send Example", Core.PartitionedSend, sendClient =>
            {
                for (int i = 0; i < Core.SendCount; i++)
                    sendClient.Send(new BrokeredMessage(new MyPoco()));
            });
        }

        public static void ExpressSendBenchmark()
        {
            Benchmark("Express Send Example", Core.ExpressSend, sendClient =>
            {
                for (int i = 0; i < Core.SendCount; i++)
                    sendClient.Send(new BrokeredMessage(new MyPoco()));
            });
        }

        public static async Task PartitionedAsyncSendBenchmark() => await BenchmarkAsync("Async Partitioned Send Example", Core.AsyncPartitionedSend, SendAsyncAction);

        public static async Task AsyncTaskListSendBenchmark() => await BenchmarkAsync("Async Task List Send Example", Core.AsyncTaskList, CreateTaskList);

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

            using (var timer = new Timer($"Send - AMQP Multiple Queue Client Send Example: {Core.SendCount} messages."))
            {
                timer.Time(() =>
                {
                    List<QueueClient> clients = new List<QueueClient>();

                    for (int i = 0; i < 10; i++)
                        clients.Add(QueueClient.CreateFromConnectionString(Core.AmqpConnectionString, Core.MultipleQueueClients + Core.AmqpSuffix, ReceiveMode.PeekLock));

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
            Benchmark(name, path, sender =>
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
