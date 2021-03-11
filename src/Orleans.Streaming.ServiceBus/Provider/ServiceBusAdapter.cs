using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streaming.ServiceBus.Config;
using Orleans.Streams;

namespace Orleans.Streaming.ServiceBus.Provider
{
    internal class ServiceBusAdapter : IQueueAdapter, IAsyncDisposable
    {
        protected readonly string ServiceId;
        protected readonly ServiceBusOptions QueueOptions;

        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
        protected ServiceBusClient? Client;

        protected readonly ConcurrentDictionary<QueueId, ServiceBusAdapterSender> Queues =
            new ConcurrentDictionary<QueueId, ServiceBusAdapterSender>();

        private readonly SerializationManager _serializationManager;
        private readonly IServiceBusStreamQueueMapper _streamQueueMapper;
        private readonly ILoggerFactory _loggerFactory;

        public string Name { get; }
        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public ServiceBusAdapter(
            SerializationManager serializationManager,
            IServiceBusStreamQueueMapper streamQueueMapper,
            ILoggerFactory loggerFactory,
            ServiceBusOptions queueOptions,
            string serviceId,
            string providerName)
        {
            _serializationManager = serializationManager;
            QueueOptions = queueOptions;
            ServiceId = serviceId;
            Name = providerName;
            _streamQueueMapper = streamQueueMapper;
            _loggerFactory = loggerFactory;
        }

        private bool IsConnected => Client != null && !Client.IsClosed;

        public async Task Initialize()
        {
            await ConnectAsync();
        }
        
        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            if (!IsConnected)
                ConnectAsync().Wait();

            return new ServiceBusAdapterReceiver(_serializationManager, _loggerFactory, Client!, QueueOptions,
                _streamQueueMapper.QueueIdToQueueName(queueId));
        }

        public async Task QueueMessageBatchAsync<T>(
            Guid streamGuid,
            string streamNamespace,
            IEnumerable<T> events,
            StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            if (token != null)
                throw new ArgumentException(
                    "ServiceBus stream provider does not support non-null StreamSequenceToken.",
                    nameof(token));
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            if (!Queues.TryGetValue(queueId, out var queue))
            {
                if (!IsConnected)
                    await ConnectAsync();

                var tmpQueue = new ServiceBusAdapterSender(_loggerFactory, _serializationManager, Client!, QueueOptions,
                    _streamQueueMapper.QueueIdToQueueName(queueId));
                await tmpQueue.InitializeAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }

            await queue.SendAsync(streamGuid, streamNamespace, events, requestContext);
        }

        private async Task ConnectAsync()
        {
            await _semaphore.WaitAsync();
            try
            {
                if (!IsConnected)
                {
                    Client = new ServiceBusClient(QueueOptions.ConnectionString);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Client != null)
            {
                await Client.DisposeAsync();
                Client = null;
            }
        }
    }
}