using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streaming.ServiceBus.Config;

namespace Orleans.Streaming.ServiceBus.Provider
{
    public class ServiceBusAdapterSender : IAsyncDisposable
    {
        private readonly SerializationManager _serializationManager;
        private readonly ServiceBusOptions _queueOptions;
        private readonly string _queueName;
        
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
        private readonly ServiceBusClient _client;
        private ServiceBusSender? _sender;

        public ServiceBusAdapterSender(ILoggerFactory loggerFactory, SerializationManager serializationManager,
            ServiceBusClient serviceBusClient,
            ServiceBusOptions queueOptions, string queueName)
        {
            _serializationManager = serializationManager;
            _queueName = queueName;
            _queueOptions = queueOptions;
            _client = serviceBusClient;
        }

        private bool IsConnected => _sender != null && !_sender.IsClosed;

        public async Task InitializeAsync()
        {
            await ServiceBusHelpers.CreateQueueFromOptionsAsync(_queueOptions, _queueName);
            await ConnectAsync();
        }

        private async Task ConnectAsync()
        {
            await _semaphore.WaitAsync();
            try
            {
                if (IsConnected) return;
                _sender = _client.CreateSender(_queueName);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task SendAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events,
            Dictionary<string, object> requestContext)
        {
            var eventList = events.Cast<object>().ToList();
            if (eventList.Count == 0)
                return;

            var batch = new ServiceBusBatchContainer(
                streamGuid,
                streamNamespace,
                eventList,
                _queueOptions.ImportRequestContext ? requestContext : null
            );

            if (!IsConnected)
                await ConnectAsync();

            await _sender!.SendMessageAsync(
                new ServiceBusMessage(_serializationManager.SerializeToByteArray(batch))
                {
                    PartitionKey = streamGuid.ToString()
                });
        }

        public async ValueTask DisposeAsync()
        {
            await _semaphore.WaitAsync();
            try
            {
                if (_sender != null)
                {
                    await _sender.CloseAsync();
                    await _sender.DisposeAsync();
                    _sender = null;
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
}