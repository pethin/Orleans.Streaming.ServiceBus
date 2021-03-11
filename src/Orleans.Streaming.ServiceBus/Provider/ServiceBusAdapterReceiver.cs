using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streaming.ServiceBus.Config;
using Orleans.Streams;

namespace Orleans.Streaming.ServiceBus.Provider
{
    /// <summary>
    /// Receives batches of messages from a single queue.
    /// </summary>
    internal class ServiceBusAdapterReceiver : IQueueAdapterReceiver, IAsyncDisposable
    {
        private ILogger _logger;
        private readonly SerializationManager _serializationManager;
        private readonly ServiceBusOptions _queueOptions;
        private readonly string _queueName;

        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
        private readonly ServiceBusClient _client;
        private ServiceBusReceiver? _receiver;

        public ServiceBusAdapterReceiver(SerializationManager serializationManager, ILoggerFactory loggerFactory,
            ServiceBusClient serviceBusClient, ServiceBusOptions queueOptions, string queueName)
        {
            _logger = loggerFactory.CreateLogger<ServiceBusAdapterReceiver>();
            _serializationManager = serializationManager;
            _queueOptions = queueOptions;
            _queueName = queueName;
            _client = serviceBusClient;
        }

        private bool IsConnected => _receiver != null && !_receiver.IsClosed;

        public async Task Initialize(TimeSpan timeout)
        {
            await ConnectAsync();
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            if (!IsConnected)
                await ConnectAsync();

            var messages = await _receiver!.ReceiveMessagesAsync(maxCount);
            messages ??= new ServiceBusReceivedMessage[0];

            return messages
                .Select(message =>
                {
                    var body = message.Body.ToArray();
                    var batchContainer = _serializationManager.DeserializeFromByteArray<ServiceBusBatchContainer>(body);
                    batchContainer.ServiceBusReceivedMessage = message;
                    batchContainer.SequenceToken = new EventSequenceTokenV2(message.SequenceNumber);

                    return (IBatchContainer) batchContainer;
                })
                .ToList();
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            var acks = new List<Task>(messages.Count);
            acks.AddRange(messages.Cast<ServiceBusBatchContainer>()
                .Select(async batch =>
                {
                    if (!IsConnected)
                        await ConnectAsync();

                    await _receiver!.CompleteMessageAsync(batch.ServiceBusReceivedMessage);
                }));

            await Task.WhenAll(acks);
        }

        private async Task ConnectAsync()
        {
            if (IsConnected) return;

            await _semaphore.WaitAsync();
            try
            {
                await ServiceBusHelpers.CreateQueueFromOptionsAsync(_queueOptions, _queueName);
                _receiver = _client.CreateReceiver(_queueName, new ServiceBusReceiverOptions
                {
                    PrefetchCount = 1024
                });
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            await _semaphore.WaitAsync();
            try
            {
                if (_receiver != null)
                {
                    await _receiver.CloseAsync();
                    await _receiver.DisposeAsync();
                }

                _receiver = null;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Shutdown(TimeSpan.MaxValue);
        }
    }
}