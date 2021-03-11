using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streaming.ServiceBus.Config;
using Orleans.Streams;

namespace Orleans.Streaming.ServiceBus.Provider
{
    public class ServiceBusAdapterFactory : IQueueAdapterFactory
    {
        private readonly string _providerName;
        private readonly ServiceBusOptions _options;
        private readonly ClusterOptions _clusterOptions;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IServiceBusStreamQueueMapper _streamQueueMapper;
        private readonly IQueueAdapterCache _adapterCache;

        protected SerializationManager SerializationManager { get; }
        
        /// <summary>
        /// Application level failure handler override.
        /// </summary>
        protected Func<QueueId, Task<IStreamFailureHandler>>? StreamFailureHandlerFactory { private get; set; }
        
        public ServiceBusAdapterFactory(
            ILoggerFactory loggerFactory,
            string name,
            ServiceBusOptions options,
            SimpleQueueCacheOptions cacheOptions,
            IOptions<ClusterOptions> clusterOptions,
            SerializationManager serializationManager)
        {
            _providerName = name;
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _clusterOptions = clusterOptions.Value;
            SerializationManager = serializationManager ?? throw new ArgumentNullException(nameof(serializationManager));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _streamQueueMapper = new ServiceBusStreamQueueMapper(options, $"{options.QueuePrefix}-{name}");
            _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _providerName, _loggerFactory);
        }

        public virtual void Init()
        {
            StreamFailureHandlerFactory ??= _ => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
        }
        
        public async Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new ServiceBusAdapter(
                SerializationManager,
                _streamQueueMapper,
                _loggerFactory,
                _options,
                _clusterOptions.ServiceId,
                _providerName);
            await adapter.Initialize();
            return adapter;
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _streamQueueMapper;
        }

        public Task<IStreamFailureHandler>? GetDeliveryFailureHandler(QueueId queueId)
        {
            return StreamFailureHandlerFactory?.Invoke(queueId);
        }
        
        public static ServiceBusAdapterFactory Create(IServiceProvider services, string name)
        {
            var serviceBusOptions = services.GetOptionsByName<ServiceBusOptions>(name);
            var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
            var clusterOptions = services.GetProviderClusterOptions(name);
            var factory = ActivatorUtilities.CreateInstance<ServiceBusAdapterFactory>(services, name, serviceBusOptions, cacheOptions, clusterOptions);
            factory.Init();
            return factory;
        }
    }
}