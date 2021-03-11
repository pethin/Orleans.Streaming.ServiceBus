using Orleans.Configuration;
using Orleans.Streaming.ServiceBus.Config;
using Orleans.Streams;

namespace Orleans.Streaming.ServiceBus.Provider
{
    public interface IServiceBusStreamQueueMapper : IStreamQueueMapper
    {
        public string QueueIdToQueueName(QueueId queueId);
    }

    public class ServiceBusStreamQueueMapper : HashRingBasedStreamQueueMapper, IServiceBusStreamQueueMapper
    {
        public ServiceBusStreamQueueMapper(ServiceBusOptions options, string queueNamePrefix) : base(
            GetHashRingStreamQueueMapperOptions(options), queueNamePrefix)
        {
        }

        private static HashRingStreamQueueMapperOptions GetHashRingStreamQueueMapperOptions(ServiceBusOptions options) =>
            new HashRingStreamQueueMapperOptions
            {
                TotalQueueCount = options.NumberOfQueues
            };

        public string QueueIdToQueueName(QueueId queueId)
        {
            return queueId.ToString();
        }
    }
}