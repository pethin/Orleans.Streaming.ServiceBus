using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Orleans.Streaming.ServiceBus.Config;

namespace Orleans.Streaming.ServiceBus.Provider
{
    public static class ServiceBusHelpers
    {
        public static async Task CreateQueueFromOptionsAsync(ServiceBusOptions options, string queueName)
        {
            var adminClient = new ServiceBusAdministrationClient(options.ConnectionString);
            try
            {
                await adminClient.CreateQueueAsync(new CreateQueueOptions(queueName)
                {
                    EnablePartitioning = true,
                    RequiresSession = false,
                    EnableBatchedOperations = true,
                    AutoDeleteOnIdle = options.QueueTtl,
                    DefaultMessageTimeToLive = options.MessageTtl,
                    MaxSizeInMegabytes = options.MaxQueueSizeMegabytes
                });
            }
            catch (ServiceBusException exception)
                when (exception.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
            {
                // Ignore
            }
        }
    }
}