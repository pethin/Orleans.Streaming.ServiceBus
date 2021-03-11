using System;
using Orleans.Runtime;

namespace Orleans.Streaming.ServiceBus.Config
{
    public class ServiceBusOptions
    {
        [RedactConnectionString]
        public string? ConnectionString { get; set; }

        public ushort NumberOfQueues { get; set; } = 3;
        public string QueuePrefix { get; set; } = "orleans-streaming";

        public TimeSpan QueueTtl { get; set; } = TimeSpan.FromDays(1);
        public TimeSpan MessageTtl { get; set; } = TimeSpan.FromDays(1);
        public uint MaxQueueSizeMegabytes { get; set; } = 1024;
        public bool ImportRequestContext { get; set; } = false;
    }

    public class ServiceBusOptionsValidator : IConfigurationValidator
    {
        private readonly ServiceBusOptions _options;
        private readonly string _name;

        private ServiceBusOptionsValidator(ServiceBusOptions options, string name)
        {
            _options = options;
            _name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrEmpty(_options.ConnectionString))
                throw new OrleansConfigurationException(
                    $"{nameof(ServiceBusOptions)} on stream provider {this._name} is invalid. {nameof(ServiceBusOptions.ConnectionString)} is invalid");
            
            if (_options.NumberOfQueues == 0)
                throw new OrleansConfigurationException(
                    $"{nameof(ServiceBusOptions)} on stream provider {this._name} is invalid. {nameof(ServiceBusOptions.NumberOfQueues)} must be greater than 0");
        }

        public static IConfigurationValidator Create(IServiceProvider services, string name)
        {
            var options = services.GetOptionsByName<ServiceBusOptions>(name);
            return new ServiceBusOptionsValidator(options, name);
        }
    }
}