using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streaming.ServiceBus.Provider;

namespace Orleans.Streaming.ServiceBus.Config
{
    public interface IServiceBusStreamConfigurator : INamedServiceConfigurator { }
    
    public interface ISiloServiceBusStreamConfigurator : IServiceBusStreamConfigurator, ISiloPersistentStreamConfigurator { }
    
    public class SiloServiceBusStreamConfigurator : SiloPersistentStreamConfigurator, ISiloServiceBusStreamConfigurator
    {
        public SiloServiceBusStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate,
            Action<Action<IApplicationPartManager>> configureAppPartsDelegate)
            : base(name, configureServicesDelegate, ServiceBusAdapterFactory.Create)
        {
            configureAppPartsDelegate(ServiceBusStreamConfiguratorCommon.AddParts);
            this.ConfigureComponent(ServiceBusOptionsValidator.Create);
            this.ConfigureComponent(SimpleQueueCacheOptionsValidator.Create);
        }
    }
    
    public interface IClusterClientServiceBusStreamConfigurator : IServiceBusStreamConfigurator, IClusterClientPersistentStreamConfigurator { }

    public class ClusterClientServiceBusStreamConfigurator : ClusterClientPersistentStreamConfigurator, IClusterClientServiceBusStreamConfigurator
    {
        public ClusterClientServiceBusStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, ServiceBusAdapterFactory.Create)
        {
            builder.ConfigureApplicationParts(ServiceBusStreamConfiguratorCommon.AddParts);
            this.ConfigureComponent(ServiceBusOptionsValidator.Create);
        }
    }

    
    public static class ServiceBusStreamConfiguratorExtensions
    {
        public static void ConfigureServiceBus(this IServiceBusStreamConfigurator configurator, Action<OptionsBuilder<ServiceBusOptions>> configureOptions)
        {
            configurator.Configure(configureOptions);
        }
    }
    
    public static class ServiceBusStreamConfiguratorCommon
    {
        public static void AddParts(IApplicationPartManager parts)
        {
            parts.AddFrameworkPart(typeof(ServiceBusAdapterFactory).Assembly);
        }
    }
}