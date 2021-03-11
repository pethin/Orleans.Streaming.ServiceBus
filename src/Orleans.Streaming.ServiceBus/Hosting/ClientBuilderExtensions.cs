using System;
using Microsoft.Extensions.Options;
using Orleans.Streaming.ServiceBus.Config;

namespace Orleans.Streaming.ServiceBus.Hosting
{
    public static class ClientBuilderExtensions
    {
        /// <summary>
        /// Configure cluster client to use Service Bus persistent streams.
        /// </summary>
        public static IClientBuilder AddServiceBusStreams(this IClientBuilder builder,
            string name,
            Action<ClusterClientServiceBusStreamConfigurator> configure)
        {
            // The constructor wires up DI with Service Bus Stream, so it has to be called regardless configure is null or not
            var configurator = new ClusterClientServiceBusStreamConfigurator(name, builder);
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure cluster client to use Service Bus persistent streams.
        /// </summary>
        public static IClientBuilder AddServiceBusStreams(this IClientBuilder builder,
            string name, Action<OptionsBuilder<ServiceBusOptions>> configureOptions)
        {
            builder.AddServiceBusStreams(name, b => b.ConfigureServiceBus(configureOptions));
            return builder;
        }
    }
}