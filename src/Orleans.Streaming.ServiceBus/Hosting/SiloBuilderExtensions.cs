using System;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Streaming.ServiceBus.Config;

namespace Orleans.Streaming.ServiceBus.Hosting
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use Service Bus persistent streams.
        /// </summary>
        public static ISiloHostBuilder AddServiceBusStreams(this ISiloHostBuilder builder, string name,
            Action<SiloServiceBusStreamConfigurator> configure)
        {
            var configurator = new SiloServiceBusStreamConfigurator(name,
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure silo to use Service Bus persistent streams with default settings
        /// </summary>
        public static ISiloHostBuilder AddServiceBusStreams(this ISiloHostBuilder builder, string name,
            Action<OptionsBuilder<ServiceBusOptions>> configureOptions)
        {
            builder.AddServiceBusStreams(name, b => b.ConfigureServiceBus(configureOptions));
            return builder;
        }

        /// <summary>
        /// Configure silo to use Service Bus persistent streams.
        /// </summary>
        public static ISiloBuilder AddServiceBusStreams(this ISiloBuilder builder, string name,
            Action<SiloServiceBusStreamConfigurator> configure)
        {
            var configurator = new SiloServiceBusStreamConfigurator(name,
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure silo to use Service Bus persistent streams with default settings
        /// </summary>
        public static ISiloBuilder AddServiceBusStreams(this ISiloBuilder builder, string name,
            Action<OptionsBuilder<ServiceBusOptions>> configureOptions)
        {
            builder.AddServiceBusStreams(name, b => b.ConfigureServiceBus(configureOptions));
            return builder;
        }
    }
}