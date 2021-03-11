# Service Bus Provider for Orleans Streaming

Usage
-----
```cs
public class SiloBuilderConfigurator : IHostConfigurator
{
    public void Configure(IHostBuilder hostBuilder)
        => hostBuilder
            .UseOrleans(builder =>
            {
                builder
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddServiceBusStreams("ServiceBus", streamConfigurator =>
                    {
                        streamConfigurator.Configure(options =>
                        {
                            options.ConnectionString = "ConnectionString";
                        });
                    })
            });
}

public class ClientBuilderConfigurator : IClientBuilderConfigurator
{
    public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        => clientBuilder
            .AddServiceBusStreams("ServiceBus", streamConfigurator =>
            {
                streamConfigurator.Configure(options =>
                {
                    options.ConnectionString = "ConnectionString";
                });
            })
}
```