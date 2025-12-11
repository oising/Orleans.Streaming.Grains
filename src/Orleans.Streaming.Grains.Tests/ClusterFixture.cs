// <copyright file="ClusterFixture.cs" company="Surveily Sp. z o.o.">
// Copyright (c) Surveily Sp. z o.o.. All rights reserved.
// </copyright>

#nullable enable

using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Orleans.Streaming.Grains.Tests.Streams.Messages;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Streaming.Grains.Test;

/// <summary>
/// Base class for cluster fixtures that manages TestCluster lifecycle.
/// Shared across multiple test classes via xUnit Collection Fixtures.
/// Uses InProcessTestClusterBuilder for cleaner, more efficient testing.
/// </summary>
public abstract class ClusterFixture : IAsyncLifetime
{
    private InProcessTestCluster? _cluster;

    protected ClusterFixture()
    {
    }

    public InProcessTestCluster Cluster => _cluster!;

    public IClusterClient Client => Cluster.Client;

    public IServiceProvider Container => Cluster.Silos.First().ServiceProvider;

    public Mock<IProcessor>? Processor => Container.GetService<Mock<IProcessor>>();

    public async ValueTask InitializeAsync()
    {
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureSilo((options, siloBuilder) => ConfigureSilo(siloBuilder));

        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_cluster != null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    public async Task WaitFor(Func<object?> subject)
    {
        await WaitFor(subject, TimeSpan.FromSeconds(15));
    }

    public async Task WaitFor(Func<object?> subject, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();

        try
        {
            while (subject() == null)
            {
                if (sw.Elapsed > timeout)
                {
                    throw new TimeoutException($"Timeout while waiting for subject.");
                }

                await Task.Delay(100);
            }
        }
        finally
        {
            sw.Stop();
        }
    }

    public async Task WaitForState(Func<Task<bool>> condition)
    {
        await WaitForState(condition, TimeSpan.FromSeconds(15));
    }

    public async Task WaitForState(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();

        try
        {
            while (!await condition())
            {
                if (sw.Elapsed > timeout)
                {
                    throw new TimeoutException($"Timeout while waiting for state condition.");
                }

                await Task.Delay(100);
            }
        }
        finally
        {
            sw.Stop();
        }
    }

    protected abstract void ConfigureSilo(ISiloBuilder siloBuilder);
}

/// <summary>
/// Fixture for tests that use non-wait mode (fireAndForget = false in BaseGrainTestConfig).
/// </summary>
public class DefaultClusterFixture : ClusterFixture
{
    private readonly Mock<IProcessor> _processor = new ();

    protected override void ConfigureSilo(ISiloBuilder siloBuilder)
    {
        var config = new DefaultConfig(_processor);
        config.Configure(siloBuilder);
    }

    private class DefaultConfig(Mock<IProcessor> processor) : BaseGrainTestConfig(false)
    {
        public override void Configure(IServiceCollection services)
        {
            services.AddSingleton(processor);
            services.AddSingleton(processor.Object);
        }
    }
}

/// <summary>
/// Fixture for tests that use wait/fire-and-forget mode (fireAndForget = true in BaseGrainTestConfig).
/// </summary>
public class WaitClusterFixture : ClusterFixture
{
    private readonly Mock<IProcessor> _processor = new ();

    protected override void ConfigureSilo(ISiloBuilder siloBuilder)
    {
        var config = new WaitConfig(_processor);
        config.Configure(siloBuilder);
    }

    private class WaitConfig(Mock<IProcessor> processor) : BaseGrainTestConfig(true)
    {
        public override void Configure(IServiceCollection services)
        {
            services.AddSingleton(processor);
            services.AddSingleton(processor.Object);
        }
    }
}

/// <summary>
/// Fixture for transaction tests that don't need the processor mock.
/// </summary>
public class TransactionClusterFixture : ClusterFixture
{
    protected override void ConfigureSilo(ISiloBuilder siloBuilder)
    {
        var config = new TransactionConfig();
        config.Configure(siloBuilder);
    }

    private class TransactionConfig : BaseGrainTestConfig
    {
        public TransactionConfig()
            : base(false)
        {
        }

        public override void Configure(IServiceCollection services)
        {
        }
    }
}

/// <summary>
/// Collection definition for tests using the default cluster (non-wait mode).
/// </summary>
[CollectionDefinition(Name)]
public class DefaultClusterCollection : ICollectionFixture<DefaultClusterFixture>
{
    public const string Name = "DefaultCluster";
}

/// <summary>
/// Collection definition for tests using wait mode cluster.
/// </summary>
[CollectionDefinition(Name)]
public class WaitClusterCollection : ICollectionFixture<WaitClusterFixture>
{
    public const string Name = "WaitCluster";
}

/// <summary>
/// Collection definition for transaction tests.
/// </summary>
[CollectionDefinition(Name)]
public class TransactionClusterCollection : ICollectionFixture<TransactionClusterFixture>
{
    public const string Name = "TransactionCluster";
}
