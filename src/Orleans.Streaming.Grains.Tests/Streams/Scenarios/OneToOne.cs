// <copyright file="OneToOne.cs" company="Surveily Sp. z o.o.">
// Copyright (c) Surveily Sp. z o.o.. All rights reserved.
// </copyright>

using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Orleans.Streaming.Grains.Test;
using Orleans.Streaming.Grains.Tests.Streams.Grains;
using Orleans.Streaming.Grains.Tests.Streams.Messages;
using Should;
using Xunit;

namespace Orleans.Streaming.Grains.Tests.Streams.Scenarios;

[Collection(DefaultClusterCollection.Name)]
public class When_Sending_Simple_Message_One_To_One(DefaultClusterFixture fixture) : IAsyncLifetime
{
    private string _result;
    private string _expected = "text";

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();
        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x => _result = x);

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
            await grain.SendAsync(_expected);
        }
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Deliver()
    {
        fixture.Processor.Verify(x => x.Process(_expected), Times.AtLeast(10));
    }

    [Fact]
    public void It_Should_Deliver_Expected()
    {
        _expected.ShouldEqual(_result);
    }
}

[Collection(DefaultClusterCollection.Name)]
public class When_Sending_Blob_Message_One_To_One(DefaultClusterFixture fixture) : IAsyncLifetime
{
    private byte[] _result;
    private byte[] _expected = new byte[1024];
    private List<Stopwatch> _timers = new ();

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();
        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x => _result = x);

        for (var i = 0; i < 1024; i++)
        {
            _expected[i] = Convert.ToByte(i % 2);
        }

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());

            _timers.Add(Stopwatch.StartNew());
            await grain.SendAsync(_expected);
            _timers.Last().Stop();
        }
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Fast()
    {
        TimeSpan.FromTicks(Convert.ToInt64(_timers.Average(x => x.Elapsed.Ticks)))
                .ShouldBeLessThan(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void It_Should_Deliver()
    {
        fixture.Processor.Verify(x => x.Process(_expected), Times.AtLeast(10));
    }

    [Fact]
    public void It_Should_Deliver_Expected()
    {
        _expected.ShouldEqual(_result);
    }
}
