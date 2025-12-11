// <copyright file="OneToOneWait.cs" company="Surveily Sp. z o.o.">
// Copyright (c) Surveily Sp. z o.o.. All rights reserved.
// </copyright>

using Microsoft.Extensions.DependencyInjection;
using Moq;
using Orleans.Streaming.Grains.Test;
using Orleans.Streaming.Grains.Tests.Streams.Grains;
using Orleans.Streaming.Grains.Tests.Streams.Messages;
using Should;
using Xunit;

namespace Orleans.Streaming.Grains.Tests.Streams.Scenarios;

[Collection(WaitClusterCollection.Name)]
public class When_Sending_Simple_Message_One_To_One_Wait(WaitClusterFixture fixture) : IAsyncLifetime
{
    private string _result;
    private string _expected = "text";

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();
        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x =>
                          {
                              if (x == _expected)
                              {
                                  _result = x;
                              }
                          });

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
            await grain.SendAsync(_expected);
        }

        await fixture.WaitFor(() => _result);
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

[Collection(WaitClusterCollection.Name)]
public class When_Sending_Blob_Message_One_To_One_Wait(WaitClusterFixture fixture) : IAsyncLifetime
{
    private byte[] _result;
    private byte[] _expected = new byte[1024];

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();

        for (var i = 0; i < 1024; i++)
        {
            _expected[i] = Convert.ToByte(i % 2);
        }

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x =>
                          {
                              if (x != null && x.SequenceEqual(_expected))
                              {
                                  _result = x;
                              }
                          });

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
            await grain.SendAsync(_expected);
        }

        await fixture.WaitFor(() => _result);
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
