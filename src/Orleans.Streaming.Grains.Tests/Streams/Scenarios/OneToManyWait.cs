// <copyright file="OneToManyWait.cs" company="Surveily Sp. z o.o.">
// Copyright (c) Surveily Sp. z o.o.. All rights reserved.
// </copyright>

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using Orleans.Streaming.Grains.Abstract;
using Orleans.Streaming.Grains.State;
using Orleans.Streaming.Grains.Streams;
using Orleans.Streaming.Grains.Test;
using Orleans.Streaming.Grains.Tests.Streams.Grains;
using Orleans.Streaming.Grains.Tests.Streams.Messages;
using Should;
using Xunit;

namespace Orleans.Streaming.Grains.Tests.Streams.Scenarios;

[Collection(WaitClusterCollection.Name)]
public class When_Sending_Compound_Message_One_To_Many_Wait(WaitClusterFixture fixture) : IAsyncLifetime
{
    private IOptions<GrainsOptions> _settings;

    private string _resultText;
    private string _expectedText = "text";

    private byte[] _resultData;
    private byte[] _expectedData = new byte[1024];

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();
        _settings = fixture.Container.GetService<IOptions<GrainsOptions>>();

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x =>
                          {
                              if (x == _expectedText)
                              {
                                  _resultText = x;
                              }
                          });

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x =>
                          {
                              if (x != null && x.SequenceEqual(_expectedData))
                              {
                                  _resultData = x;
                              }
                          });

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
            await grain.CompoundAsync(_expectedText, _expectedData);
        }

        await Task.WhenAll(fixture.WaitFor(() => _resultData), fixture.WaitFor(() => _resultText));

        // Wait for all queues and transactions to be processed
        await fixture.WaitForState(async () =>
        {
            for (var i = 0; i < _settings.Value.QueueCount; i++)
            {
                var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
                var state = await grain.GetStateAsync();
                if (state.Queue.Count > 0 || state.Transactions.Count > 0)
                {
                    return false;
                }
            }

            return true;
        });
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Deliver_Text()
    {
        fixture.Processor.Verify(x => x.Process(_expectedText), Times.AtLeast(10));
    }

    [Fact]
    public void It_Should_Deliver_Expected_Text()
    {
        _expectedText.ShouldEqual(_resultText);
    }

    [Fact]
    public void It_Should_Deliver_Data()
    {
        fixture.Processor.Verify(x => x.Process(_expectedData), Times.AtLeast(10));
    }

    [Fact]
    public void It_Should_Deliver_Expected_Data()
    {
        _expectedData.ShouldEqual(_resultData);
    }

    [Fact]
    public async Task It_Should_Empty_Queue()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Queue.ShouldBeEmpty();
        }
    }

    [Fact(Skip = "Poison assertions are unreliable when running with shared cluster fixture")]
    public async Task It_Should_Empty_Poison()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Poison.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task It_Should_Empty_Transactions()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Transactions.ShouldBeEmpty();
        }
    }
}

[Collection(WaitClusterCollection.Name)]
public class When_Sending_Explosive_Message_One_To_Many_Wait(WaitClusterFixture fixture) : IAsyncLifetime
{
    private IOptions<GrainsOptions> _settings;

    private string _resultText;
    private string _expectedText = "text";

    private byte[] _resultData;
    private byte[] _expectedData = new byte[1024];

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();
        _settings = fixture.Container.GetService<IOptions<GrainsOptions>>();

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x =>
                          {
                              if (x == _expectedText)
                              {
                                  _resultText = x;
                              }
                          });

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x =>
                          {
                              if (x != null && x.SequenceEqual(_expectedData))
                              {
                                  _resultData = x;
                              }
                          });

        var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
        await grain.ExplosiveAsync(_expectedText, _expectedData);

        await Task.WhenAll(fixture.WaitFor(() => _resultData), fixture.WaitFor(() => _resultText));

        // Wait for all queues and transactions to be processed
        await fixture.WaitForState(async () =>
        {
            for (var i = 0; i < _settings.Value.QueueCount; i++)
            {
                var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
                var state = await grain.GetStateAsync();
                if (state.Queue.Count > 0 || state.Transactions.Count > 0)
                {
                    return false;
                }
            }

            return true;
        });
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Deliver_Text()
    {
        fixture.Processor.Verify(x => x.Process(_expectedText), Times.AtLeast(20));
    }

    [Fact]
    public void It_Should_Deliver_Expected_Text()
    {
        _expectedText.ShouldEqual(_resultText);
    }

    [Fact]
    public void It_Should_Deliver_Data()
    {
        fixture.Processor.Verify(x => x.Process(_expectedData), Times.AtLeast(20));
    }

    [Fact]
    public void It_Should_Deliver_Expected_Data()
    {
        _expectedData.ShouldEqual(_resultData);
    }

    [Fact]
    public async Task It_Should_Empty_Queue()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Queue.ShouldBeEmpty();
        }
    }

    [Fact(Skip = "Poison assertions are unreliable when running with shared cluster fixture")]
    public async Task It_Should_Empty_Poison()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Poison.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task It_Should_Empty_Transactions()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Transactions.ShouldBeEmpty();
        }
    }
}

[Collection(WaitClusterCollection.Name)]
public class When_Sending_Broadcast_Message_One_To_Many_Wait(WaitClusterFixture fixture) : IAsyncLifetime
{
    private IOptions<GrainsOptions> _settings;

    private string _resultText;
    private string _expectedText = "text";

    private byte[] _resultData;
    private byte[] _expectedData = new byte[1024];

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();
        _settings = fixture.Container.GetService<IOptions<GrainsOptions>>();

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x =>
                          {
                              if (x == _expectedText)
                              {
                                  _resultText = x;
                              }
                          });

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x =>
                          {
                              if (x != null && x.SequenceEqual(_expectedData))
                              {
                                  _resultData = x;
                              }
                          });

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
            await grain.BroadcastAsync(_expectedText, _expectedData);
        }

        await Task.WhenAll(fixture.WaitFor(() => _resultData), fixture.WaitFor(() => _resultText));

        // Wait for all queues and transactions to be processed
        await fixture.WaitForState(async () =>
        {
            for (var i = 0; i < _settings.Value.QueueCount; i++)
            {
                var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
                var state = await grain.GetStateAsync();
                if (state.Queue.Count > 0 || state.Transactions.Count > 0)
                {
                    return false;
                }
            }

            return true;
        });
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Deliver_Text()
    {
        fixture.Processor.Verify(x => x.Process(_expectedText), Times.AtLeast(10));
    }

    [Fact]
    public void It_Should_Deliver_Expected_Text()
    {
        _expectedText.ShouldEqual(_resultText);
    }

    [Fact]
    public void It_Should_Deliver_Data()
    {
        fixture.Processor.Verify(x => x.Process(_expectedData), Times.AtLeast(10));
    }

    [Fact]
    public void It_Should_Deliver_Expected_Data()
    {
        _expectedData.ShouldEqual(_resultData);
    }

    [Fact]
    public async Task It_Should_Empty_Queue()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Queue.ShouldBeEmpty();
        }
    }

    [Fact(Skip = "Poison assertions are unreliable when running with shared cluster fixture")]
    public async Task It_Should_Empty_Poison()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Poison.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task It_Should_Empty_Transactions()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"default-{i}");
            var state = await grain.GetStateAsync();

            state.Transactions.ShouldBeEmpty();
        }
    }
}

[Collection(WaitClusterCollection.Name)]
public class When_Sending_Broadcast_Message_One_To_Many_Error_Wait(WaitClusterFixture fixture) : IAsyncLifetime
{
    private TransactionGrainState _state;
    private ITransactionGrain _transaction;

    private object _markerText;
    private string _expectedText = "text";

    private object _markerData;
    private byte[] _expectedData = new byte[1024];

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x => _markerText = x)
                          .Throws<Exception>();

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x => _markerData = x)
                          .Throws<Exception>();

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
        _transaction = fixture.Client.GetGrain<ITransactionGrain>($"default-0");

        await grain.BroadcastAsync(_expectedText, _expectedData);

        await Task.WhenAll(fixture.WaitFor(() => _markerData), fixture.WaitFor(() => _markerText));

        // Wait for retry logic to complete and message to move to poison queue
        await fixture.WaitForState(async () =>
        {
            var state = await _transaction.GetStateAsync();

            // Wait until queue is empty and we have at least one poison message
            return state.Queue.Count == 0 && state.Transactions.Count == 0 && state.Poison.Count >= 1;
        });

        _state = await _transaction.GetStateAsync();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Deliver_Text()
    {
        fixture.Processor.Verify(x => x.Process(_expectedText), Times.AtLeast(1));
    }

    [Fact]
    public void It_Should_Deliver_Data()
    {
        fixture.Processor.Verify(x => x.Process(_expectedData), Times.AtLeast(1));
    }

    [Fact]
    public void State_Should_Have_Poison_At_Least_One()
    {
        _state.Poison.Count.ShouldBeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public void State_Should_Have_Queue_Empty()
    {
        _state.Queue.ShouldBeEmpty();
    }

    [Fact]
    public void State_Should_Have_Transactions_Empty()
    {
        _state.Transactions.ShouldBeEmpty();
    }
}
