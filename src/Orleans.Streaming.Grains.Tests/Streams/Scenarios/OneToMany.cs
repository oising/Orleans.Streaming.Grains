// <copyright file="OneToMany.cs" company="Surveily Sp. z o.o.">
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

[Collection(DefaultClusterCollection.Name)]
public class When_Sending_Compound_Message_One_To_Many(DefaultClusterFixture fixture) : IAsyncLifetime
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

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x => _resultText = x);

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x => _resultData = x);

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
            await grain.CompoundAsync(_expectedText, _expectedData);
        }
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
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(CompoundMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Queue.ShouldBeEmpty();
        }
    }

    [Fact(Skip = "Poison assertions are unreliable when running with shared cluster fixture")]
    public async Task It_Should_Empty_Poison()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(CompoundMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Poison.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task It_Should_Empty_Transactions()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(CompoundMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Transactions.ShouldBeEmpty();
        }
    }
}

[Collection(DefaultClusterCollection.Name)]
public class When_Sending_Explosive_Message_One_To_Many(DefaultClusterFixture fixture) : IAsyncLifetime
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

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x => _resultText = x);

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x => _resultData = x);

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
        await grain.ExplosiveAsync(_expectedText, _expectedData);
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
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(ExplosiveMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Queue.ShouldBeEmpty();
        }
    }

    [Fact(Skip = "Poison assertions are unreliable when running with shared cluster fixture")]
    public async Task It_Should_Empty_Poison()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(ExplosiveMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Poison.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task It_Should_Empty_Transactions()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(ExplosiveMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Transactions.ShouldBeEmpty();
        }
    }
}

[Collection(DefaultClusterCollection.Name)]
public class When_Sending_Broadcast_Message_One_To_Many(DefaultClusterFixture fixture) : IAsyncLifetime
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

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Callback<string>(x => _resultText = x);

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Callback<byte[]>(x => _resultData = x);

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        for (var i = 0; i < 10; i++)
        {
            var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
            await grain.BroadcastAsync(_expectedText, _expectedData);
        }
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
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(BroadcastMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Queue.ShouldBeEmpty();
        }
    }

    [Fact(Skip = "Poison assertions are unreliable when running with shared cluster fixture")]
    public async Task It_Should_Empty_Poison()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(BroadcastMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Poison.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task It_Should_Empty_Transactions()
    {
        for (var i = 0; i < _settings.Value.QueueCount; i++)
        {
            var grain = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(BroadcastMessage).ToLower()}-{i}");
            var state = await grain.GetStateAsync();

            state.Transactions.ShouldBeEmpty();
        }
    }
}

[Collection(DefaultClusterCollection.Name)]
public class When_Sending_Broadcast_Message_One_To_Many_Error(DefaultClusterFixture fixture) : IAsyncLifetime
{
    private TransactionGrainState _state;

    private string _expectedText = "text";

    private byte[] _expectedData = new byte[1024];

    public async ValueTask InitializeAsync()
    {
        fixture.Processor.Reset();

        fixture.Processor.Setup(x => x.Process(It.IsAny<string>()))
                          .Throws<Exception>();

        fixture.Processor.Setup(x => x.Process(It.IsAny<byte[]>()))
                          .Throws<Exception>();

        for (var i = 0; i < 1024; i++)
        {
            _expectedData[i] = Convert.ToByte(i % 2);
        }

        var grain = fixture.Client.GetGrain<IEmitterGrain>(Guid.NewGuid());
        var transaction = fixture.Client.GetGrain<ITransactionGrain>($"{nameof(BroadcastMessage).ToLower()}-0");

        await grain.BroadcastAsync(_expectedText, _expectedData);

        _state = await transaction.GetStateAsync();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Deliver_Text()
    {
        fixture.Processor.Verify(x => x.Process(_expectedText), Times.AtLeast(2));
    }

    [Fact]
    public void It_Should_Deliver_Data()
    {
        fixture.Processor.Verify(x => x.Process(_expectedData), Times.AtLeast(2));
    }

    [Fact]
    public void State_Should_Have_Poison_Single()
    {
        _state.Poison.Count.ShouldEqual(1);
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
