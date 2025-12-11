// <copyright file="TransactionTests.cs" company="Surveily Sp. z o.o.">
// Copyright (c) Surveily Sp. z o.o.. All rights reserved.
// </copyright>

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Streaming.Grains.Abstract;
using Orleans.Streaming.Grains.Services;
using Orleans.Streaming.Grains.State;
using Orleans.Streaming.Grains.Streams;
using Orleans.Streaming.Grains.Test;
using Should;
using Xunit;

namespace Orleans.Streaming.Grains.Tests.Grains;

[Collection(TransactionClusterCollection.Name)]
public class WhenPoppingEmpty(TransactionClusterFixture fixture) : IAsyncLifetime
{
    private readonly string _grainId = $"popping-empty-{Guid.NewGuid()}";
    private ITransactionService _service;
    private List<(Guid Id, Immutable<int> Item)> _results;

    public async ValueTask InitializeAsync()
    {
        _service = fixture.Container.GetService<ITransactionService>();
        _results = await _service.PopAsync<int>(_grainId, 1);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Return_Null()
    {
        _results.ShouldBeEmpty();
    }
}

[Collection(TransactionClusterCollection.Name)]
public class WhenPoppingSingle(TransactionClusterFixture fixture) : IAsyncLifetime
{
    private readonly string _grainId = $"popping-single-{Guid.NewGuid()}";
    private IClusterClient _client;
    private ITransactionService _service;
    private TransactionGrainState _state;
    private List<(Guid Id, Immutable<int> Item)> _results;

    public async ValueTask InitializeAsync()
    {
        _client = fixture.Container.GetService<IClusterClient>();
        _service = fixture.Container.GetService<ITransactionService>();

        await _service.PostAsync(new Immutable<int>(100), false, _grainId);

        _results = await _service.PopAsync<int>(_grainId, 1);

        var transaction = _client.GetGrain<ITransactionGrain>(_grainId);

        _state = await transaction.GetStateAsync();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Return()
    {
        _results.ShouldNotBeEmpty();
    }

    [Fact]
    public void It_Should_Return_Item()
    {
        _results.First().Item.Value.ShouldEqual(100);
    }

    [Fact]
    public void It_Should_Return_Id()
    {
        _results.First().Id.ShouldNotEqual(Guid.Empty);
    }

    [Fact]
    public void State_Should_Have_Poison_Empty()
    {
        _state.Poison.ShouldBeEmpty();
    }

    [Fact]
    public void State_Should_Have_Queue_Empty()
    {
        _state.Queue.ShouldBeEmpty();
    }

    [Fact]
    public void State_Should_Have_Transactions_Single()
    {
        _state.Transactions.Count.ShouldEqual(1);
    }
}

[Collection(TransactionClusterCollection.Name)]
public class WhenPoppingSingleTimeout(TransactionClusterFixture fixture) : IAsyncLifetime
{
    private readonly string _grainId = $"popping-single-timeout-{Guid.NewGuid()}";
    private IClusterClient _client;
    private ITransactionService _service;
    private TransactionGrainState _state;
    private List<(Guid Id, Immutable<int> Item)> _results;

    public async ValueTask InitializeAsync()
    {
        _client = fixture.Container.GetService<IClusterClient>();
        _service = fixture.Container.GetService<ITransactionService>();

        await _service.PostAsync(new Immutable<int>(100), false, _grainId);

        _results = await _service.PopAsync<int>(_grainId, 1);

        var transaction = _client.GetGrain<ITransactionGrain>(_grainId);

        await Task.Delay(TimeSpan.FromSeconds(2));

        _state = await transaction.GetStateAsync();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Return()
    {
        _results.ShouldNotBeEmpty();
    }

    [Fact]
    public void It_Should_Return_Item()
    {
        _results.First().Item.Value.ShouldEqual(100);
    }

    [Fact]
    public void It_Should_Return_Id()
    {
        _results.First().Id.ShouldNotEqual(Guid.Empty);
    }

    [Fact]
    public void State_Should_Have_Poison_Empty()
    {
        _state.Poison.ShouldBeEmpty();
    }

    [Fact]
    public void State_Should_Have_Queue_One()
    {
        _state.Queue.Count.ShouldEqual(1);
    }

    [Fact]
    public void State_Should_Have_Transactions_Empty()
    {
        _state.Transactions.Count.ShouldEqual(1);
    }
}

[Collection(TransactionClusterCollection.Name)]
public class WhenPoppingSingleAfterComplete(TransactionClusterFixture fixture) : IAsyncLifetime
{
    private readonly string _grainId = $"popping-after-complete-{Guid.NewGuid()}";
    private IClusterClient _client;
    private ITransactionService _service;
    private TransactionGrainState _state;
    private List<(Guid Id, Immutable<int> Item)> _results;
    private List<(Guid Id, Immutable<int> Item)> _results2;

    public async ValueTask InitializeAsync()
    {
        _client = fixture.Container.GetService<IClusterClient>();
        _service = fixture.Container.GetService<ITransactionService>();

        await _service.PostAsync(new Immutable<int>(100), false, _grainId);

        _results = await _service.PopAsync<int>(_grainId, 1);

        await _service.CompleteAsync<int>(_results.First().Id, true, _grainId);

        _results2 = await _service.PopAsync<int>(_grainId, 1);

        var transaction = _client.GetGrain<ITransactionGrain>(_grainId);

        _state = await transaction.GetStateAsync();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Return()
    {
        _results.ShouldNotBeEmpty();
    }

    [Fact]
    public void It_Should_Return_Item()
    {
        _results.First().Item.Value.ShouldEqual(100);
    }

    [Fact]
    public void It_Should_Return_Id()
    {
        _results.First().Id.ShouldNotEqual(Guid.Empty);
    }

    [Fact]
    public void It_Should_Return_Second_Null()
    {
        _results2.ShouldBeEmpty();
    }

    [Fact]
    public void State_Should_Have_Poison_Empty()
    {
        _state.Poison.ShouldBeEmpty();
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

[Collection(TransactionClusterCollection.Name)]
public class WhenPoppingSingleAfterCompletePoison(TransactionClusterFixture fixture) : IAsyncLifetime
{
    private readonly string _grainId = $"popping-after-complete-poison-{Guid.NewGuid()}";
    private IClusterClient _client;
    private ITransactionService _service;
    private TransactionGrainState _state;
    private List<(Guid Id, Immutable<int> Item)> _results;
    private List<(Guid Id, Immutable<int> Item)> _results2;

    public async ValueTask InitializeAsync()
    {
        _client = fixture.Container.GetService<IClusterClient>();
        _service = fixture.Container.GetService<ITransactionService>();

        await _service.PostAsync(new Immutable<int>(100), false, _grainId);

        _results = await _service.PopAsync<int>(_grainId, 1);

        await _service.CompleteAsync<int>(_results.First().Id, false, _grainId);

        _results2 = await _service.PopAsync<int>(_grainId, 1);

        var transaction = _client.GetGrain<ITransactionGrain>(_grainId);

        _state = await transaction.GetStateAsync();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void It_Should_Return()
    {
        _results.ShouldNotBeEmpty();
    }

    [Fact]
    public void It_Should_Return_Item()
    {
        _results.First().Item.Value.ShouldEqual(100);
    }

    [Fact]
    public void It_Should_Return_Id()
    {
        _results.First().Id.ShouldNotEqual(Guid.Empty);
    }

    [Fact]
    public void It_Should_Return_Second_Null()
    {
        _results2.ShouldBeEmpty();
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
