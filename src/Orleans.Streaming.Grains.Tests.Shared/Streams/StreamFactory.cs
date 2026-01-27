// <copyright file="StreamFactory.cs" company="Surveily Sp. z o.o.">
// Copyright (c) Surveily Sp. z o.o.. All rights reserved.
// </copyright>

using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.Grains.Tests.Streams;

public static class StreamFactory
{
    public static IAsyncStream<T> Create<T>(IStreamProvider provider, Guid id)
    {
        return provider.GetStream<T>(StreamId.Create(typeof(T).Name, id));
    }
}
