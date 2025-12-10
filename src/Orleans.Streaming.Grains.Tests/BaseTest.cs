// <copyright file="BaseTest.cs" company="Surveily Sp. z o.o.">
// Copyright (c) Surveily Sp. z o.o.. All rights reserved.
// </copyright>

using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Orleans.Streaming.Grains.Test
{
    public abstract class BaseTest<T> : IAsyncLifetime
        where T : class
    {
        public BaseTest()
        {
            Services = new ServiceCollection();
        }

        public T Subject { get; private set; }

        public ServiceCollection Services { get; }

        public virtual ValueTask InitializeAsync()
        {
            if (Services.All(x => x.ServiceType != typeof(T)))
            {
                Services.AddTransient<T>();
            }

            var provider = Services.BuildServiceProvider();

            if (provider != null)
            {
                var service = provider.GetService<T>();

                if (service != null)
                {
                    Subject = service;
                }
                else
                {
                    throw new InvalidOperationException("Subject not registered.");
                }
            }

            return ValueTask.CompletedTask;
        }

        public virtual ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
