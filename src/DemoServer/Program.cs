using Orleans.Providers;
using Orleans.Streaming.Grains.Extensions;
using Orleans.Streaming.Grains.Tests.Streams.Grains;
using Orleans.Streaming.Grains.Tests.Streams.Messages;

var builder = WebApplication.CreateBuilder(args);

// aspire app host setup
builder.AddServiceDefaults();

// Orleans set up
builder.AddKeyedAzureTableServiceClient("clustering");
builder.AddKeyedAzureBlobServiceClient("grainstate");

builder.UseOrleans(silo =>
{
    silo.AddActivityPropagation();
    silo.AddGrainsStreams(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, // "Default"
        4, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5));
});

// add logging
builder.Logging.AddConsole();

// Add services to the container.
builder.Services.AddSingleton<IProcessor, Processor>();
builder.Services.AddControllers();
builder.Services.AddHostedService<EmitterWorker>();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.MapGet("/", (IGrainFactory factory) => "Hello, world");

app.Run();

// this will be injected into the SimpleReceiverGrain to process messages
internal class Processor(ILogger<Processor> logger) : IProcessor
{
    public void Process(string message)
    {
        logger.LogInformation("Processed message: {Message}", message);
    }

    /// <inheritdoc />
    public void Process(byte[] data)
    {
        throw new NotImplementedException();
    }
}

// generates messages for a stream random id
internal class EmitterWorker(IGrainFactory grainFactory, ILogger<EmitterWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // generate a random stream id - this will activate an associated SimpleReceiverGrain using
        // an implicit subscription on the other end that will use the stream id as its primary key
        var emitter = grainFactory.GetGrain<IEmitterGrain>(Guid.NewGuid());
        while (!stoppingToken.IsCancellationRequested)
        {
            // pump a message out that will be picked up by the SimpleReceiverGrain
            // with an implicit subscription
            var message = $"Message at {DateTime.UtcNow}";
            await emitter.SendAsync(message);
            logger.LogInformation("Sent message: {Message}", message);
            await Task.Delay(1000, stoppingToken);
        }
    }
}
