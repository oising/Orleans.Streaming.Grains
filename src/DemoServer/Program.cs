using Orleans.Providers;
using Orleans.Streaming.Grains.Extensions;

var builder = WebApplication.CreateBuilder(args);

// aspire app host setup
builder.AddServiceDefaults();

// Orleans set up
builder.AddKeyedAzureTableServiceClient("clustering");
builder.AddKeyedAzureBlobServiceClient("grainstate");
builder.UseOrleans(silo =>
{
    silo.AddActivityPropagation();
    silo.AddMemoryGrainStorage(ProviderConstants.DEFAULT_PUBSUB_PROVIDER_NAME);
    silo.AddGrainsStreams("GrainsStreamProvider", 4, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5));
});

// Add services to the container.
builder.Services.AddControllers();

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

app.MapGet("/", (IGrainFactory factory) => "Hello World!");

app.Run();
