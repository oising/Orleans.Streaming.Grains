var builder = DistributedApplication.CreateBuilder(args);

var storage = builder.AddAzureStorage("storage").RunAsEmulator();
var clusteringTable = storage.AddTables("clustering");
var grainStorage = storage.AddBlobs("grainstate");

var orleans = builder.AddOrleans("stream-demo")
                     .WithClustering(clusteringTable)
                     .WithGrainStorage("Default", grainStorage);

builder.AddProject<Projects.DemoServer>("demoserver")
    .WithReference(orleans);

builder.Build().Run();
