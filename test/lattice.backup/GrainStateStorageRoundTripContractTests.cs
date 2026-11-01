using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.GrainReferences;
using Orleans.Lattice.Testing;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Concrete guard for the <c>Orleans.Lattice.Backup</c> assembly: every grain persistent-state
/// type it declares must round-trip through the grain-storage serializer a silo
/// hands Azure Table storage - Orleans' post-configured JSON serializer behind
/// <see cref="LatticeGrainStorageSerializer"/> (issue #5579). Reuses
/// <see cref="GrainStateStorageRoundTripContractTestsBase"/>.
/// </summary>
[TestFixture]
public sealed class GrainStateStorageRoundTripContractTests : GrainStateStorageRoundTripContractTestsBase
{
    private ServiceProvider services = null!;
    private LatticeGrainStorageSerializer storage = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        var collection = new ServiceCollection();
        collection.AddSerializer();
        collection.AddOptions();
        collection.AddSingleton<GrainReferenceActivator>();
        collection.AddSingleton<IPostConfigureOptions<OrleansJsonSerializerOptions>, ConfigureOrleansJsonSerializerOptions>();
        collection.AddSingleton<OrleansJsonSerializer>();
        this.services = collection.BuildServiceProvider();
        this.storage = new LatticeGrainStorageSerializer(
            this.services.GetRequiredService<Serializer>(),
            ActivatorUtilities.CreateInstance<JsonGrainStorageSerializer>(this.services));
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => this.services.Dispose();

    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeBackupCrossTreeFenceException).Assembly;

    /// <inheritdoc />
    protected override T RoundTrip<T>(T state) => this.storage.Deserialize<T>(this.storage.Serialize(state));
}