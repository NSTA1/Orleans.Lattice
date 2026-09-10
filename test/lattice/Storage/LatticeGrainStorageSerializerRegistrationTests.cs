using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Covers how <see cref="LatticeGrainStorageSerializer"/> is installed: that
/// it becomes the serializer the container resolves, that it does not
/// discard a serializer the host registered itself, and that installing it
/// twice cannot stack one wrapper on another.
/// </summary>
[TestFixture]
public sealed class LatticeGrainStorageSerializerRegistrationTests
{
    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        return services;
    }

    [Test]
    public void AddLatticeGrainStorageSerializer_RejectsNullServices()
    {
        Assert.Throws<ArgumentNullException>(
            () => ((IServiceCollection)null!).AddLatticeGrainStorageSerializer());
    }

    [Test]
    public void AddLatticeGrainStorageSerializer_BecomesTheResolvedSerializer()
    {
        var services = CreateServices();

        // The positive control for the displacement claim: without the call
        // the container resolves whatever was registered before, and the
        // assertion below is only meaningful because this one holds.
        using (var baseline = services.BuildServiceProvider())
        {
            Assert.That(
                baseline.GetService<IGrainStorageSerializer>(),
                Is.Not.InstanceOf<LatticeGrainStorageSerializer>());
        }

        services.AddSingleton<IGrainStorageSerializer, StubGrainStorageSerializer>();
        services.AddLatticeGrainStorageSerializer();

        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<IGrainStorageSerializer>(),
            Is.InstanceOf<LatticeGrainStorageSerializer>());
    }

    [Test]
    public void AddLatticeGrainStorageSerializer_KeepsAHostSuppliedSerializerAsTheFallback()
    {
        var services = CreateServices();
        services.AddSingleton<IGrainStorageSerializer, StubGrainStorageSerializer>();
        services.AddLatticeGrainStorageSerializer();

        using var provider = services.BuildServiceProvider();
        var subject = (LatticeGrainStorageSerializer)provider.GetRequiredService<IGrainStorageSerializer>();

        Assert.That(subject.Fallback, Is.InstanceOf<StubGrainStorageSerializer>());
    }

    [Test]
    public void AddLatticeGrainStorageSerializer_IsIdempotent()
    {
        var services = CreateServices();
        services.AddSingleton<IGrainStorageSerializer, StubGrainStorageSerializer>();
        services.AddLatticeGrainStorageSerializer();
        services.AddLatticeGrainStorageSerializer();

        using var provider = services.BuildServiceProvider();
        var subject = (LatticeGrainStorageSerializer)provider.GetRequiredService<IGrainStorageSerializer>();

        Assert.That(
            subject.Fallback,
            Is.InstanceOf<StubGrainStorageSerializer>(),
            "a second install must not wrap the first");
    }

    [Test]
    public void AddLatticeGrainStorageSerializer_MaterialisesAFactoryRegistration()
    {
        var services = CreateServices();
        var host = new StubGrainStorageSerializer();
        services.AddSingleton<IGrainStorageSerializer>(_ => host);
        services.AddLatticeGrainStorageSerializer();

        using var provider = services.BuildServiceProvider();
        var subject = (LatticeGrainStorageSerializer)provider.GetRequiredService<IGrainStorageSerializer>();

        Assert.That(subject.Fallback, Is.SameAs(host));
    }

    [Test]
    public void AddLatticeGrainStorageSerializer_MaterialisesAnInstanceRegistration()
    {
        var services = CreateServices();
        var host = new StubGrainStorageSerializer();
        services.AddSingleton<IGrainStorageSerializer>(host);
        services.AddLatticeGrainStorageSerializer();

        using var provider = services.BuildServiceProvider();
        var subject = (LatticeGrainStorageSerializer)provider.GetRequiredService<IGrainStorageSerializer>();

        Assert.That(subject.Fallback, Is.SameAs(host));
    }

    [Test]
    public void LeafSnapshotBlob_IsWrittenThroughTheBinarySerializer()
    {
        // Issue #2481: this is the state type whose JSON write exhausts the
        // heap on a warm-volume replay, and marking it is the whole fix.
        Assert.That(LatticeGrainStorageSerializer.WritesBinary(typeof(LeafSnapshotBlob)), Is.True);
    }

    private sealed class StubGrainStorageSerializer : IGrainStorageSerializer
    {
        public BinaryData Serialize<T>(T value) => new(Array.Empty<byte>());

        public T Deserialize<T>(BinaryData input) => Activator.CreateInstance<T>();
    }
}
