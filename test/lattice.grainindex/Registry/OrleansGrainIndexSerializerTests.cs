using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="OrleansGrainIndexSerializer{T}"/>: the adapter that keeps
/// durable registry state in the Orleans wire format the alias table governs
/// rather than in a lossy JSON projection.
/// </summary>
[TestFixture]
public sealed class OrleansGrainIndexSerializerTests
{
    private ServiceProvider _provider = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        _provider = services.BuildServiceProvider();
    }

    [TearDown]
    public void TearDown() => _provider.Dispose();

    private OrleansGrainIndexSerializer<T> SerializerFor<T>() =>
        new(_provider.GetRequiredService<Serializer<T>>());

    [Test]
    public void A_descriptor_round_trips_with_every_field_intact()
    {
        var serializer = SerializerFor<GrainIndexDescriptor>();
        var descriptor = DescriptorFactory.Create(allowReplication: true);

        var round = serializer.Deserialize(serializer.Serialize(descriptor));

        Assert.Multiple(() =>
        {
            Assert.That(round.Name, Is.EqualTo(descriptor.Name));
            Assert.That(round.TreeName, Is.EqualTo(descriptor.TreeName));
            Assert.That(round.Properties, Is.EqualTo(descriptor.Properties));
            Assert.That(round.AllowReplication, Is.True,
                "The descriptor exposes get-only properties, which is exactly what a JSON "
                + "serializer would have dropped.");
        });
    }

    [Test]
    public void A_fingerprint_round_trips_by_value()
    {
        var serializer = SerializerFor<GrainIndexFingerprint>();
        var fingerprint = GrainIndexFingerprint.Compute(
            DescriptorFactory.Create(), DescriptorFactory.DefaultKeyCodecId);

        Assert.That(serializer.Deserialize(serializer.Serialize(fingerprint)), Is.EqualTo(fingerprint));
    }

    [Test]
    public void Serialize_produces_bytes_that_are_not_empty()
    {
        var serializer = SerializerFor<GrainIndexFingerprint>();

        Assert.That(serializer.Serialize(new GrainIndexFingerprint("ABCD")), Is.Not.Empty);
    }

    [Test]
    public void Deserialize_rejects_a_null_buffer()
    {
        Assert.That(
            () => SerializerFor<GrainIndexFingerprint>().Deserialize(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_orleans_serializer_is_rejected()
    {
        Assert.That(
            () => new OrleansGrainIndexSerializer<GrainIndexFingerprint>(null!),
            Throws.ArgumentNullException);
    }
}
