using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LeafProjectionStaleException"/>: its
/// construction overloads, the sealed / public contract, its derivation from
/// <see cref="InvalidOperationException"/>, and the stable Orleans
/// serialization surface (alias, <c>[GenerateSerializer]</c>, and a full
/// serialize/deserialize round-trip). The round-trip is load-bearing: leaf
/// activation faults are routinely raised on a leaf placed on a peer silo
/// (the data API and replication digest probe both activate leaves
/// cross-silo), so the exception must serialise across the grain boundary
/// rather than degrade into an opaque <c>CodecNotFoundException</c> messaging
/// storm.
/// </summary>
[TestFixture]
public class LeafProjectionStaleExceptionTests
{
    private ServiceProvider _services = null!;
    private Serializer<LeafProjectionStaleException> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LeafProjectionStaleException>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Parameterless_constructor_initialises_with_empty_context()
    {
        var ex = new LeafProjectionStaleException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_constructor_preserves_message()
    {
        var ex = new LeafProjectionStaleException("projection stale");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("projection stale"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("underlying");
        var ex = new LeafProjectionStaleException("projection stale", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("projection stale"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        var ex = new LeafProjectionStaleException("m");
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LeafProjectionStaleException).IsSealed, Is.True);
            Assert.That(typeof(LeafProjectionStaleException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(LeafProjectionStaleException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.lps"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(LeafProjectionStaleException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void Round_trips_message_and_inner_through_the_Orleans_serializer()
    {
        var inner = new InvalidOperationException("checkpoint fell off the log");
        var original = new LeafProjectionStaleException(
            "Leaf projection for tree 'test' partition 4 cannot be rebuilt from the WAL",
            inner);

        var bytes = _serializer.SerializeToArray(original);
        var restored = _serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(restored, Is.Not.Null);
            Assert.That(restored.Message, Is.EqualTo(original.Message));
            Assert.That(restored.InnerException, Is.Not.Null);
            Assert.That(restored.InnerException!.Message, Is.EqualTo(inner.Message));
        });
    }
}
