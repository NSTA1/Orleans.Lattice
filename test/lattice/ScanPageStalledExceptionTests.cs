using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="ScanPageStalledException"/>: its
/// construction overloads, the sealed / public contract, its derivation from
/// <see cref="TimeoutException"/>, the typed attribution slots, and the stable
/// Orleans serialization surface (alias, <c>[GenerateSerializer]</c>, a full
/// round-trip, and the same-silo deep copy).
/// <para>
/// The round-trip is load-bearing: a shard-root page fill is routinely issued
/// from a lattice grain on a peer silo, so the stall fault must cross the grain
/// boundary as itself - carrying the phase and leaf attribution that makes the
/// stall diagnosable - rather than degrade into an opaque
/// <c>CodecNotFoundException</c>.
/// </para>
/// </summary>
[TestFixture]
public class ScanPageStalledExceptionTests
{
    private ServiceProvider _services = null!;
    private Serializer<ScanPageStalledException> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<ScanPageStalledException>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Parameterless_constructor_initialises_with_empty_context()
    {
        var ex = new ScanPageStalledException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.TreeId, Is.Empty);
            Assert.That(ex.Operation, Is.Empty);
            Assert.That(ex.Phase, Is.Empty);
            Assert.That(ex.ShardIndex, Is.Zero);
            Assert.That(ex.LeavesVisited, Is.Zero);
            Assert.That(ex.TimeoutSeconds, Is.Zero);
        });
    }

    [Test]
    public void Message_constructor_preserves_message()
    {
        var ex = new ScanPageStalledException("page fill stalled");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("page fill stalled"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new OperationCanceledException("deadline");
        var ex = new ScanPageStalledException("page fill stalled", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("page fill stalled"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_from_TimeoutException()
    {
        // Callers already retry TimeoutException from the sibling shard-root
        // wedge guards (shard forward, activation readiness), so the stall
        // fault deliberately joins that family rather than inventing a new one.
        Assert.That(new ScanPageStalledException("m"), Is.InstanceOf<TimeoutException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ScanPageStalledException).IsSealed, Is.True);
            Assert.That(typeof(ScanPageStalledException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(ScanPageStalledException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.spt"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(ScanPageStalledException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void Round_trips_every_attribution_slot_through_the_Orleans_serializer()
    {
        var inner = new OperationCanceledException("ceiling fired");
        var original = new ScanPageStalledException("GetSortedEntriesBatchAsync stalled", inner)
        {
            TreeId = "orders",
            ShardIndex = 7,
            Operation = "GetSortedEntriesBatchAsync",
            Phase = "leaf-walk",
            LeavesVisited = 41,
            TimeoutSeconds = 30d,
        };

        var restored = _serializer.Deserialize(_serializer.SerializeToArray(original));

        Assert.Multiple(() =>
        {
            Assert.That(restored, Is.Not.Null);
            Assert.That(restored.Message, Is.EqualTo(original.Message));
            Assert.That(restored.InnerException, Is.Not.Null);
            Assert.That(restored.TreeId, Is.EqualTo("orders"));
            Assert.That(restored.ShardIndex, Is.EqualTo(7));
            Assert.That(restored.Operation, Is.EqualTo("GetSortedEntriesBatchAsync"));
            Assert.That(restored.Phase, Is.EqualTo("leaf-walk"));
            Assert.That(restored.LeavesVisited, Is.EqualTo(41));
            Assert.That(restored.TimeoutSeconds, Is.EqualTo(30d));
        });
    }

    [Test]
    public void Deep_copies_through_the_Orleans_copier_on_a_same_silo_boundary()
    {
        // Regression guard for the repository-wide contract: this exception
        // derives from TimeoutException, a BCL subclass Orleans registers no
        // base-type copier for, so without the no-op copier a co-located page
        // fill would fail the deep copy with an opaque KeyNotFoundException and
        // mask the real stall.
        var copier = _services.GetRequiredService<DeepCopier<ScanPageStalledException>>();
        var original = new ScanPageStalledException("stalled", new OperationCanceledException("x"))
        {
            TreeId = "orders",
            ShardIndex = 3,
            Operation = "CountBoundedAsync",
            Phase = "prologue",
            LeavesVisited = 0,
            TimeoutSeconds = 30d,
        };

        var copy = copier.Copy(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.Not.Null);
            Assert.That(copy.Message, Is.EqualTo(original.Message));
            Assert.That(copy.Phase, Is.EqualTo("prologue"));
            Assert.That(copy.Operation, Is.EqualTo("CountBoundedAsync"));
        });
    }
}
