using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The WAL append fence's supersession refusal is the existing
/// <see cref="LeafRetiredException"/> with two fields appended (epic #5605, review
/// S1): <see cref="LeafRetiredException.ActivationSuperseded"/> at id 1 and
/// <see cref="LeafRetiredException.SupersededEpoch"/> at id 2, after 10.2.0's only
/// field, <see cref="LeafRetiredException.LeafId"/> at id 0, under the unchanged
/// alias <c>ol.lrt</c>. These tests pin that it reads both ways across a rolling
/// upgrade, against a test-local mirror of the 10.2.0 shape: a refusal written by
/// new code reads on a 10.2.0 silo as the retired-leaf refusal its shard root
/// already retries, and one written by a 10.2.0 silo reads on new code as a
/// genuine reclaim, never as a supersession.
/// </summary>
[TestFixture]
public sealed class LeafRetiredExceptionWireCompatibilityTests
{
    private ServiceProvider _services = null!;
    private Serializer<LeafRetiredException> _serializer = null!;
    private Serializer<PreFenceLeafRetiredException> _oldSerializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LeafRetiredException>>();
        _oldSerializer = _services.GetRequiredService<Serializer<PreFenceLeafRetiredException>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void The_alias_is_the_one_10_2_0_resolves()
    {
        var alias = typeof(LeafRetiredException).GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>().Single().Alias;

        Assert.That(alias, Is.EqualTo("ol.lrt"));
    }

    [Test]
    public void A_supersession_round_trips_its_marker_on_new_code()
    {
        var leaf = Guid.NewGuid();
        var bytes = _serializer.SerializeToArray(LeafRetiredException.ForSupersededActivation(leaf, 7));

        var decoded = _serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ActivationSuperseded, Is.True);
            Assert.That(decoded.SupersededEpoch, Is.EqualTo(7));
            Assert.That(decoded.LeafId, Is.EqualTo(leaf.ToString("N")));
        });
    }

    [Test]
    public void A_supersession_written_by_new_code_reads_on_a_10_2_0_silo_as_a_retired_leaf()
    {
        var leaf = Guid.NewGuid();
        var bytes = _serializer.SerializeToArray(LeafRetiredException.ForSupersededActivation(leaf, 7));

        var old = _oldSerializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(old.LeafId, Is.EqualTo(leaf.ToString("N")), "the 10.2.0 field reads unchanged");
            Assert.That(old.Message, Does.Contain("superseded"), "the base exception slice reads unchanged");
        });
    }

    [Test]
    public void A_refusal_written_by_a_10_2_0_silo_reads_on_new_code_as_a_reclaim()
    {
        var bytes = _oldSerializer.SerializeToArray(new PreFenceLeafRetiredException { LeafId = "leaf" });

        var decoded = _serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.LeafId, Is.EqualTo("leaf"));
            Assert.That(decoded.ActivationSuperseded, Is.False,
                "a 10.2.0 reclaim is never mistaken for a supersession, so the seam leaves it to re-route");
            Assert.That(decoded.SupersededEpoch, Is.Zero);
        });
    }

    /// <summary>Test-local mirror of the 10.2.0 <see cref="LeafRetiredException"/> wire shape: id 0 only.</summary>
    [GenerateSerializer]
    public sealed class PreFenceLeafRetiredException : Exception
    {
        /// <summary>The 10.2.0 field.</summary>
        [Id(0)] public string LeafId { get; set; } = string.Empty;
    }
}
