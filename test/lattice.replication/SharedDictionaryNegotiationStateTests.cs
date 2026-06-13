namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="SharedDictionaryNegotiationState"/>: null
/// guards, empty snapshot, record/snapshot round-trip, per-<c>(tree, peer)</c>
/// isolation, and reconnect overwrite semantics.
/// </summary>
[TestFixture]
public class SharedDictionaryNegotiationStateTests
{
    [Test]
    public void Record_throws_when_tree_is_null()
    {
        var state = new SharedDictionaryNegotiationState();

        Assert.That(
            () => state.Record(null!, "peer", default),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Record_throws_when_peer_is_null()
    {
        var state = new SharedDictionaryNegotiationState();

        Assert.That(
            () => state.Record("tree", null!, default),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Snapshot_is_empty_before_any_record()
    {
        var state = new SharedDictionaryNegotiationState();

        Assert.That(state.Snapshot(), Is.Empty);
    }

    [Test]
    public void Record_then_snapshot_round_trips_the_outcome()
    {
        var state = new SharedDictionaryNegotiationState();
        state.Record("tree-a", "peer-a",
            new SharedDictionaryNegotiationResult(7u, Matched: true, PeerCapabilityKnown: true, FellBack: false));

        var snap = state.Snapshot().Single();

        Assert.Multiple(() =>
        {
            Assert.That(snap.Tree, Is.EqualTo("tree-a"));
            Assert.That(snap.Peer, Is.EqualTo("peer-a"));
            Assert.That(snap.EffectiveDictionaryId, Is.EqualTo(7u));
            Assert.That(snap.Matched, Is.True);
            Assert.That(snap.PeerCapabilityKnown, Is.True);
            Assert.That(snap.FellBack, Is.False);
        });
    }

    [Test]
    public void Record_keeps_tree_peer_pairs_isolated()
    {
        var state = new SharedDictionaryNegotiationState();
        state.Record("tree-a", "peer-a",
            new SharedDictionaryNegotiationResult(7u, true, true, false));
        state.Record("tree-a", "peer-b",
            new SharedDictionaryNegotiationResult(0u, false, true, true));

        var snaps = state.Snapshot();
        Assert.That(snaps, Has.Count.EqualTo(2));

        var a = snaps.Single(s => s.Peer == "peer-a");
        var b = snaps.Single(s => s.Peer == "peer-b");
        Assert.Multiple(() =>
        {
            Assert.That(a.EffectiveDictionaryId, Is.EqualTo(7u));
            Assert.That(a.Matched, Is.True);
            Assert.That(b.EffectiveDictionaryId, Is.EqualTo(0u));
            Assert.That(b.FellBack, Is.True);
        });
    }

    [Test]
    public void Record_overwrites_prior_outcome_for_the_same_pair_on_reconnect()
    {
        var state = new SharedDictionaryNegotiationState();
        // First tick: peer capability unknown, fell back.
        state.Record("tree-a", "peer-a",
            new SharedDictionaryNegotiationResult(0u, false, false, true));
        // Peer reconnects and now advertises the configured dictionary.
        state.Record("tree-a", "peer-a",
            new SharedDictionaryNegotiationResult(7u, true, true, false));

        var snap = state.Snapshot().Single();

        Assert.Multiple(() =>
        {
            Assert.That(snap.EffectiveDictionaryId, Is.EqualTo(7u));
            Assert.That(snap.Matched, Is.True);
            Assert.That(snap.PeerCapabilityKnown, Is.True);
            Assert.That(snap.FellBack, Is.False);
        });
    }
}
