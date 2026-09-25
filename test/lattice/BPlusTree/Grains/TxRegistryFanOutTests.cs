using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="TxRegistryFanOut"/>, the tree-wide aggregation over
/// every shard of the sharded saga decision registry plus the legacy registry
/// (issue #3501).
/// </summary>
[TestFixture]
public class TxRegistryFanOutTests
{
    private const string TreeId = "tree-fan";
    private const int Count = 3;

    private IGrainFactory _factory = null!;
    private Dictionary<string, ITxRegistryGrain> _registries = null!;

    [SetUp]
    public void SetUp()
    {
        _factory = Substitute.For<IGrainFactory>();
        _registries = new Dictionary<string, ITxRegistryGrain>(StringComparer.Ordinal);
        foreach (var key in TxRegistryRouting.EnumerateKeys(TreeId, Count))
        {
            var registry = Substitute.For<ITxRegistryGrain>();
            registry.SnapshotWithRevisionAsync().Returns(new TxRegistrySnapshot { Decisions = new(), Revision = 0 });
            registry.SnapshotAsync().Returns(new Dictionary<Guid, TxStatus>());
            _registries[key] = registry;
            _factory.GetGrain<ITxRegistryGrain>(key).Returns(registry);
        }
    }

    private ITxRegistryGrain Shard(int shard) => _registries[TxRegistryRouting.ShardKeyAt(TreeId, shard)];

    private ITxRegistryGrain Legacy => _registries[TreeId];

    private static Guid IdOnShard(int shard)
    {
        while (true)
        {
            var id = TxRegistryRouting.MintTransactionId(Count);
            if (TxRegistryRouting.ShardOf(id, Count) == shard) return id;
        }
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_unions_decisions_and_sums_revisions_across_every_key()
    {
        var a = IdOnShard(0);
        var b = IdOnShard(2);
        var legacy = Guid.NewGuid();
        Shard(0).SnapshotWithRevisionAsync().Returns(new TxRegistrySnapshot { Decisions = new() { [a] = TxStatus.Committed }, Revision = 3 });
        Shard(2).SnapshotWithRevisionAsync().Returns(new TxRegistrySnapshot { Decisions = new() { [b] = TxStatus.Aborted }, Revision = 5 });
        Legacy.SnapshotWithRevisionAsync().Returns(new TxRegistrySnapshot { Decisions = new() { [legacy] = TxStatus.Committed }, Revision = 7 });

        var snapshot = await TxRegistryFanOut.SnapshotWithRevisionAsync(_factory, TreeId, Count);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Revision, Is.EqualTo(15));
            Assert.That(snapshot.Decisions, Has.Count.EqualTo(3));
            Assert.That(snapshot.Decisions[a], Is.EqualTo(TxStatus.Committed));
            Assert.That(snapshot.Decisions[b], Is.EqualTo(TxStatus.Aborted));
            Assert.That(snapshot.Decisions[legacy], Is.EqualTo(TxStatus.Committed));
        });
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_with_count_one_reads_only_the_legacy_key()
    {
        var id = Guid.NewGuid();
        Legacy.SnapshotWithRevisionAsync().Returns(new TxRegistrySnapshot { Decisions = new() { [id] = TxStatus.Committed }, Revision = 9 });

        var snapshot = await TxRegistryFanOut.SnapshotWithRevisionAsync(_factory, TreeId, 1);

        Assert.That(snapshot.Revision, Is.EqualTo(9));
        await Shard(0).DidNotReceive().SnapshotWithRevisionAsync();
    }

    [Test]
    public async Task GetDecisionsRevisionAsync_sums_every_key()
    {
        Shard(0).GetDecisionsRevisionAsync().Returns(1L);
        Shard(1).GetDecisionsRevisionAsync().Returns(2L);
        Shard(2).GetDecisionsRevisionAsync().Returns(4L);
        Legacy.GetDecisionsRevisionAsync().Returns(8L);

        var sharded = await TxRegistryFanOut.GetDecisionsRevisionAsync(_factory, TreeId, Count);
        var legacyOnly = await TxRegistryFanOut.GetDecisionsRevisionAsync(_factory, TreeId, 1);

        Assert.Multiple(() =>
        {
            Assert.That(sharded, Is.EqualTo(15L));
            Assert.That(legacyOnly, Is.EqualTo(8L));
        });
    }

    [Test]
    public async Task SnapshotAsync_unions_every_key()
    {
        var a = IdOnShard(1);
        var legacy = Guid.NewGuid();
        Shard(1).SnapshotAsync().Returns(new Dictionary<Guid, TxStatus> { [a] = TxStatus.Committed });
        Legacy.SnapshotAsync().Returns(new Dictionary<Guid, TxStatus> { [legacy] = TxStatus.Aborted });

        var decisions = await TxRegistryFanOut.SnapshotAsync(_factory, TreeId, Count);

        Assert.That(decisions, Is.EquivalentTo(new Dictionary<Guid, TxStatus> { [a] = TxStatus.Committed, [legacy] = TxStatus.Aborted }));
    }

    [Test]
    public async Task StableSnapshotAsync_retries_until_the_bracketing_revision_matches()
    {
        var id = IdOnShard(0);
        Shard(0).SnapshotWithRevisionAsync().Returns(
            new TxRegistrySnapshot { Decisions = new(), Revision = 1 },
            new TxRegistrySnapshot { Decisions = new() { [id] = TxStatus.Committed }, Revision = 2 });
        // First bracket sees the revision move (1 -> 2); the second is stable.
        Shard(0).GetDecisionsRevisionAsync().Returns(2L, 2L);

        var decisions = await TxRegistryFanOut.StableSnapshotAsync(_factory, TreeId, Count);

        Assert.That(decisions, Contains.Key(id));
        await Shard(0).Received(2).SnapshotWithRevisionAsync();
    }

    [Test]
    public async Task StableSnapshotAsync_settles_after_the_attempt_budget_when_never_stable()
    {
        long revision = 0;
        Shard(0).SnapshotWithRevisionAsync().Returns(_ => new TxRegistrySnapshot { Decisions = new(), Revision = ++revision });
        Shard(0).GetDecisionsRevisionAsync().Returns(_ => ++revision);

        var decisions = await TxRegistryFanOut.StableSnapshotAsync(_factory, TreeId, Count);

        Assert.That(decisions, Is.Not.Null);
        await Shard(0).Received(TxRegistryFanOut.StableSnapshotAttempts).SnapshotWithRevisionAsync();
    }

    [Test]
    public async Task StableSnapshotAsync_with_count_one_reads_the_legacy_snapshot_once()
    {
        await TxRegistryFanOut.StableSnapshotAsync(_factory, TreeId, 1);

        await Legacy.Received(1).SnapshotAsync();
        await Legacy.DidNotReceive().GetDecisionsRevisionAsync();
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_sums_every_key()
    {
        Shard(0).ObserveCrossTreeInFlightAsync().Returns(new CrossTreeInFlightObservation(1, 10, 0));
        Shard(1).ObserveCrossTreeInFlightAsync().Returns(new CrossTreeInFlightObservation(2, 20, 1));
        Shard(2).ObserveCrossTreeInFlightAsync().Returns(new CrossTreeInFlightObservation(0, 30, 0));
        Legacy.ObserveCrossTreeInFlightAsync().Returns(new CrossTreeInFlightObservation(4, 40, 2));

        var observation = await TxRegistryFanOut.ObserveCrossTreeInFlightAsync(_factory, TreeId, Count);

        Assert.That(observation, Is.EqualTo(new CrossTreeInFlightObservation(7, 100, 3)));
    }

    [Test]
    public async Task GetStatusManyAsync_asks_each_owning_key_once_with_its_own_ids()
    {
        var a = IdOnShard(0);
        var b = IdOnShard(0);
        var c = IdOnShard(2);
        var legacy = Guid.NewGuid();
        Shard(0).GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(new Dictionary<Guid, TxStatus> { [a] = TxStatus.Committed, [b] = TxStatus.Aborted });
        Shard(2).GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(new Dictionary<Guid, TxStatus> { [c] = TxStatus.Committed });
        Legacy.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(new Dictionary<Guid, TxStatus> { [legacy] = TxStatus.Indeterminate });

        var merged = await TxRegistryFanOut.GetStatusManyAsync(_factory, TreeId, Count, [a, c, legacy, b]);

        Assert.That(merged, Has.Count.EqualTo(4));
        await Shard(0).Received(1).GetStatusManyAsync(Arg.Is<IReadOnlyList<Guid>>(l => l.Count == 2 && l.Contains(a) && l.Contains(b)));
        await Shard(2).Received(1).GetStatusManyAsync(Arg.Is<IReadOnlyList<Guid>>(l => l.Count == 1 && l[0] == c));
        await Legacy.Received(1).GetStatusManyAsync(Arg.Is<IReadOnlyList<Guid>>(l => l.Count == 1 && l[0] == legacy));
        await Shard(1).DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
    }

    [Test]
    public async Task GetStatusManyAsync_with_a_single_owning_key_returns_that_keys_answer()
    {
        var a = IdOnShard(1);
        var answer = new Dictionary<Guid, TxStatus> { [a] = TxStatus.Committed };
        Shard(1).GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(answer);

        var merged = await TxRegistryFanOut.GetStatusManyAsync(_factory, TreeId, Count, [a]);

        Assert.That(merged, Is.SameAs(answer));
    }

    [Test]
    public async Task PinSnapshotAsync_pins_only_the_owning_keys()
    {
        var pin = Guid.NewGuid();
        var a = IdOnShard(1);
        var legacy = Guid.NewGuid();
        var ttl = TimeSpan.FromMinutes(1);

        await TxRegistryFanOut.PinSnapshotAsync(_factory, TreeId, Count, pin, [a, legacy], ttl);

        await Shard(1).Received(1).PinSnapshotAsync(pin, Arg.Is<IReadOnlyCollection<Guid>>(c => c.Count == 1 && c.Contains(a)), ttl);
        await Legacy.Received(1).PinSnapshotAsync(pin, Arg.Is<IReadOnlyCollection<Guid>>(c => c.Count == 1 && c.Contains(legacy)), ttl);
        await Shard(0).DidNotReceive().PinSnapshotAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyCollection<Guid>>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RefreshPinAsync_is_true_only_when_every_owning_key_still_holds_the_pin()
    {
        var pin = Guid.NewGuid();
        var a = IdOnShard(0);
        var b = IdOnShard(2);
        var ttl = TimeSpan.FromMinutes(1);
        Shard(0).RefreshPinAsync(pin, ttl).Returns(true);
        Shard(2).RefreshPinAsync(pin, ttl).Returns(true);

        Assert.That(await TxRegistryFanOut.RefreshPinAsync(_factory, TreeId, Count, pin, [a, b], ttl), Is.True);

        Shard(2).RefreshPinAsync(pin, ttl).Returns(false);
        Assert.That(await TxRegistryFanOut.RefreshPinAsync(_factory, TreeId, Count, pin, [a, b], ttl), Is.False);
    }

    [Test]
    public async Task RefreshPinAsync_with_an_empty_set_falls_back_to_the_legacy_key()
    {
        var pin = Guid.NewGuid();
        var ttl = TimeSpan.FromMinutes(1);
        Legacy.RefreshPinAsync(pin, ttl).Returns(true);

        Assert.That(await TxRegistryFanOut.RefreshPinAsync(_factory, TreeId, Count, pin, [], ttl), Is.True);
        await Legacy.Received(1).RefreshPinAsync(pin, ttl);
    }

    [Test]
    public async Task UnpinSnapshotAsync_releases_the_pin_on_every_key()
    {
        var pin = Guid.NewGuid();

        await TxRegistryFanOut.UnpinSnapshotAsync(_factory, TreeId, Count, pin);

        foreach (var registry in _registries.Values)
        {
            await registry.Received(1).UnpinSnapshotAsync(pin);
        }
    }

    [Test]
    public async Task UnpinSnapshotAsync_with_count_one_releases_only_the_legacy_key()
    {
        var pin = Guid.NewGuid();

        await TxRegistryFanOut.UnpinSnapshotAsync(_factory, TreeId, 1, pin);

        await Legacy.Received(1).UnpinSnapshotAsync(pin);
        await Shard(0).DidNotReceive().UnpinSnapshotAsync(Arg.Any<Guid>());
    }

    [Test]
    public void GroupByKey_groups_ids_by_owning_key()
    {
        var a = IdOnShard(0);
        var b = IdOnShard(0);
        var legacy = Guid.NewGuid();

        var groups = TxRegistryFanOut.GroupByKey(TreeId, Count, [a, legacy, b]);

        Assert.Multiple(() =>
        {
            Assert.That(groups.Keys, Is.EquivalentTo(new[] { TxRegistryRouting.ShardKeyAt(TreeId, 0), TreeId }));
            Assert.That(groups[TxRegistryRouting.ShardKeyAt(TreeId, 0)], Is.EqualTo(new[] { a, b }));
            Assert.That(groups[TreeId], Is.EqualTo(new[] { legacy }));
        });
    }

    [Test]
    public void Fan_out_members_reject_a_null_factory_or_id_set()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => TxRegistryFanOut.SnapshotWithRevisionAsync(null!, TreeId, Count));
            Assert.ThrowsAsync<ArgumentNullException>(() => TxRegistryFanOut.GetDecisionsRevisionAsync(null!, TreeId, Count));
            Assert.ThrowsAsync<ArgumentNullException>(() => TxRegistryFanOut.SnapshotAsync(null!, TreeId, Count));
            Assert.ThrowsAsync<ArgumentNullException>(() => TxRegistryFanOut.StableSnapshotAsync(null!, TreeId, Count));
            Assert.ThrowsAsync<ArgumentNullException>(() => TxRegistryFanOut.ObserveCrossTreeInFlightAsync(null!, TreeId, Count));
            Assert.ThrowsAsync<ArgumentNullException>(() => TxRegistryFanOut.GetStatusManyAsync(_factory, TreeId, Count, null!));
            Assert.ThrowsAsync<ArgumentNullException>(() => TxRegistryFanOut.RefreshPinAsync(_factory, TreeId, Count, Guid.NewGuid(), null!, TimeSpan.FromSeconds(1)));
            Assert.Throws<ArgumentNullException>(() => TxRegistryFanOut.PinSnapshotAsync(_factory, TreeId, Count, Guid.NewGuid(), null!, TimeSpan.FromSeconds(1)));
            Assert.Throws<ArgumentNullException>(() => TxRegistryFanOut.UnpinSnapshotAsync(null!, TreeId, Count, Guid.NewGuid()));
        });
    }
}
