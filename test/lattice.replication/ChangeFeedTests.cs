using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ChangeFeedTests
{
    private const string Tree = "tree";
    private const string LocalCluster = "site-a";
    private const string RemoteCluster = "site-b";

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(int partitions, string clusterId = LocalCluster)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = clusterId,
            ReplogPartitions = partitions,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static WalRecord Entry(string key, HybridLogicalClock ts, string origin = LocalCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = origin,
    };

    private static IWalShardGrain Grain(params WalRecord[] entries)
    {
        var grain = Substitute.For<IWalShardGrain>();
        var sequenced = new WalShardSequencedEntry[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            sequenced[i] = new WalShardSequencedEntry { Sequence = i, Entry = entries[i] };
        }

        grain.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var from = (long)call[0];
                var max = (int)call[1];
                if (from >= sequenced.Length)
                {
                    return Task.FromResult(WalShardPage.Empty(from));
                }

                var available = sequenced.Length - (int)from;
                var take = Math.Min(max, available);
                var page = new WalShardSequencedEntry[take];
                Array.Copy(sequenced, (int)from, page, 0, take);
                return Task.FromResult(new WalShardPage
                {
                    Entries = page,
                    NextSequence = from + take,
                });
            });
        return grain;
    }

    private static (ChangeFeed Feed, IGrainFactory Factory) CreateFeed(int partitions, string clusterId = LocalCluster)
    {
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var feed = new ChangeFeed(factory, Monitor(partitions, clusterId), resolver);
        return (feed, factory);
    }

    private static async Task<List<WalRecord>> CollectAsync(IAsyncEnumerable<WalRecord> source)
    {
        var result = new List<WalRecord>();
        await foreach (var entry in source)
        {
            result.Add(entry);
        }
        return result;
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void Subscribe_throws_when_tree_name_is_null()
    {
        var (feed, _) = CreateFeed(partitions: 1);

        Assert.That(
            async () => await CollectAsync(feed.Subscribe(null!, HybridLogicalClock.Zero)),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Subscribe_returns_empty_stream_when_wal_is_empty()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var empty = Grain();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(empty);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task Subscribe_yields_entries_with_timestamp_strictly_greater_than_cursor()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("a", Hlc(1)),
            Entry("b", Hlc(5)),
            Entry("c", Hlc(10)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, Hlc(5)));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public async Task Subscribe_emits_every_entry_when_cursor_is_zero()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(Entry("a", Hlc(1)), Entry("b", Hlc(2)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Subscribe_yields_in_hlc_ascending_order_across_partitions()
    {
        var (feed, factory) = CreateFeed(partitions: 2);
        var p0 = Grain(Entry("p0a", Hlc(1)), Entry("p0b", Hlc(4)));
        var p1 = Grain(Entry("p1a", Hlc(2)), Entry("p1b", Hlc(3)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(p0);
        factory.GetGrain<IWalShardGrain>($"{Tree}/1").Returns(p1);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "p0a", "p1a", "p1b", "p0b" }));
    }

    [Test]
    public async Task Subscribe_walks_every_partition_indicated_by_options()
    {
        var (feed, factory) = CreateFeed(partitions: 4);
        var empty = Grain();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(empty);

        await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        for (var p = 0; p < 4; p++)
        {
            factory.Received(1).GetGrain<IWalShardGrain>($"{Tree}/{p}");
        }
    }

    [Test]
    public async Task Subscribe_excludes_local_origin_when_include_local_origin_is_false()
    {
        // Under the WAL-as-sole-durability-boundary contract the WAL
        // captures foreign-origin entries installed by
        // `IReplicationApplier`; the change-feed contract
        // ("locally-authored writes only") drops them unconditionally,
        // so this test seeds only locally-authored rows (an empty-
        // origin durability writer record and a local-origin observer
        // record) to exercise the `includeLocalOrigin` filter in
        // isolation.
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("local-stamped", Hlc(1), origin: LocalCluster),
            Entry("durability-only", Hlc(2), origin: string.Empty));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(
            feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        // Local-origin entry is suppressed by the filter; the
        // empty-origin durability-only entry survives because its
        // origin does not match the local cluster id.
        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "durability-only" }));
    }

    [Test]
    public async Task Subscribe_includes_local_origin_by_default()
    {
        // Default `includeLocalOrigin=true` keeps both local-origin
        // observer entries and empty-origin durability entries.
        // Foreign-origin (apply-installed) entries remain filtered
        // unconditionally and are covered by the dedicated regressions
        // in `Subscribe_filters_foreign_origin_entries_*` below.
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("local", Hlc(1), origin: LocalCluster),
            Entry("durability-only", Hlc(2), origin: string.Empty));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "local", "durability-only" }));
    }

    [Test]
    public async Task Subscribe_does_not_filter_durability_only_entries_when_include_local_origin_is_false()
    {
        // Empty-origin (durability-only) entries are produced by the
        // local `ICommitLogWriter` path on every authored write. They
        // do not match the local cluster id, so the
        // `includeLocalOrigin=false` filter must not suppress them -
        // the filter targets observer-stamped local entries
        // specifically. Foreign-origin entries are independently
        // dropped by the apply-installed guard and are excluded from
        // this scenario.
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("durability-a", Hlc(1), origin: string.Empty),
            Entry("durability-b", Hlc(2), origin: string.Empty));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(
            feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "durability-a", "durability-b" }));
    }

    [Test]
    public async Task Subscribe_keeps_entries_with_null_origin_when_include_local_origin_is_false()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(Entry("nullorigin", Hlc(1)) with { OriginClusterId = null });
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(
            feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "nullorigin" }));
    }

    [Test]
    public async Task Subscribe_pages_through_more_entries_than_page_size()
    {
        const int total = 600; // greater than the internal PageSize of 256.
        var entries = new WalRecord[total];
        for (var i = 0; i < total; i++)
        {
            entries[i] = Entry($"k{i:D4}", Hlc(i + 1));
        }

        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(entries);
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var result = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(result, Has.Count.EqualTo(total));
        Assert.That(result[0].Key, Is.EqualTo("k0000"));
        Assert.That(result[^1].Key, Is.EqualTo($"k{total - 1:D4}"));
    }

    [Test]
    public void Subscribe_observes_cancellation()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(Entry("a", Hlc(1)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero, cancellationToken: cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Subscribe_does_not_read_partitions_outside_configured_count()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var empty = Grain();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(empty);

        await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        factory.Received(1).GetGrain<IWalShardGrain>($"{Tree}/0");
        factory.DidNotReceive().GetGrain<IWalShardGrain>($"{Tree}/1");
    }

    [Test]
    public async Task Subscribe_resolves_options_using_tree_name()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ReplogPartitions = 1,
        });
        var factory = Substitute.For<IGrainFactory>();
        var empty = Grain();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(empty);
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var feed = new ChangeFeed(factory, monitor, resolver);

        await CollectAsync(feed.Subscribe("alpha", HybridLogicalClock.Zero));
        await CollectAsync(feed.Subscribe("beta", HybridLogicalClock.Zero));

        monitor.Received().Get("alpha");
        monitor.Received().Get("beta");
    }

    // --- Tombstone-reap filtering ---

    private static WalRecord TombstoneReapEntry(string key, HybridLogicalClock ts, string origin = LocalCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Tombstone,
        Key = key,
        Timestamp = ts,
        IsTombstone = true,
        OriginClusterId = origin,
    };

    [Test]
    public async Task Subscribe_filters_out_tombstone_reap_envelopes()
    {
        // Tombstone-reap envelopes record a local structural cleanup
        // (see `BPlusLeafGrain.CompactTombstonesAsync`). The change
        // feed must drop them so bootstrap and replication consumers
        // never observe a kind they have no apply rule for.
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("alive", Hlc(1)),
            TombstoneReapEntry("dead", Hlc(2)),
            Entry("alive2", Hlc(3)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "alive", "alive2" }));
        Assert.That(entries.All(e => e.Op != MutationKind.Tombstone), Is.True,
            "tombstone-reap envelopes must be filtered out at the change-feed boundary");
    }

    [Test]
    public async Task Subscribe_filters_tombstone_reap_independently_of_origin_filter()
    {
        // A tombstone-reap whose OriginClusterId is local must still be
        // dropped on the Op classification, not on origin. (Foreign-
        // origin entries are independently filtered by the receiver-
        // apply guard - see `Subscribe_filters_foreign_origin_entries`.)
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            TombstoneReapEntry("dead-local", Hlc(1), origin: LocalCluster),
            Entry("alive-local", Hlc(2), origin: LocalCluster));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: true));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "alive-local" }));
    }

    // --- Foreign-origin (apply-installed) filtering ---

    [Test]
    public async Task Subscribe_filters_foreign_origin_entries_under_wal_as_sole_durability_boundary()
    {
        // Receiver-apply contract regression: under the
        // WAL-as-sole-durability-boundary contract the per-shard WAL
        // captures every leaf commit, including entries installed by
        // `IReplicationApplier` on this cluster (those entries stamp
        // `OriginClusterId` with the *source* cluster). The change
        // feed's documented contract ("locally-authored writes only")
        // requires those foreign-origin records to be dropped at the
        // feed boundary so the outbound ship loop and bootstrap
        // consumers do not re-emit a peer's writes back across the
        // wire. Without this filter, a three-cluster topology
        // (A authors -> B applies -> B's feed surfaces the A-origin
        // entry -> C consumes B's feed and observes A's entries as
        // if they were B's) would loop.
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("local", Hlc(1), origin: LocalCluster),
            Entry("foreign", Hlc(2), origin: RemoteCluster),
            Entry("durability-only", Hlc(3), origin: string.Empty));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        // Foreign-origin entries are dropped. Local-origin and
        // empty-origin entries (durability-only authoring records) are
        // retained because they represent locally-authored writes.
        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "local", "durability-only" }));
    }

    [Test]
    public async Task Subscribe_filters_foreign_origin_entries_even_when_include_local_origin_disabled()
    {
        // The foreign-origin filter is independent of
        // `includeLocalOrigin`: with the flag disabled, local entries
        // are also suppressed, but foreign-origin entries remain
        // filtered out (they were never eligible to appear).
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("local", Hlc(1), origin: LocalCluster),
            Entry("foreign", Hlc(2), origin: RemoteCluster),
            Entry("durability-only", Hlc(3), origin: string.Empty));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        // Only the durability-only entry (empty origin) survives.
        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "durability-only" }));
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        Assert.That(
            () => new ChangeFeed(null!, Monitor(partitions: 1), resolver),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        Assert.That(
            () => new ChangeFeed(factory, null!, resolver),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_mode_resolver_is_null()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => new ChangeFeed(factory, Monitor(partitions: 1), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Subscribe_restamps_mode_on_every_yielded_entry_from_resolver()
    {
        // Regression: `WalRecord.Mode` is `[field: NonSerialized]` so
        // the canonical Orleans codec drops the slot on every grain
        // RPC return path. `IWalShardGrain.ReadAsync` stamps Mode on
        // the silo side via its injected resolver, but that stamp is
        // erased on the way back to the client. Without re-stamping
        // at the change-feed seam, every CRDT mode collapses to the
        // default `LwwRegister` and `ReplicationApplier` dispatches
        // state-merge through the LWW branch instead of the configured
        // CRDT branch. This test pins the seam: every yielded entry
        // carries the mode returned by the injected
        // `ILatticeMergeModeResolver`, regardless of whatever `Mode`
        // value the underlying grain handed back.
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns(LatticeMergeMode.OrSet);
        var feed = new ChangeFeed(factory, Monitor(partitions: 1), resolver);
        // Entries arrive from the grain stamped with the wire default
        // (Orleans drops the [field: NonSerialized] property), which
        // happens to be `LwwRegister`. The change feed must override it.
        var grain = Grain(
            Entry("k1", Hlc(1)) with { Mode = LatticeMergeMode.LwwRegister },
            Entry("k2", Hlc(2)) with { Mode = LatticeMergeMode.LwwRegister });
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Mode), Is.All.EqualTo(LatticeMergeMode.OrSet));
        resolver.Received().Resolve(Tree);
    }

    [Test]
    public async Task Subscribe_falls_back_to_lww_register_when_resolver_returns_null()
    {
        // When the resolver returns null (tree not in the replicated set
        // for this silo), the change feed falls back to `LwwRegister`
        // rather than yielding `default(LatticeMergeMode)`. This mirrors
        // the same fallback used by `ReplicationShipperGrain` and the
        // gRPC marshalling path so receivers see one consistent default.
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns((LatticeMergeMode?)null);
        var feed = new ChangeFeed(factory, Monitor(partitions: 1), resolver);
        var grain = Grain(Entry("k1", Hlc(1)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(entries[0].Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
    }
}
