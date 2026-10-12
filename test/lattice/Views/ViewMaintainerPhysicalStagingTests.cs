using System.Collections;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Actual maintainer drain/rebuild transitions with controlled source and WAL
/// transport. Physical logs intentionally reuse transaction ids and have
/// unrelated offsets; neither payload nor resume state may cross that seam.
/// </summary>
[TestFixture]
public sealed partial class ViewMaintainerPhysicalStagingTests
{
    private const string Logical = "orders";
    private const string Old = "orders-old";
    private const string New = "orders-new";
    private const string Third = "orders-third";
    private static readonly Guid Transaction = Guid.Parse("ad109f9c-1ae3-4779-84b1-044e9c7fb033");

    [TestCase(false)]
    [TestCase(true)]
    public async Task Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal(bool directRebuild)
        => await AssertPhysicalStagingAsync(directRebuild, mutation: 0);

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public void Drain_physical_staging_detector_rejects_restored_old_state_controls(int mutation)
    {
        var failure = Assert.ThrowsAsync<AssertionException>(async () =>
            await AssertPhysicalStagingAsync(directRebuild: true, mutation));
        Assert.That(failure!.Message, Does.Contain(mutation switch
        {
            1 => "old log's staged payload",
            2 => "new-only",
            _ => "candidate head of 2",
        }), "each control must fail at its intended public-output or physical-checkpoint assertion");
    }

    private static async Task AssertPhysicalStagingAsync(bool directRebuild, int mutation)
    {
        var h = await Harness.CreateAsync();
        h.Append(Old, 7, Prepare(Old, "old-only", 11));
        await h.Grain.DrainAsync();
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6),
            "the first pass really stages an unresolved old-physical prepare");
        Assert.That(h.AtomicWrites, Is.Empty);
        var oldStaging = SnapshotDictionary(h.Grain, "_staging");
        var oldReadOffsets = SnapshotDictionary(h.Grain, "_stagingReadOffsets");
        h.Alias = New;
        if (mutation == 3)
        {
            h.State.BeforeWrite = () =>
            {
                // Model incorrectly publishing the new binding with the old
                // physical checkpoint, at the actual persistence boundary.
                h.State.State.AppliedOffsets[0] = 6;
                return Task.CompletedTask;
            };
        }
        if (directRebuild)
        {
            await h.Grain.RebuildAsync();
        }
        else
        {
            await h.Grain.NotifySourceIdentityChangedAsync(New);
            await h.Grain.DrainAsync();
        }

        if (mutation == 1)
        {
            RestoreDictionary(h.Grain, "_staging", oldStaging);
            h.SourceRows[(New, "old-only")] = new LwwEntry
            {
                Key = "old-only", Value = [1], Timestamp = Clock(11),
            };
        }
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(New));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(1),
            "a candidate head of 2 is unrelated to the old prepare at offset 7");
        h.Append(New, 2, Terminal(New));
        if (mutation == 2)
        {
            // A stale in-activation cursor becomes dangerous only if its
            // physical label is also incorrectly changed to the new log.
            RestoreDictionary(h.Grain, "_staging", oldStaging);
            RestoreDictionary(h.Grain, "_stagingReadOffsets", oldReadOffsets);
            Field("_stagingReadWalTreeId").SetValue(h.Grain, New);
        }
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites, Is.Empty,
            "a terminal on the new physical log must not resolve an old log's staged payload with the same txid");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(1),
            "the unmatched new terminal remains replayable until its own prepare arrives");

        h.Append(New, 3, Prepare(New, "new-only", 21));
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key), Is.EqualTo(new[] { "new-only" }));
        Assert.That(h.AtomicWrites.Single().Tree, Is.EqualTo(await h.Grain.GetActiveTreeIdAsync()));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(3));
        Assert.That(h.Reads.Where(x => x.Tree == New).Select(x => x.From), Is.EqualTo(new long[] { 1, 2 }),
            "the new terminal and its late prepare must be read from new-log positions, never the old staging cursor");
    }

    private static FieldInfo Field(string name) => typeof(ViewMaintainerGrain)
        .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException($"Physical mutation anchor disappeared: {name}");

    private static IDictionary ReadDictionary(ViewMaintainerGrain grain, string name)
        => (IDictionary)(Field(name).GetValue(grain)
            ?? throw new InvalidOperationException($"Physical mutation anchor is null: {name}"));

    private static DictionaryEntry[] SnapshotDictionary(ViewMaintainerGrain grain, string name)
    {
        var entries = new List<DictionaryEntry>();
        foreach (DictionaryEntry entry in ReadDictionary(grain, name)) entries.Add(entry);
        return entries.ToArray();
    }

    private static void RestoreDictionary(ViewMaintainerGrain grain, string name, DictionaryEntry[] entries)
    {
        // Negative controls only: capture/restore state, never invoke a private
        // behavior. The detector still reaches staging/flush through DrainAsync.
        var dictionary = ReadDictionary(grain, name);
        dictionary.Clear();
        foreach (var entry in entries) dictionary.Add(entry.Key, entry.Value);
    }

    [Test]
    public async Task Rebuild_stale_scan_rejects_then_retry_publishes_the_retry_physical_tuple()
    {
        var h = await Harness.CreateAsync();
        h.Alias = New;
        h.OnScan = tree =>
        {
            Assert.That(tree, Is.EqualTo(New));
            h.Alias = Third;
            return Task.CompletedTask;
        };
        Assert.ThrowsAsync<StaleTreeRoutingException>(async () => await h.Grain.RebuildAsync());
        Assert.That(h.State.State.ActiveGeneration, Is.Zero);
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Old));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6));
        h.OnScan = null;

        await h.Grain.RebuildAsync();

        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Third));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(4));
        Assert.That(h.PublishedSnapshots.Single(), Is.EqualTo((Third, 1L, 4L)));
        await h.Cursors.DidNotReceive().ReportCursorAsync(New, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
        await h.Cursors.Received().ReportCursorAsync(Third, Arg.Any<string>(), Clock(50),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Rebuild_late_alias_move_installs_captured_tuple_then_notified_drain_heals()
    {
        var h = await Harness.CreateAsync();
        h.Alias = New;
        h.State.BeforeWrite = () =>
        {
            h.Alias = Third;
            return Task.CompletedTask;
        };

        await h.Grain.RebuildAsync();

        Assert.That(h.PublishedSnapshots.Single(), Is.EqualTo((New, 1L, 1L)),
            "an alias move after validation must not relabel the captured scan or its checkpoint");
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(New), Is.EqualTo(new long[] { 2 }));
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Third), Is.Null);
        await h.Cursors.Received().ReportCursorAsync(New, Arg.Any<string>(), Clock(20),
            Arg.Any<CancellationToken>());
        await h.Cursors.DidNotReceive().ReportCursorAsync(Third, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
        h.State.BeforeWrite = null;
        await h.Grain.NotifySourceIdentityChangedAsync(Third);
        await h.Grain.DrainAsync();

        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Third));
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(2));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(4));
        Assert.That(h.PublishedSnapshots.Last(), Is.EqualTo((Third, 2L, 4L)));
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(New), Is.Null);
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Third), Is.EqualTo(new long[] { 5 }));
    }

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    private static LatticeMutation Prepare(string tree, string key, long ticks) => new()
    {
        TreeId = tree, Key = key, Value = [1], Timestamp = Clock(ticks),
        Kind = MutationKind.Set, Category = MutationCategory.User, IsPrepared = true,
        TransactionId = Transaction, AtomicBatchSize = 1, AtomicBatchIndex = 0,
    };

    private static LatticeMutation Terminal(string tree) => new()
    {
        TreeId = tree, Kind = MutationKind.TxCommit, Category = MutationCategory.User,
        TransactionId = Transaction, AtomicShardCount = 1, Timestamp = Clock(30),
    };

    private sealed class Harness
    {
        public required ViewMaintainerGrain Grain { get; init; }
        public required FakePersistentState<ViewCheckpointState> State { get; init; }
        public required IWalCursorRegistry Cursors { get; init; }
        public string Alias { get; set; } = Old;
        public Func<string, Task>? OnScan { get; set; }
        public Action<string>? BeforeTailEntries { get; set; }
        public List<(string Tree, List<KeyValuePair<string, byte[]>> Writes)> AtomicWrites { get; } = [];
        public List<(string Tree, long From)> Reads { get; } = [];
        public List<(string Tree, long Generation, long Offset)> PublishedSnapshots { get; } = [];
        public Dictionary<(string Tree, string Key), LwwEntry> SourceRows { get; } = [];
        private readonly Dictionary<string, List<(long Offset, LatticeMutation Mutation)>> _logs = new();
        private readonly Dictionary<string, long> _heads = new() { [Old] = 7, [New] = 2, [Third] = 5 };

        public List<LwwEntry?> VisibleRows(string physical, List<string> keys)
        {
            var entries = _logs.GetValueOrDefault(physical, []);
            var committed = entries.Where(x => x.Mutation.Kind == MutationKind.TxCommit)
                .Select(x => x.Mutation.TransactionId).ToHashSet();
            return keys.Select(key =>
            {
                if (SourceRows.TryGetValue((physical, key), out var observation)) return (LwwEntry?)observation;
                LatticeMutation? winner = null;
                foreach (var (_, mutation) in entries)
                {
                    if (mutation.Key != key || mutation.Kind is not (MutationKind.Set or MutationKind.Delete)
                        || (mutation.IsPrepared && !committed.Contains(mutation.TransactionId))) continue;
                    if (winner is null || mutation.Timestamp > winner.Value.Timestamp) winner = mutation;
                }
                return winner is { } row
                    ? (LwwEntry?)new LwwEntry
                    {
                        Key = key, Value = row.Value, Timestamp = row.Timestamp,
                        IsTombstone = row.IsTombstone || row.Kind == MutationKind.Delete,
                        ExpiresAtTicks = row.ExpiresAtTicks,
                        OriginClusterId = row.OriginClusterId,
                        VectorClock = row.VectorClock?.Clone(),
                    }
                    : null;
            }).ToList();
        }

        public void Append(string tree, long offset, LatticeMutation mutation)
        {
            if (!_logs.TryGetValue(tree, out var entries)) _logs[tree] = entries = [];
            entries.Add((offset, mutation));
            _heads[tree] = Math.Max(_heads[tree], offset + 1);
        }

        public static async Task<Harness> CreateAsync()
        {
            Harness h = null!;
            var factory = Substitute.For<IGrainFactory>();
            var registry = Substitute.For<ILatticeRegistry>();
            registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
                new TreeRegistryEntry { PhysicalTreeId = h.Alias, ShardCount = 1, MaxLeafKeys = 128, MaxInternalChildren = 128 }));
            factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
            factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Arg.Any<string>())
                .Returns(Substitute.For<IWalOffsetConsumerRegistryGrain>());
            foreach (var physical in new[] { Old, New, Third })
            {
                var root = Substitute.For<IShardRootGrain>();
                root.GetVisibleRawEntriesAsync(Arg.Any<List<string>>())
                    .Returns(call => Task.FromResult(h.VisibleRows(physical, call.Arg<List<string>>())));
                factory.GetGrain<IShardRootGrain>($"{physical}/0").Returns(root);
            }
            var trees = new Dictionary<string, ILattice>();
            ILattice Tree(string id)
            {
                if (trees.TryGetValue(id, out var existing)) return existing;
                var tree = Substitute.For<ILattice>();
                var rows = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                trees.Add(id, tree);
                tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>()).Returns(call =>
                    Task.FromResult(call.Arg<List<string>>().Where(rows.ContainsKey)
                        .Distinct(StringComparer.Ordinal).ToDictionary(key => key, key => rows[key].ToArray(),
                            StringComparer.Ordinal)));
                tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>()).Returns(call =>
                {
                    rows[call.Arg<string>()] = call.Arg<byte[]>().ToArray();
                    return Task.CompletedTask;
                });
                tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(call =>
                    Task.FromResult(rows.Remove(call.Arg<string>())));
                tree.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(_ =>
                    new ValueTask<RoutingInfo>(new RoutingInfo(id == Logical ? h.Alias : id, ShardMap.CreateDefault(1, 1))));
                tree.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(_ =>
                    new ValueTask<RoutingInfo>(new RoutingInfo(id == Logical ? h.Alias : id, ShardMap.CreateDefault(1, 1))));
                tree.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                    .Returns(call => Keys(call.ArgAt<string?>(0), id == Old || id == New || id == Third));
                tree.GetWithVersionAsync("snapshot", Arg.Any<CancellationToken>()).Returns(async _ =>
                {
                    if (h.OnScan is { } onScan) await onScan(id);
                    return new VersionedValue { Value = [2], Version = Clock(id == Third ? 50 : id == New ? 20 : 10) };
                });
                tree.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<List<string>>(), Arg.Any<string>(),
                    Arg.Any<CancellationToken>()).Returns(call =>
                {
                    h.AtomicWrites.Add((id, [.. call.Arg<List<KeyValuePair<string, byte[]>>>()]));
                    foreach (var write in call.Arg<List<KeyValuePair<string, byte[]>>>())
                        rows[write.Key] = write.Value.ToArray();
                    foreach (var key in call.Arg<List<string>>()) rows.Remove(key);
                    return Task.FromResult(new AtomicWriteOutcome());
                });
                return tree;
            }
            factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call => Tree(call.ArgAt<string>(0)));
            var projection = Substitute.For<ILatticeViewProjection>();
            projection.Project(Arg.Any<LatticeMutation>()).Returns(call =>
            {
                var m = call.Arg<LatticeMutation>();
                return new[] { ViewWrite.Upsert(m.Key, m.Value!, m.Timestamp) };
            });
            var registration = new ViewRegistration("physical-staging", Logical, projection);
            var catalog = Substitute.For<IViewCatalog>();
            catalog.TryGet("physical-staging").Returns(registration);
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("viewmaintainer", "physical-staging"));
            context.ActivationServices.Returns(Substitute.For<IServiceProvider>());
            var state = new FakePersistentState<ViewCheckpointState>();
            state.State.BoundPhysicalTreeId = Old;
            state.State.LegacyGenerationCeiling = 0;
            state.State.AppliedOffsets[0] = 6;
            state.State.ProjectionVersion = registration.ProjectionVersion;
            var views = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
            views.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
            var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = 1, WalRetention = TimeSpan.Zero });
            var reader = Substitute.For<ICommitLogReader>();
            reader.GetHeadOffsetAsync(Arg.Any<string>(), 0, Arg.Any<CancellationToken>())
                .Returns(call => Task.FromResult(h._heads[call.ArgAt<string>(0)]));
            var subscriber = Substitute.For<IWalSubscriber>();
            subscriber.DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var c = call.Arg<WalSubscriptionContext>();
                    var handler = call.Arg<IWalSubscriptionHandler>();
                    var from = c.Checkpoints.GetValueOrDefault(0, -1);
                    h.Reads.Add((c.SourceTreeId, from));
                    var entries = h._logs.GetValueOrDefault(c.SourceTreeId, []).Where(x => x.Offset > from).ToArray();
                    h.BeforeTailEntries?.Invoke(c.SourceTreeId);
                    foreach (var e in entries) handler.OnEntry(new WalSubscriptionEntry(0, e.Offset, e.Mutation));
                    return Task.FromResult(new WalDrainResult
                    {
                        EntriesRead = entries.Length, EntriesSurfaced = entries.Length,
                        HighestTimestamp = entries.Aggregate(c.HighestApplied,
                            (highest, entry) => entry.Mutation.Timestamp > highest ? entry.Mutation.Timestamp : highest),
                        AdvancedOffsets = entries.Length == 0 ? new Dictionary<int, long>()
                            : new Dictionary<int, long> { [0] = entries.Max(x => x.Offset) },
                    });
                });
            var cursors = Substitute.For<IWalCursorRegistry>();
            var grain = new ViewMaintainerGrain(context, factory, null!, NullLogger<ViewMaintainerGrain>.Instance,
                catalog, reader, subscriber, cursors,
                TestOptionsResolver.ForFactory(factory, new LatticeOptions { WalPartitions = 1, WalRetention = TimeSpan.Zero }),
                views, options, Substitute.For<ILatticeReplicationContext>(), null, null!, state);
            h = new Harness { Grain = grain, State = state, Cursors = cursors };
            state.OnWriteState = s => h.PublishedSnapshots.Add((s.BoundPhysicalTreeId, s.ActiveGeneration,
                s.AppliedOffsets.GetValueOrDefault(0, -1)));
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
            return h;
        }

        private static async IAsyncEnumerable<string> Keys(string? start, bool source)
        {
            await Task.Yield();
            if (source && (start is null || string.CompareOrdinal(start, "snapshot") <= 0)) yield return "snapshot";
        }
    }
}
