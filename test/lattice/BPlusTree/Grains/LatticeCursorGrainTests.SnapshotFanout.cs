using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage of the snapshot-cursor fan-out paths that the existing
/// <c>LatticeCursorGrainTests.Snapshot</c> partial leaves unexercised: the
/// best-effort WAL-pin and frozen-baseline cleanup arms (each of which must
/// swallow its failure rather than fail the caller), the persisted-state
/// rollback every <c>Next*</c> page performs when <c>WriteStateAsync</c>
/// throws, the read-path access-gate key filter that snapshot pages re-apply
/// because they bypass the public filtered scan surface, the raw-entry
/// (<see cref="LwwEntry"/>) drain used by the backup capture engine, and the
/// legacy coordinate-hash snapshot-leaf key derivation taken by a
/// from-zero replay coordinate that carries no per-open baseline token.
/// </summary>
public partial class LatticeCursorGrainTests
{
    /// <summary>
    /// Hand-written <see cref="ILatticeAccessGate"/> fake. NSubstitute cannot
    /// mock the <c>in</c> parameter on
    /// <see cref="ILatticeAccessGate.AuthorizeAsync"/>, so snapshot key-filter
    /// tests drive the gate through this double.
    /// </summary>
    private sealed class SnapshotGate(Func<LatticeAccessRequest, LatticeAccessDecision> decide)
        : ILatticeAccessGate
    {
        public int CallCount { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            var copy = request;
            CallCount++;
            return new ValueTask<LatticeAccessDecision>(decide(copy));
        }
    }

    /// <summary>
    /// Builds a cursor grain whose activation service provider is configurable,
    /// so a test can register an <see cref="ILatticeAccessGate"/> or an
    /// <see cref="IWalCursorRegistry"/> that the grain resolves through
    /// <c>services.GetService</c>. Mirrors <c>CreateGrainWithFactory</c>
    /// otherwise.
    /// </summary>
    private static (LatticeCursorGrain grain,
                    FakePersistentState<LatticeCursorState> state,
                    IGrainFactory grainFactory) CreateSnapshotGrain(
        Action<ServiceCollection>? configureServices = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice-cursor", $"{TreeId}/{CursorId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);
        var txRegistry = Substitute.For<ITxRegistryGrain>();
        grainFactory.GetGrain<ITxRegistryGrain>(TreeId).Returns(txRegistry);
        WireCatalogue(grainFactory);

        var reminders = Substitute.For<IReminderRegistry>();
        var reminder = Substitute.For<IGrainReminder>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        var opts = new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var services = new ServiceCollection();
        configureServices?.Invoke(services);

        var state = new FakePersistentState<LatticeCursorState>();
        var grain = new LatticeCursorGrain(
            context,
            grainFactory,
            reminders,
            optionsMonitor,
            services.BuildServiceProvider(),
            new LoggerFactory().CreateLogger<LatticeCursorGrain>(),
            state);
        return (grain, state, grainFactory);
    }

    private static LwwEntry RawEntry(string key, long ticks = 1) => new()
    {
        Key = key,
        Value = [1, 2, 3],
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
        IsTombstone = false,
    };

    /// <summary>
    /// Registers a snapshot leaf for <paramref name="shardIndex"/> that answers
    /// the entries-shape drain with <paramref name="entries"/>.
    /// </summary>
    private static ISnapshotLeafGrain WireEntriesShard(
        IGrainFactory grainFactory, Guid token, int shardIndex,
        List<KeyValuePair<string, byte[]>> entries)
    {
        var leaf = Substitute.For<ISnapshotLeafGrain>();
        leaf.GetEntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<bool>())
            .Returns(_ => Task.FromResult(new List<KeyValuePair<string, byte[]>>(entries)));
        grainFactory.GetGrain<ISnapshotLeafGrain>(
            SnapshotLeafGrain.BuildBaselineKey(TreeId, shardIndex, token)).Returns(leaf);
        return leaf;
    }

    /// <summary>
    /// Registers a snapshot leaf for <paramref name="shardIndex"/> that answers
    /// the raw-entry drain with <paramref name="entries"/>.
    /// </summary>
    private static ISnapshotLeafGrain WireRawShard(
        IGrainFactory grainFactory, Guid token, int shardIndex, List<LwwEntry> entries)
    {
        var leaf = Substitute.For<ISnapshotLeafGrain>();
        leaf.GetRawEntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<bool>())
            .Returns(_ => Task.FromResult(new List<LwwEntry>(entries)));
        grainFactory.GetGrain<ISnapshotLeafGrain>(
            SnapshotLeafGrain.BuildBaselineKey(TreeId, shardIndex, token)).Returns(leaf);
        return leaf;
    }

    private static LatticeCursorSpec SnapshotSpec(
        LatticeCursorKind kind = LatticeCursorKind.Keys, bool reverse = false) => new()
        {
            Kind = kind,
            ZeroObservableWrites = true,
            Reverse = reverse,
        };

    // --- Persisted-state rollback on a failing WriteStateAsync ---

    [Test]
    public void OpenSnapshotAsync_reverts_every_snapshot_field_when_WriteStateAsync_throws()
    {
        var (grain, state, _) = CreateSnapshotGrain();
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.OpenSnapshotAsync(
                TreeId, SnapshotSpec(), MakeCoordinate(treeMapVersion: 9, (0, 42))));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
                "Phase must stay NotStarted in-memory so a retry re-enters the guarded open branch.");
            Assert.That(state.State.TreeId, Is.Empty,
                "TreeId must not retain the unpersisted mutation, or the spec-mismatch guard rejects valid retries.");
            Assert.That(state.State.SnapshotCoordinate, Is.Null,
                "An unpersisted coordinate must not survive in memory; a later page would replay against a cut disk never saw.");
            Assert.That(state.State.SnapshotPinId, Is.EqualTo(Guid.Empty),
                "The freshly-minted pin id must be rolled back with the rest of the open.");
        });
    }

    [Test]
    public async Task NextKeysAsync_snapshot_reverts_state_when_WriteStateAsync_throws()
    {
        var token = Guid.NewGuid();
        var (grain, state, factory) = CreateSnapshotGrain();
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextKeysAsync(3));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LastYieldedKey, Is.Null,
                "The unpersisted continuation key must be rolled back, or a retry silently skips keys.");
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open),
                "Phase must stay Open so the retry re-drains rather than short-circuiting as exhausted.");
            Assert.That(state.State.SnapshotBaselinePersisted, Is.False,
                "The baseline-persisted flag is set before the write, so a failed write must clear it again.");
        });
    }

    [Test]
    public async Task NextEntriesAsync_snapshot_reverts_state_when_WriteStateAsync_throws()
    {
        var token = Guid.NewGuid();
        var (grain, state, factory) = CreateSnapshotGrain();
        WireEntriesShard(factory, token, 0,
        [
            new KeyValuePair<string, byte[]>("a", [1]),
            new KeyValuePair<string, byte[]>("b", [2]),
            new KeyValuePair<string, byte[]>("c", [3]),
        ]);

        await grain.OpenSnapshotAsync(
            TreeId, SnapshotSpec(LatticeCursorKind.Entries), MakeTokenCoordinate(token, (0, 0)));
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextEntriesAsync(3));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LastYieldedKey, Is.Null);
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open));
            Assert.That(state.State.SnapshotBaselinePersisted, Is.False);
        });
    }

    [Test]
    public async Task NextRawEntriesAsync_reverts_state_when_WriteStateAsync_throws()
    {
        var token = Guid.NewGuid();
        var (grain, state, factory) = CreateSnapshotGrain();
        WireRawShard(factory, token, 0, [RawEntry("a"), RawEntry("b"), RawEntry("c")]);

        await grain.OpenSnapshotAsync(
            TreeId, SnapshotSpec(LatticeCursorKind.Entries), MakeTokenCoordinate(token, (0, 0)));
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NextRawEntriesAsync(3));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LastYieldedKey, Is.Null);
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open));
            Assert.That(state.State.SnapshotBaselinePersisted, Is.False);
        });
    }

    // --- Best-effort WAL retention pin ---

    [Test]
    public async Task OpenSnapshotAsync_swallows_a_failing_cursor_registry_report()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.ReportCursorAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
                Arg.Any<HybridLogicalClock?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("registry unavailable")));
        var (grain, state, _) = CreateSnapshotGrain(s => s.AddSingleton(registry));

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeCoordinate(shards: (0, 5)));

        await registry.Received(1).ReportCursorAsync(
            TreeId, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<HybridLogicalClock?>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open),
            "A registry failure is best-effort: the open must still succeed and the cursor stay usable.");
    }

    [Test]
    public async Task CloseAsync_swallows_a_failing_cursor_registry_unregister()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("registry unavailable")));
        var (grain, state, _) = CreateSnapshotGrain(s => s.AddSingleton(registry));

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeCoordinate(shards: (0, 5)));

        await grain.CloseAsync();

        await registry.Received(1).UnregisterAsync(
            TreeId, Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
            "A failed unregister must not abort the close; the persisted state is still cleared.");
    }

    [Test]
    public async Task CloseAsync_swallows_a_failing_frozen_baseline_delete()
    {
        var token = Guid.NewGuid();
        var (grain, state, factory) = CreateSnapshotGrain();
        var (_, baseline) = WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);
        baseline.ClearAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("storage unavailable")));

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));
        await grain.NextKeysAsync(3);
        Assert.That(state.State.SnapshotBaselinePersisted, Is.True,
            "Guard: the multi-page drain must have flushed the baselines, or the delete arm is never reached.");

        await grain.CloseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
                "A failed baseline delete leaves only an orphaned row; the close itself must still complete.");
            Assert.That(state.State.SnapshotCoordinate, Is.Null,
                "The cleared state must not retain the snapshot coordinate.");
        });
    }

    // --- Read-path access-gate key filter re-applied at page emit ---

    [Test]
    public async Task NextKeysAsync_snapshot_prunes_keys_the_access_gate_filters_out()
    {
        var token = Guid.NewGuid();
        var gate = new SnapshotGate(_ => LatticeAccessDecision.Filtered(k => k != "b"));
        var (grain, _, factory) = CreateSnapshotGrain(s => s.AddSingleton<ILatticeAccessGate>(gate));
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));
        var page = await grain.NextKeysAsync(10);

        Assert.Multiple(() =>
        {
            Assert.That(page.Keys, Is.EqualTo(new[] { "a", "c" }).AsCollection,
                "Snapshot leaf reads bypass the public filtered surface, so the cursor must re-apply the key filter.");
            Assert.That(gate.CallCount, Is.EqualTo(1),
                "The gate is consulted once per page, not once per key.");
        });
    }

    [Test]
    public async Task NextKeysAsync_snapshot_prunes_every_key_when_the_access_gate_denies()
    {
        var token = Guid.NewGuid();
        var gate = new SnapshotGate(_ => LatticeAccessDecision.Deny("no read"));
        var (grain, _, factory) = CreateSnapshotGrain(s => s.AddSingleton<ILatticeAccessGate>(gate));
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));
        var page = await grain.NextKeysAsync(10);

        Assert.That(page.Keys, Is.Empty,
            "A full deny must fail closed and prune the whole page.");
    }

    [Test]
    public async Task NextEntriesAsync_snapshot_prunes_entries_the_access_gate_filters_out()
    {
        var token = Guid.NewGuid();
        var gate = new SnapshotGate(_ => LatticeAccessDecision.Filtered(k => k != "b"));
        var (grain, _, factory) = CreateSnapshotGrain(s => s.AddSingleton<ILatticeAccessGate>(gate));
        WireEntriesShard(factory, token, 0,
        [
            new KeyValuePair<string, byte[]>("a", [1]),
            new KeyValuePair<string, byte[]>("b", [2]),
            new KeyValuePair<string, byte[]>("c", [3]),
        ]);

        await grain.OpenSnapshotAsync(
            TreeId, SnapshotSpec(LatticeCursorKind.Entries), MakeTokenCoordinate(token, (0, 0)));
        var page = await grain.NextEntriesAsync(10);

        Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "c" }).AsCollection,
            "The entries page must re-apply the same read-path key filter as the keys page.");
    }

    [Test]
    public async Task NextRawEntriesAsync_prunes_entries_the_access_gate_filters_out()
    {
        var token = Guid.NewGuid();
        var gate = new SnapshotGate(_ => LatticeAccessDecision.Filtered(k => k != "b"));
        var (grain, _, factory) = CreateSnapshotGrain(s => s.AddSingleton<ILatticeAccessGate>(gate));
        WireRawShard(factory, token, 0, [RawEntry("a"), RawEntry("b"), RawEntry("c")]);

        await grain.OpenSnapshotAsync(
            TreeId, SnapshotSpec(LatticeCursorKind.Entries), MakeTokenCoordinate(token, (0, 0)));
        var page = await grain.NextRawEntriesAsync(10);

        Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "c" }).AsCollection,
            "The raw-entry drain feeds backup capture and must be filtered exactly like the projection path.");
    }

    // --- Raw-entry drain: k-way merge, pagination and lazy baseline flush ---

    [Test]
    public async Task NextRawEntriesAsync_merges_shards_and_preserves_the_causal_envelope()
    {
        var token = Guid.NewGuid();
        var (grain, _, factory) = CreateSnapshotGrain();
        WireRawShard(factory, token, 0, [RawEntry("a", ticks: 11), RawEntry("c", ticks: 13)]);
        WireRawShard(factory, token, 1, [RawEntry("b", ticks: 12)]);

        await grain.OpenSnapshotAsync(
            TreeId, SnapshotSpec(LatticeCursorKind.Entries), MakeTokenCoordinate(token, (0, 0), (1, 0)));
        var page = await grain.NextRawEntriesAsync(10);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", "c" }).AsCollection,
                "The raw drain must k-way merge the per-shard runs into one ordinal-sorted page.");
            Assert.That(page.Entries.Select(e => e.Timestamp.WallClockTicks),
                Is.EqualTo(new long[] { 11, 12, 13 }).AsCollection,
                "The HLC envelope must survive the merge intact - that is the whole point of the raw shape.");
        });
    }

    [Test]
    public async Task NextRawEntriesAsync_multi_page_reports_hasMore_and_flushes_baselines_once()
    {
        // A full page whose shards still hold entries must report HasMore, which
        // drives the raw path's lazy frozen-baseline flush before the
        // continuation token escapes to the client (issue #916).
        var token = Guid.NewGuid();
        var (grain, state, factory) = CreateSnapshotGrain();
        var leaf0 = WireRawShard(factory, token, 0, [RawEntry("a"), RawEntry("c")]);
        var leaf1 = WireRawShard(factory, token, 1, [RawEntry("b"), RawEntry("d")]);

        await grain.OpenSnapshotAsync(
            TreeId, SnapshotSpec(LatticeCursorKind.Entries), MakeTokenCoordinate(token, (0, 0), (1, 0)));

        var page = await grain.NextRawEntriesAsync(3);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(3),
                "The merge must fill the page exactly to the cap.");
            Assert.That(page.HasMore, Is.True,
                "Shards still holding entries beyond the cap must keep the cursor live.");
            Assert.That(state.State.SnapshotBaselinePersisted, Is.True,
                "A raw drain that survives past page 1 must mark its baselines persisted.");
        });
        await leaf0.Received(1).EnsurePersistedAsync(Arg.Any<CancellationToken>());
        await leaf1.Received(1).EnsurePersistedAsync(Arg.Any<CancellationToken>());

        await grain.NextRawEntriesAsync(3);

        await leaf0.Received(1).EnsurePersistedAsync(Arg.Any<CancellationToken>());
        await leaf1.Received(1).EnsurePersistedAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task NextRawEntriesAsync_reverse_merges_descending_across_an_exhausted_shard()
    {
        // Shard 0 runs dry after its single entry while shard 1 still has two, so
        // the reverse merge must skip the exhausted shard on every later pass
        // rather than indexing past the start of its run.
        var token = Guid.NewGuid();
        var (grain, _, factory) = CreateSnapshotGrain();
        WireRawShard(factory, token, 0, [RawEntry("c")]);
        WireRawShard(factory, token, 1, [RawEntry("a"), RawEntry("b")]);

        await grain.OpenSnapshotAsync(
            TreeId,
            SnapshotSpec(LatticeCursorKind.Entries, reverse: true),
            MakeTokenCoordinate(token, (0, 0), (1, 0)));

        var page = await grain.NextRawEntriesAsync(3);

        Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "c", "b", "a" }).AsCollection,
            "A reverse raw drain must yield descending keys across shards of unequal length.");
    }

    [Test]
    public async Task NextRawEntriesAsync_deduplicates_a_key_present_in_two_shards()
    {
        var token = Guid.NewGuid();
        var (grain, _, factory) = CreateSnapshotGrain();
        WireRawShard(factory, token, 0, [RawEntry("a"), RawEntry("b")]);
        WireRawShard(factory, token, 1, [RawEntry("b"), RawEntry("c")]);

        await grain.OpenSnapshotAsync(
            TreeId, SnapshotSpec(LatticeCursorKind.Entries), MakeTokenCoordinate(token, (0, 0), (1, 0)));
        var page = await grain.NextRawEntriesAsync(10);

        Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", "c" }).AsCollection,
            "The merge must collapse a key that two shards both report rather than emitting it twice.");
    }

    [Test]
    public void NextRawEntriesAsync_rejects_a_non_snapshot_cursor()
    {
        var (grain, _, _) = CreateSnapshotGrain();

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await grain.OpenAsync(TreeId, new LatticeCursorSpec { Kind = LatticeCursorKind.Entries });
            await grain.NextRawEntriesAsync(10);
        }, "Raw metadata is only well-defined against a pinned point-in-time cut.");
    }

    // --- Legacy from-zero coordinate: coordinate-hash snapshot-leaf key ---

    /// <summary>
    /// Opens a snapshot cursor over a legacy coordinate (no per-open baseline
    /// token) and returns the snapshot-leaf grain keys the cursor derived, in
    /// the order it requested them.
    /// </summary>
    private static async Task<List<string>> CaptureLegacyLeafKeysAsync(
        LatticeSnapshotCoordinate coordinate)
    {
        var (grain, _, factory) = CreateSnapshotGrain();
        var captured = new List<string>();
        var leaf = Substitute.For<ISnapshotLeafGrain>();
        leaf.GetKeysAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<bool>())
            .Returns(_ => Task.FromResult(new List<string>()));
        factory.GetGrain<ISnapshotLeafGrain>(Arg.Any<string>())
            .Returns(call =>
            {
                captured.Add((string)call[0]!);
                return leaf;
            });

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), coordinate);
        await grain.NextKeysAsync(10);
        return captured;
    }

    [Test]
    public async Task Legacy_coordinate_without_a_baseline_token_uses_a_coordinate_hash_leaf_key()
    {
        var keys = await CaptureLegacyLeafKeysAsync(MakeCoordinate(treeMapVersion: 4, (2, 7), (0, 3)));

        Assert.That(keys, Has.Count.EqualTo(2),
            "One snapshot leaf must be addressed per shard in the coordinate.");
        Assert.Multiple(() =>
        {
            foreach (var key in keys)
            {
                Assert.That(key, Does.Match(@"^" + TreeId + @"/\d+/[0-9a-f]{16}$"),
                    "A token-less coordinate must fall back to the {treeId}/{shard}/{coordHash} key shape.");
            }
        });
    }

    [Test]
    public async Task Legacy_coordinate_hash_is_stable_across_two_opens_of_the_same_cut()
    {
        var first = await CaptureLegacyLeafKeysAsync(MakeCoordinate(treeMapVersion: 4, (0, 3), (2, 7)));
        // Same cut, shards supplied in the opposite order: the hash sorts the
        // shard ids before mixing them, so enumeration order must not leak into
        // the derived key. Compared order-insensitively because the per-shard
        // fan-out itself follows the coordinate's enumeration order, which is
        // not part of this contract.
        var second = await CaptureLegacyLeafKeysAsync(MakeCoordinate(treeMapVersion: 4, (2, 7), (0, 3)));

        Assert.That(second, Is.EquivalentTo(first),
            "Two callers that build the same coordinate must activate the same snapshot leaves, "
            + "so the coordinate hash must not depend on shard enumeration order.");
    }

    [Test]
    public async Task Legacy_coordinate_hash_changes_when_the_captured_cut_changes()
    {
        var baseline = await CaptureLegacyLeafKeysAsync(MakeCoordinate(treeMapVersion: 4, (0, 3)));
        var otherOffset = await CaptureLegacyLeafKeysAsync(MakeCoordinate(treeMapVersion: 4, (0, 4)));
        var otherMapVersion = await CaptureLegacyLeafKeysAsync(MakeCoordinate(treeMapVersion: 5, (0, 3)));

        Assert.Multiple(() =>
        {
            Assert.That(otherOffset, Is.Not.EqualTo(baseline).AsCollection,
                "A different captured WAL offset is a different cut and must not share a snapshot leaf.");
            Assert.That(otherMapVersion, Is.Not.EqualTo(baseline).AsCollection,
                "A different tree-map version is a different cut and must not share a snapshot leaf.");
        });
    }
}
