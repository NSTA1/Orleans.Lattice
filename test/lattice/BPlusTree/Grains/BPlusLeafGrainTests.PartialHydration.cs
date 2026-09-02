using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Bounded leaf hydration at the grain seam (issue #1839): a leaf that
/// rehydrates from a binary snapshot comes online without decoding it and
/// materialises entry ranges only as reads require them.
/// <para>
/// The properties asserted here are the ones that make that safe rather than
/// merely fast. Reads, scans, digests and the canonical full-walk hash are
/// byte-identical under partial and full hydration; a partially hydrated leaf
/// never stamps snapshot coverage it does not hold, and never captures a blob
/// short of a row - which is the #1535 no-loss invariant, since the
/// coverage-gated WAL GC trims a checkpointed prefix precisely because a
/// snapshot claims to cover it; a WAL tail replay lands correctly over a
/// partially hydrated cache; and the kill switch restores the previous
/// behaviour exactly.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const int HydrationCorpusRows = 320;

    private static (BPlusLeafGrain Grain, ILeafSnapshotStorageGrain SnapshotStub, FakePersistentState<LeafNodeState> State)
        CreateHydrationLeaf(
            bool partialHydrationEnabled = true,
            long residentBudgetBytes = 0L,
            long persistedCheckpoint = 0L,
            bool maintainProjectionDigest = true)
    {
        var leafKey = Guid.NewGuid();
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-hydration";
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                LeafPartialHydrationEnabled = partialHydrationEnabled,
                LeafHydrationResidentBytes = residentBudgetBytes,
                MaintainProjectionDigest = maintainProjectionDigest,
            },
            maxLeafKeys: 100_000,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, snapshotStub, state);
    }

    private static string HydrationKey(int i) => $"k{i:D5}";

    private static byte[] HydrationValue(int i)
    {
        var bytes = new byte[256];
        for (var b = 0; b < bytes.Length; b++)
        {
            bytes[b] = (byte)((i + b) & 0xFF);
        }

        return bytes;
    }

    private static LeafSnapshotRow[] HydrationRows(int rowCount = HydrationCorpusRows)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = i % 7 == 6
                ? new LeafSnapshotRow(
                    HydrationKey(i),
                    new LwwValue<byte[]>
                    {
                        Value = null,
                        IsTombstone = true,
                        Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i },
                    })
                : new LeafSnapshotRow(
                    HydrationKey(i),
                    LwwValue<byte[]>.Create(
                        HydrationValue(i),
                        new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i }),
                    i % 11 == 3 ? LatticeMergeMode.GCounter : null);
        }

        return rows;
    }

    private static LeafSnapshotBlob HydrationBlob(LeafSnapshotRow[] rows, long offset = 25L)
        => new()
        {
            SnapshotOffset = offset,
            EncodedRows = LeafSnapshotCodec.Encode(rows),
            SnapshotOffsetsByPartition = [offset],
        };

    private static async Task<BPlusLeafGrain> RehydratedLeafAsync(
        LeafSnapshotRow[] rows,
        bool partialHydrationEnabled = true,
        long residentBudgetBytes = 0L,
        long offset = 25L,
        bool maintainProjectionDigest = true)
    {
        var (grain, stub, _) = CreateHydrationLeaf(
            partialHydrationEnabled, residentBudgetBytes, maintainProjectionDigest: maintainProjectionDigest);
        stub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(HydrationBlob(rows, offset)));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);
        return grain;
    }

    [Test]
    public void LeafPartialHydrationEnabled_defaults_to_on_so_an_upgraded_deployment_adopts_bounded_hydration()
    {
        Assert.That(LatticeOptions.DefaultLeafPartialHydrationEnabled, Is.True);
        Assert.That(new LatticeOptions().LeafPartialHydrationEnabled, Is.True);
        Assert.That(LatticeOptions.DefaultLeafHydrationResidentBytes, Is.EqualTo(1L * 1024 * 1024));
        Assert.That(new LatticeOptions().LeafHydrationResidentBytes, Is.EqualTo(1L * 1024 * 1024));
    }

    [Test]
    public async Task Rehydrate_brings_the_leaf_online_without_decoding_the_snapshot()
    {
        var rows = HydrationRows();
        var grain = await RehydratedLeafAsync(rows);

        var cache = grain.CacheForTest;
        Assert.That(cache.HasPendingHydration, Is.True);
        Assert.That(cache.HydratedRowCount, Is.Zero, "activation must not materialise a single row");
        Assert.That(cache.SnapshotBytesRead, Is.Zero);
        Assert.That(cache.Count, Is.EqualTo(rows.Length),
            "the leaf must still report the whole projection it is responsible for");
    }

    [Test]
    public async Task A_point_read_costs_a_block_and_a_full_read_costs_the_leaf()
    {
        var rows = HydrationRows();
        var frameLength = LeafSnapshotCodec.Encode(rows).Length;

        var pointLeaf = await RehydratedLeafAsync(rows);
        var value = await pointLeaf.GetAsync(HydrationKey(100));
        Assert.That(value, Is.EqualTo(HydrationValue(100)));
        var pointBytes = pointLeaf.CacheForTest.SnapshotBytesRead;

        var fullLeaf = await RehydratedLeafAsync(rows);
        _ = await fullLeaf.GetLiveEntriesAsync();
        var fullBytes = fullLeaf.CacheForTest.SnapshotBytesRead;

        Assert.That(pointBytes, Is.GreaterThan(0));
        Assert.That(fullBytes, Is.GreaterThan(pointBytes * 8),
            "an unbounded read still pays for the leaf; that is the cost a bounded read avoids");
        Assert.That(pointBytes, Is.LessThan(frameLength / 4));
    }

    [Test]
    public async Task Activation_cost_tracks_the_requested_key_range_and_not_the_leaf_size()
    {
        // The acceptance property, asserted on work rather than on wall clock:
        // hold the requested range fixed and grow the leaf around it.
        var smallLeaf = await RehydratedLeafAsync(HydrationRows(128));
        var largeLeaf = await RehydratedLeafAsync(HydrationRows(1024));

        var smallEntries = await smallLeaf.GetEntriesAsync(HydrationKey(32), HydrationKey(64));
        var largeEntries = await largeLeaf.GetEntriesAsync(HydrationKey(32), HydrationKey(64));

        Assert.That(largeEntries.Count, Is.EqualTo(smallEntries.Count));
        Assert.That(largeLeaf.CacheForTest.SnapshotRowsMaterialised,
            Is.EqualTo(smallLeaf.CacheForTest.SnapshotRowsMaterialised),
            "an eight-times-larger leaf must materialise the same rows for the same key range");
        Assert.That(largeLeaf.CacheForTest.SnapshotBytesRead,
            Is.EqualTo(smallLeaf.CacheForTest.SnapshotBytesRead));
    }

    [Test]
    public async Task A_ranged_scan_leaves_the_rest_of_the_leaf_unread()
    {
        var rows = HydrationRows();
        var grain = await RehydratedLeafAsync(rows);

        var keys = await grain.GetKeysAsync(HydrationKey(40), HydrationKey(60));

        Assert.That(keys, Is.Not.Empty);
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
        Assert.That(grain.CacheForTest.HydratedRowCount, Is.LessThan(rows.Length));
    }

    [Test]
    public async Task Every_read_and_scan_is_identical_under_partial_and_full_hydration()
    {
        var rows = HydrationRows();
        var partial = await RehydratedLeafAsync(rows, partialHydrationEnabled: true);
        var full = await RehydratedLeafAsync(rows, partialHydrationEnabled: false);

        Assert.That(await partial.GetAsync(HydrationKey(0)), Is.EqualTo(await full.GetAsync(HydrationKey(0))));
        Assert.That(await partial.GetAsync(HydrationKey(6)), Is.EqualTo(await full.GetAsync(HydrationKey(6))),
            "a tombstoned key must read the same either way");
        Assert.That(await partial.GetAsync("absent"), Is.EqualTo(await full.GetAsync("absent")));

        var partialEntries = await partial.GetEntriesAsync();
        var fullEntries = await full.GetEntriesAsync();
        Assert.That(partialEntries.Select(kv => kv.Key).ToArray(),
            Is.EqualTo(fullEntries.Select(kv => kv.Key).ToArray()).AsCollection);
        for (var i = 0; i < fullEntries.Count; i++)
        {
            Assert.That(partialEntries[i].Value, Is.EqualTo(fullEntries[i].Value));
        }

        var partialRanged = await partial.GetEntriesAsync(HydrationKey(50), HydrationKey(120));
        var fullRanged = await full.GetEntriesAsync(HydrationKey(50), HydrationKey(120));
        Assert.That(partialRanged.Select(kv => kv.Key).ToArray(),
            Is.EqualTo(fullRanged.Select(kv => kv.Key).ToArray()).AsCollection);

        var partialAfter = await partial.GetKeysAsync(afterExclusive: HydrationKey(300));
        var fullAfter = await full.GetKeysAsync(afterExclusive: HydrationKey(300));
        Assert.That(partialAfter, Is.EqualTo(fullAfter).AsCollection);

        var partialBefore = await partial.GetKeysAsync(beforeExclusive: HydrationKey(5));
        var fullBefore = await full.GetKeysAsync(beforeExclusive: HydrationKey(5));
        Assert.That(partialBefore, Is.EqualTo(fullBefore).AsCollection);

        Assert.That((await partial.GetLiveEntriesAsync()).Count, Is.EqualTo((await full.GetLiveEntriesAsync()).Count));
    }

    [Test]
    public async Task The_leaf_digest_and_canonical_hash_are_identical_under_partial_and_full_hydration()
    {
        var rows = HydrationRows();

        var partial = await RehydratedLeafAsync(rows, partialHydrationEnabled: true);
        // Read one key first, so the hash is folded over a genuinely
        // half-hydrated cache rather than a pristine one.
        _ = await partial.GetAsync(HydrationKey(64));
        var partialDigest = await partial.GetProjectionDigestAsync();

        var full = await RehydratedLeafAsync(rows, partialHydrationEnabled: false);
        var fullDigest = await full.GetProjectionDigestAsync();

        Assert.That(partialDigest.Hash, Is.EqualTo(fullDigest.Hash),
            "the chained internal-node fold depends on the canonical full-walk hash matching bit for bit");
        Assert.That(partialDigest.EntryCount, Is.EqualTo(fullDigest.EntryCount));
    }

    [Test]
    public async Task Leaf_stats_are_identical_under_partial_and_full_hydration()
    {
        var rows = HydrationRows();
        var partial = await RehydratedLeafAsync(rows, partialHydrationEnabled: true);
        var full = await RehydratedLeafAsync(rows, partialHydrationEnabled: false);

        Assert.That(partial.CacheForTest.Count, Is.EqualTo(full.CacheForTest.Count));
        Assert.That(partial.CacheForTest.StateBytes, Is.EqualTo(full.CacheForTest.StateBytes));
        Assert.That(partial.CacheForTest.LiveCount, Is.EqualTo(full.CacheForTest.LiveCount));
        Assert.That(partial.CacheForTest.HydratedRowCount, Is.Zero,
            "and it reports all of that without having materialised anything");
    }

    [Test]
    public async Task A_capture_from_a_partially_hydrated_leaf_carries_every_row()
    {
        // The #1535 no-loss invariant. The coverage-gated WAL GC trims a
        // checkpointed prefix precisely because a snapshot claims to cover it,
        // so a capture that dropped the rows the leaf had not yet materialised
        // would authorise trimming the last durable copy of them.
        var rows = HydrationRows();
        var (grain, stub, state) = CreateHydrationLeaf();
        stub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(HydrationBlob(rows)));
        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);
        _ = await grain.GetAsync(HydrationKey(10));
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True, "the leaf must still be partially hydrated");

        state.State.ProjectionCheckpointOffset = 30L;
        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());

        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.ValidateRowPayload(), Is.True);
        Assert.That(captured.GetRowCount(), Is.EqualTo(rows.Length),
            "a partially hydrated leaf must materialise its snapshot in full before it captures");
        var capturedRows = Materialize(captured);
        for (var i = 0; i < rows.Length; i++)
        {
            Assert.That(capturedRows[i].Key, Is.EqualTo(rows[i].Key));
            Assert.That(capturedRows[i].Value.Value, Is.EqualTo(rows[i].Value.Value));
            Assert.That(capturedRows[i].MergeMode, Is.EqualTo(rows[i].MergeMode));
        }

        Assert.That(captured.SnapshotBytes, Is.EqualTo(grain.CacheForTest.StateBytes));
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.False);
    }

    [Test]
    public async Task A_partially_hydrated_leaf_reports_exactly_the_coverage_the_loaded_blob_carries()
    {
        var rows = HydrationRows();
        var grain = await RehydratedLeafAsync(rows, offset: 42L);

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(42L),
            "coverage is stamped from the blob's own offsets, never from what happens to be resident");
        Assert.That(grain.CacheForTest.HydratedRowCount, Is.Zero);

        // Materialising more of the leaf must not move coverage either way.
        _ = await grain.GetAsync(HydrationKey(5));
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(42L));
        _ = await grain.GetLiveEntriesAsync();
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(42L));
    }

    [Test]
    public async Task Coverage_and_capture_are_identical_under_partial_and_full_hydration()
    {
        var rows = HydrationRows();

        var partial = await CaptureAfterRehydrateAsync(partialHydrationEnabled: true);
        var full = await CaptureAfterRehydrateAsync(partialHydrationEnabled: false);

        Assert.That(partial.Coverage, Is.EqualTo(full.Coverage));
        Assert.That(partial.Blob!.GetRowCount(), Is.EqualTo(full.Blob!.GetRowCount()));
        Assert.That(partial.Blob.SnapshotOffset, Is.EqualTo(full.Blob.SnapshotOffset));
        Assert.That(partial.Blob.SnapshotBytes, Is.EqualTo(full.Blob.SnapshotBytes));
        Assert.That(partial.Blob.SnapshotOffsetsByPartition, Is.EqualTo(full.Blob.SnapshotOffsetsByPartition).AsCollection);
        Assert.That(partial.Blob.EncodedRows, Is.EqualTo(full.Blob.EncodedRows),
            "the captured frame must be byte-identical either way");

        async Task<(long Coverage, LeafSnapshotBlob? Blob)> CaptureAfterRehydrateAsync(bool partialHydrationEnabled)
        {
            var (grain, stub, state) = CreateHydrationLeaf(partialHydrationEnabled);
            stub.LoadAsync(Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<LeafSnapshotBlob?>(HydrationBlob(rows)));
            Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);
            state.State.ProjectionCheckpointOffset = 30L;

            LeafSnapshotBlob? captured = null;
            await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());
            await grain.CaptureSnapshotAsync();
            return (grain.DurableSnapshotCoverageForPartition(0), captured);
        }
    }

    [Test]
    public async Task An_unreadable_blob_records_no_coverage_and_attaches_no_hydration_source()
    {
        var frame = LeafSnapshotCodec.Encode(HydrationRows(8));
        var corrupt = new LeafSnapshotBlob
        {
            SnapshotOffset = 50L,
            EncodedRows = frame.AsSpan(0, frame.Length - 4).ToArray(),
            SnapshotOffsetsByPartition = [50L],
        };

        var (grain, stub, _) = CreateHydrationLeaf();
        stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(corrupt));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.False);
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L));
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.False);
        Assert.That(grain.CacheForTest.Count, Is.Zero);
    }

    [Test]
    public async Task A_legacy_blob_still_rehydrates_in_full_because_it_has_no_seekable_frame()
    {
        var rows = HydrationRows(16);
        var legacy = new LeafSnapshotBlob
        {
            SnapshotOffset = 12L,
            Rows = rows,
            SnapshotOffsetsByPartition = [12L],
        };

        var (grain, stub, _) = CreateHydrationLeaf();
        stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(legacy));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.False);
        Assert.That(grain.CacheForTest.Count, Is.EqualTo(rows.Length));
        Assert.That(await grain.GetAsync(HydrationKey(0)), Is.EqualTo(HydrationValue(0)));
    }

    [Test]
    public async Task The_kill_switch_restores_the_previous_full_hydration_behaviour()
    {
        var rows = HydrationRows();
        var grain = await RehydratedLeafAsync(rows, partialHydrationEnabled: false);

        var cache = grain.CacheForTest;
        Assert.That(cache.HasPendingHydration, Is.False, "nothing may be left lazily hydrated");
        Assert.That(cache.HydratedRowCount, Is.EqualTo(rows.Length), "every row is decoded up front");
        Assert.That(cache.SnapshotBytesRead, Is.Zero, "the bounded read path is not used at all");
        Assert.That(cache.PendingHydrationRowCount, Is.Zero);
        Assert.That(cache.EvictedBlockCount, Is.Zero);
        Assert.That(await grain.GetAsync(HydrationKey(1)), Is.EqualTo(HydrationValue(1)));
    }

    [Test]
    public async Task A_write_over_a_partially_hydrated_leaf_wins_and_is_visible_to_every_reader()
    {
        // The tail-replay shape: a leaf comes online over a snapshot and then
        // applies later WAL entries, which land through the ordinary mutation
        // funnel. Each must merge against the snapshot row underneath it.
        var rows = HydrationRows();
        var grain = await RehydratedLeafAsync(rows);
        var replacement = Encoding.UTF8.GetBytes("tail-replayed");

        await grain.SetAsync(HydrationKey(100), replacement);
        await grain.SetAsync("k99999-appended", replacement);
        await grain.DeleteAsync(HydrationKey(101));

        Assert.That(await grain.GetAsync(HydrationKey(100)), Is.EqualTo(replacement));
        Assert.That(await grain.GetAsync("k99999-appended"), Is.EqualTo(replacement));
        Assert.That(await grain.GetAsync(HydrationKey(101)), Is.Null);

        var entries = await grain.GetEntriesAsync();
        Assert.That(entries.Single(kv => kv.Key == HydrationKey(100)).Value, Is.EqualTo(replacement),
            "a full walk after the writes must never resurrect the snapshot values they replaced");
        Assert.That(entries.Any(kv => kv.Key == HydrationKey(101)), Is.False);
        Assert.That(entries.Any(kv => kv.Key == "k99999-appended"), Is.True);
    }

    [Test]
    public async Task A_tail_write_over_a_partially_hydrated_leaf_matches_a_fully_hydrated_one()
    {
        var rows = HydrationRows();
        var replacement = Encoding.UTF8.GetBytes("tail-replayed");

        var partialEntries = await ApplyTailAsync(partialHydrationEnabled: true);
        var fullEntries = await ApplyTailAsync(partialHydrationEnabled: false);

        Assert.That(partialEntries.Select(kv => kv.Key).ToArray(),
            Is.EqualTo(fullEntries.Select(kv => kv.Key).ToArray()).AsCollection);
        for (var i = 0; i < fullEntries.Count; i++)
        {
            Assert.That(partialEntries[i].Value, Is.EqualTo(fullEntries[i].Value));
        }

        async Task<List<KeyValuePair<string, byte[]>>> ApplyTailAsync(bool partialHydrationEnabled)
        {
            var grain = await RehydratedLeafAsync(rows, partialHydrationEnabled);
            await grain.SetAsync(HydrationKey(100), replacement);
            await grain.DeleteAsync(HydrationKey(101));
            await grain.SetAsync("k99999-appended", replacement);
            return await grain.GetEntriesAsync();
        }
    }

    [Test]
    public async Task Hydrated_ranges_are_evicted_under_budget_pressure_and_re_materialise_correctly()
    {
        var rows = HydrationRows(512);
        // Roughly two blocks' worth of payload, so a third block forces eviction.
        var grain = await RehydratedLeafAsync(rows, residentBudgetBytes: 2L * 32 * 256);

        for (var i = 0; i < 512; i += 32)
        {
            // Drives hydration across the whole leaf. No liveness assertion
            // here: every seventh row of the corpus is a tombstone, which reads
            // back as null by design.
            _ = await grain.GetAsync(HydrationKey(i));
        }

        var cache = grain.CacheForTest;
        Assert.That(cache.EvictedBlockCount, Is.GreaterThan(0), "the resident budget must actually bite");
        Assert.That(cache.HydratedRowCount, Is.LessThan(rows.Length),
            "a large tree must not have to be wholly resident to be queryable");
        Assert.That(cache.Count, Is.EqualTo(rows.Length), "eviction never changes the logical projection");

        // An evicted range comes back byte-identical, and a full walk is still
        // exactly the snapshot.
        Assert.That(await grain.GetAsync(HydrationKey(0)), Is.EqualTo(HydrationValue(0)));
        var entries = await grain.GetEntriesAsync();
        var expected = rows.Where(r => !r.Value.IsTombstone).ToArray();
        Assert.That(entries.Count, Is.EqualTo(expected.Length));
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.That(entries[i].Key, Is.EqualTo(expected[i].Key));
            Assert.That(entries[i].Value, Is.EqualTo(expected[i].Value.Value));
        }
    }

    [Test]
    public async Task Eviction_pressure_never_drops_a_write_even_when_reads_keep_arriving()
    {
        // Digest maintenance is off here on purpose. The incrementally
        // maintained projection digest is a canonical walk of every row, so the
        // first mutation after a rehydrate rebuilds it and materialises the
        // whole snapshot - correct, and required for hash equivalence, but it
        // leaves nothing partially hydrated to evict. Turning it off isolates
        // the interaction this test is about: a mutated range staying pinned
        // while reads keep pushing the resident budget.
        var rows = HydrationRows(512);
        var grain = await RehydratedLeafAsync(
            rows, residentBudgetBytes: 32 * 256, maintainProjectionDigest: false);
        var written = Encoding.UTF8.GetBytes("survives-eviction");

        await grain.SetAsync(HydrationKey(0), written);
        await grain.DeleteAsync(HydrationKey(1));

        // Interleave reads across the whole leaf so the budget is under
        // sustained pressure while the mutated range must stay pinned.
        for (var pass = 0; pass < 2; pass++)
        {
            for (var i = 32; i < 512; i += 32)
            {
                _ = await grain.GetAsync(HydrationKey(i));
                Assert.That(await grain.GetAsync(HydrationKey(0)), Is.EqualTo(written));
            }
        }

        Assert.That(grain.CacheForTest.EvictedBlockCount, Is.GreaterThan(0));
        Assert.That(await grain.GetAsync(HydrationKey(0)), Is.EqualTo(written));
        Assert.That(await grain.GetAsync(HydrationKey(1)), Is.Null, "a removal must never be resurrected");

        var entries = await grain.GetEntriesAsync();
        Assert.That(entries.Single(kv => kv.Key == HydrationKey(0)).Value, Is.EqualTo(written));
        Assert.That(entries.Any(kv => kv.Key == HydrationKey(1)), Is.False);
    }

    [Test]
    public async Task A_write_rebuilds_the_projection_digest_and_therefore_materialises_the_leaf()
    {
        // Documents the boundary of bounded hydration rather than working
        // around it. The canonical full-walk hash the chained internal-node
        // fold depends on cannot be folded over rows the leaf has not read, so
        // a leaf that takes a write while maintaining its digest materialises
        // in full. Bounded hydration therefore pays off on the read-dominated
        // cold-start path, which is the path the epic targets.
        var rows = HydrationRows(128);
        var grain = await RehydratedLeafAsync(rows, maintainProjectionDigest: true);
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);

        await grain.SetAsync(HydrationKey(0), Encoding.UTF8.GetBytes("v"));

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.False);
        Assert.That(grain.CacheForTest.SnapshotRowsMaterialised, Is.EqualTo(rows.Length),
            "and the work counter still reports what the activation actually read");
    }

    [Test]
    public async Task A_capture_after_eviction_still_carries_every_row()
    {
        var rows = HydrationRows(256);
        var (grain, stub, state) = CreateHydrationLeaf(residentBudgetBytes: 32 * 256);
        stub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(HydrationBlob(rows)));
        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);

        for (var i = 0; i < 256; i += 32)
        {
            _ = await grain.GetAsync(HydrationKey(i));
        }

        Assert.That(grain.CacheForTest.EvictedBlockCount, Is.GreaterThan(0));
        state.State.ProjectionCheckpointOffset = 30L;
        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());

        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.GetRowCount(), Is.EqualTo(rows.Length),
            "coverage must never be stamped for a row set an eviction shrank");
    }

    [Test]
    public async Task The_options_resolver_propagates_both_hydration_knobs_to_the_grain()
    {
        var rows = HydrationRows(64);
        var grain = await RehydratedLeafAsync(rows, partialHydrationEnabled: true, residentBudgetBytes: 0L);
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);

        var disabled = await RehydratedLeafAsync(rows, partialHydrationEnabled: false);
        Assert.That(disabled.CacheForTest.HasPendingHydration, Is.False,
            "a knob that the resolver drops silently keeps its default and looks like the grain ignoring config");
    }
}
