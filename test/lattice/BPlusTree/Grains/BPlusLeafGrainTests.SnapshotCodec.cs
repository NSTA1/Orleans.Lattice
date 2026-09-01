using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Codec-adoption coverage for the leaf snapshot capture and rehydrate seams:
/// a capture writes the compact binary frame, a rehydrate reads either
/// encoding transparently, a leaf whose durable blob is still legacy is
/// rewritten in the new format on its next natural capture without losing
/// coverage, and an unreadable blob of either encoding is treated as "no
/// snapshot" rather than as coverage.
/// <para>
/// That last property is the load-bearing one. The coverage-gated WAL GC trims
/// a checkpointed prefix precisely because a snapshot covers it, so a blob that
/// reported coverage it cannot reproduce would authorise trimming the last
/// durable copy of that prefix.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, ILeafSnapshotStorageGrain SnapshotStub, FakePersistentState<LeafNodeState> State)
        CreateSnapshotCodecLeaf(bool binaryEncodingEnabled = true, long persistedCheckpoint = 0L)
    {
        var leafKey = Guid.NewGuid();
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-codec";
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                LeafSnapshotBinaryEncodingEnabled = binaryEncodingEnabled,
            },
            maxLeafKeys: 128,
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

    private static List<LeafSnapshotRow> Materialize(LeafSnapshotBlob blob)
    {
        var rows = new List<LeafSnapshotRow>(blob.GetRowCount());
        foreach (var row in blob.EnumerateRows())
        {
            rows.Add(row);
        }

        return rows;
    }

    [Test]
    public async Task CaptureSnapshotAsync_writes_the_binary_frame_and_leaves_the_legacy_row_slot_empty()
    {
        var (grain, stub, state) = CreateSnapshotCodecLeaf();
        await grain.SetAsync("alpha", Encoding.UTF8.GetBytes("v-alpha"));
        await grain.SetAsync("beta", Encoding.UTF8.GetBytes("v-beta"));
        state.State.ProjectionCheckpointOffset = 7L;

        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());

        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.HasBinaryRowPayload(), Is.True);
        Assert.That(captured.ValidateRowPayload(), Is.True);
        Assert.That(captured.Rows, Is.Empty,
            "exactly one of the two row carriers may hold rows; writing both would double the persisted size");
        Assert.That(captured.GetRowCount(), Is.EqualTo(2));
        Assert.That(
            Materialize(captured).Select(r => r.Key).ToArray(),
            Is.EqualTo(new[] { "alpha", "beta" }).AsCollection,
            "rows must be encoded in ascending ordinal key order so the frame index table is seekable");
        Assert.That(captured.SnapshotOffset, Is.EqualTo(7L));
    }

    [Test]
    public async Task CaptureSnapshotAsync_writes_the_legacy_row_graph_when_the_binary_encoding_is_disabled()
    {
        var (grain, stub, state) = CreateSnapshotCodecLeaf(binaryEncodingEnabled: false);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        state.State.ProjectionCheckpointOffset = 3L;

        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());

        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.EncodedRows, Is.Null);
        Assert.That(captured.HasBinaryRowPayload(), Is.False);
        Assert.That(captured.Rows, Has.Count.EqualTo(1));
        Assert.That(captured.Rows[0].Key, Is.EqualTo("k1"));
        Assert.That(captured.ValidateRowPayload(), Is.True);
    }

    [Test]
    public async Task CaptureSnapshotAsync_round_trips_the_per_key_merge_mode_through_the_frame()
    {
        var (grain, stub, state) = CreateSnapshotCodecLeaf();
        await grain.SetAsync("plain", Encoding.UTF8.GetBytes("v"));
        state.State.ProjectionCheckpointOffset = 2L;

        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());

        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null);
        var rows = Materialize(captured!);
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].MergeMode, Is.Null, "a plain last-writer-wins key carries no mode discriminator");
        Assert.That(rows[0].Value.Value, Is.EqualTo(Encoding.UTF8.GetBytes("v")));
    }

    [Test]
    public async Task TryRehydrateFromSnapshotAsync_restores_identically_from_either_encoding()
    {
        var rows = new[]
        {
            new LeafSnapshotRow(
                "k1",
                LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("one"), new HybridLogicalClock { WallClockTicks = 10L })),
            new LeafSnapshotRow(
                "k2",
                LwwValue<byte[]>.Create([0x00, 0x01, 0x00], new HybridLogicalClock { WallClockTicks = 20L }),
                LatticeMergeMode.GCounter),
        };

        var legacy = new LeafSnapshotBlob
        {
            SnapshotOffset = 9L,
            Rows = rows,
            SnapshotOffsetsByPartition = [9L],
        };
        var binary = new LeafSnapshotBlob
        {
            SnapshotOffset = 9L,
            EncodedRows = LeafSnapshotCodec.Encode(rows),
            SnapshotOffsetsByPartition = [9L],
        };

        var fromLegacy = await RehydrateAndReadAsync(legacy);
        var fromBinary = await RehydrateAndReadAsync(binary);

        Assert.That(fromBinary, Is.EqualTo(fromLegacy).AsCollection,
            "the two encodings must rehydrate byte-identical caches");
        Assert.That(fromBinary.Select(kv => kv.Key).ToArray(), Is.EqualTo(new[] { "k1", "k2" }).AsCollection);

        static async Task<List<KeyValuePair<string, byte[]>>> RehydrateAndReadAsync(LeafSnapshotBlob blob)
        {
            var (grain, stub, _) = CreateSnapshotCodecLeaf();
            stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(blob));

            Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(9L));
            return await grain.GetEntriesAsync();
        }
    }

    [Test]
    public async Task Legacy_blob_is_rewritten_as_a_binary_frame_on_the_next_capture_without_regressing_coverage()
    {
        // The lazy-rewrite guarantee: no migration pass, no startup rewrite. A
        // leaf that rehydrates from a legacy blob simply persists the frame the
        // next time it captures, and the coverage it reports must not go
        // backwards across that rewrite.
        var legacyRows = new[]
        {
            new LeafSnapshotRow(
                "legacy-1",
                LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("one"), new HybridLogicalClock { WallClockTicks = 10L })),
            new LeafSnapshotRow(
                "legacy-2",
                LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("two"), new HybridLogicalClock { WallClockTicks = 20L })),
        };
        var legacyBlob = new LeafSnapshotBlob
        {
            SnapshotOffset = 12L,
            Rows = legacyRows,
            SnapshotOffsetsByPartition = [12L],
        };

        var (grain, stub, state) = CreateSnapshotCodecLeaf();
        stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(legacyBlob));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);
        var coverageBeforeRewrite = grain.DurableSnapshotCoverageForPartition(0);
        Assert.That(coverageBeforeRewrite, Is.EqualTo(12L));

        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());

        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.HasBinaryRowPayload(), Is.True, "the next natural capture rewrites the blob as a frame");
        Assert.That(captured.Rows, Is.Empty);
        Assert.That(
            Materialize(captured).Select(r => r.Key).ToArray(),
            Is.EqualTo(new[] { "legacy-1", "legacy-2" }).AsCollection,
            "the rewrite must carry every row the legacy blob held");
        Assert.That(captured.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(captured.SnapshotOffsetsByPartition![0], Is.GreaterThanOrEqualTo(coverageBeforeRewrite),
            "coverage must not regress across the rewrite");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.GreaterThanOrEqualTo(coverageBeforeRewrite));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
    }

    [Test]
    public async Task TryRehydrateFromSnapshotAsync_treats_a_corrupt_binary_blob_as_no_snapshot_and_records_no_coverage()
    {
        var frame = LeafSnapshotCodec.Encode(new[]
        {
            new LeafSnapshotRow("k", LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), default)),
        });
        var corrupt = new LeafSnapshotBlob
        {
            SnapshotOffset = 50L,
            EncodedRows = frame.AsSpan(0, frame.Length - 4).ToArray(),
            SnapshotOffsetsByPartition = [50L],
        };

        var (grain, stub, _) = CreateSnapshotCodecLeaf();
        stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(corrupt));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.False);
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "an unreadable blob must never be recorded as durable coverage; doing so would authorise the " +
            "WAL GC to trim a prefix nothing can reproduce");
        Assert.That(await grain.GetEntriesAsync(), Is.Empty,
            "a rejected blob must not partially populate the cache");
    }

    [Test]
    public async Task TryRehydrateFromSnapshotAsync_treats_a_corrupt_legacy_blob_as_no_snapshot_and_records_no_coverage()
    {
        var corrupt = new LeafSnapshotBlob
        {
            SnapshotOffset = 50L,
            Rows = new List<LeafSnapshotRow> { new(null!, LwwValue<byte[]>.Create([1], default)) },
            SnapshotOffsetsByPartition = [50L],
        };

        var (grain, stub, _) = CreateSnapshotCodecLeaf();
        stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(corrupt));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.False);
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L));
        Assert.That(await grain.GetEntriesAsync(), Is.Empty);
    }

    [Test]
    public void LeafSnapshotBinaryEncodingEnabled_defaults_to_on_so_an_upgraded_deployment_adopts_the_frame()
    {
        Assert.That(LatticeOptions.DefaultLeafSnapshotBinaryEncodingEnabled, Is.True);
        Assert.That(new LatticeOptions().LeafSnapshotBinaryEncodingEnabled, Is.True);
    }

    [Test]
    public async Task CaptureSnapshotAsync_encodes_an_empty_cache_as_a_valid_empty_frame()
    {
        // A leaf whose projection is empty still has a meaningful checkpoint,
        // so it captures a zero-row blob rather than nothing at all.
        var (grain, stub, state) = CreateSnapshotCodecLeaf();
        state.State.ProjectionCheckpointOffset = 4L;

        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());

        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.HasBinaryRowPayload(), Is.True);
        Assert.That(captured.ValidateRowPayload(), Is.True);
        Assert.That(captured.GetRowCount(), Is.Zero);
        Assert.That(captured.SnapshotOffset, Is.EqualTo(4L));
    }
}
