using System.Buffers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Cold-rebuild / WAL-replay coverage for the producer-side CRDT
/// delta-apply path once the durable commit-log record is delta-only.
/// Because the canonical <see cref="OrleansBinaryWalRecordEncoder"/>
/// strips <see cref="WalRecord.Value"/> for non-prepared CRDT-mode Set
/// records, a real durable serialising provider yields
/// <c>Value == null</c> on read-back, so the activation-time replay
/// (<see cref="ILeafProjection.Apply"/>) must fold the typed delta back
/// into the prior visible state. These tests drive every CRDT mode
/// through the real <c>Encode -&gt; storage -&gt; Decode -&gt; replay</c>
/// pipeline and assert byte-identical reconstruction of the post-fold
/// state versus the foreground-applied state. The in-memory test
/// providers used elsewhere retain <c>Value</c> and so bypass the strip;
/// this fixture closes that gap by encoding through the real codec.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ReplayTreeId = "tree-crdt-replay";
    private const string ReplayOrMapTreeId = "tree-crdt-replay-ormap";

    private static ServiceProvider _replaySerializerServices = null!;
    private static Serializer<WalRecord> _replayWalSerializer = null!;

    [OneTimeSetUp]
    public void CrdtReplayOneTimeSetUp()
    {
        _replaySerializerServices = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _replayWalSerializer = _replaySerializerServices.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void CrdtReplayOneTimeTearDown() => _replaySerializerServices?.Dispose();

    /// <summary>
    /// Builds a registry covering every closed shape (via the default
    /// constructor) plus the per-tree OR-Map shape the replay tree needs;
    /// the closed shapes resolve through the global fallback but the
    /// OR-Map shape must be registered per tree id.
    /// </summary>
    private static CrdtShapeRegistry BuildReplayRegistry()
    {
        var registry = new CrdtShapeRegistry();
        registry.Register(ReplayOrMapTreeId, CrdtShape.ForOrMap<string, PnCounter>());
        return registry;
    }

    private static BPlusLeafGrain CreateReplayLeaf(
        CrdtShapeRegistry registry,
        string treeId,
        out FakePersistentState<LeafNodeState> state,
        ICommitLogWriter? commitLog = null,
        string replicaId = "leaf-crdt-replay")
    {
        state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;

        var sc = new ServiceCollection();
        sc.AddSingleton(registry);
        if (commitLog is not null)
            sc.AddSingleton(commitLog);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", replicaId));
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        return new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
    }

    private static IWalRecordEncoder ReplayEncoder() => new OrleansBinaryWalRecordEncoder(_replayWalSerializer);

    /// <summary>
    /// Drives the captured WAL records through the real encoder strip and
    /// replays the decoded mutations against <paramref name="replay"/> in
    /// append (offset) order, exactly as the activation-time cold-rebuild
    /// path would after a durable serialising provider round-trip.
    /// </summary>
    private static void ReplayThroughStrippingEncoder(BPlusLeafGrain replay, IReadOnlyList<WalRecord> appended)
    {
        var encoder = ReplayEncoder();
        var projection = (ILeafProjection)replay;
        foreach (var record in appended)
        {
            var buffer = new ArrayBufferWriter<byte>();
            encoder.Encode(record, buffer);
            // Re-stamp TreeId and Mode from the surrounding context, the
            // same way the storage read seam restores them after the
            // canonical encoder strips both slots.
            var decoded = encoder.Decode(buffer.WrittenSpan, record.TreeId ?? string.Empty, record.Mode);
            var mutation = WalRecordConverter.FromWalRecord(in decoded);
            projection.Apply(mutation);
        }
    }

    private static byte[] ReplayDeltaBytes(LatticeMergeMode mode, int variant)
    {
        switch (mode)
        {
            case LatticeMergeMode.OrSet:
            {
                var delta = new OrSetDelta
                {
                    Adds = new[]
                    {
                        new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("e" + variant), ReplicaId = "r" + variant, Counter = variant },
                    },
                    Removes = Array.Empty<OrSetDeltaDot>(),
                };
                return JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);
            }
            case LatticeMergeMode.PnCounter:
            {
                var delta = new PnCounterDelta
                {
                    Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["r" + variant] = variant * 5 },
                    Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
                };
                return JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta);
            }
            case LatticeMergeMode.VersionVector:
            {
                var delta = new VersionVectorDelta
                {
                    Entries = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
                    {
                        ["r" + variant] = new HybridLogicalClock { WallClockTicks = variant, Counter = variant },
                    },
                };
                return JsonLatticeSerializer<VersionVectorDelta>.Default.Serialize(delta);
            }
            case LatticeMergeMode.MvRegister:
            {
                var delta = new MvRegisterDelta
                {
                    Entries = new[]
                    {
                        new MvRegisterEntry { ReplicaId = "r" + variant, Counter = variant, Value = Encoding.UTF8.GetBytes("v" + variant) },
                    },
                    Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r" + variant] = variant },
                };
                return JsonLatticeSerializer<MvRegisterDelta>.Default.Serialize(delta);
            }
            case LatticeMergeMode.OrFlag:
            {
                var delta = new OrFlagDelta
                {
                    Enables = new[] { new OrSetDot { ReplicaId = "r" + variant, Counter = variant } },
                    Disables = Array.Empty<OrSetDot>(),
                };
                return JsonLatticeSerializer<OrFlagDelta>.Default.Serialize(delta);
            }
            case LatticeMergeMode.RwFlag:
            {
                var delta = new RwFlagDelta
                {
                    Enables = new[] { new OrSetDot { ReplicaId = "r" + variant, Counter = variant } },
                    Disables = Array.Empty<OrSetDot>(),
                    Tombstones = Array.Empty<OrSetDot>(),
                };
                return JsonLatticeSerializer<RwFlagDelta>.Default.Serialize(delta);
            }
            case LatticeMergeMode.Sequence:
            {
                // variant 1 inserts after the root sentinel; variant 2
                // chains after variant 1's node so the two folds compose.
                var parent = variant == 1
                    ? new OrSetDot { ReplicaId = string.Empty, Counter = 0 }
                    : new OrSetDot { ReplicaId = "r1", Counter = 1 };
                var delta = new RgaDelta
                {
                    Inserts = new[]
                    {
                        new RgaDeltaNode
                        {
                            ReplicaId = "r" + variant,
                            Counter = variant,
                            ParentDot = parent,
                            Value = Encoding.UTF8.GetBytes("n" + variant),
                        },
                    },
                    Tombstones = Array.Empty<OrSetDot>(),
                };
                return JsonLatticeSerializer<RgaDelta>.Default.Serialize(delta);
            }
            case LatticeMergeMode.OrMap:
            {
                var inner = new PnCounter();
                inner.Increment("r" + variant, variant * 5);
                var delta = new OrMapDelta<string, PnCounter>
                {
                    Adds = new[]
                    {
                        new OrMapDeltaEntry<string, PnCounter>
                        {
                            Key = "mk" + variant,
                            ReplicaId = "r" + variant,
                            Counter = variant,
                            Value = inner,
                        },
                    },
                    Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
                };
                return JsonLatticeSerializer<OrMapDelta<string, PnCounter>>.Default.Serialize(delta);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, "unhandled mode");
        }
    }

    private static string TreeForMode(LatticeMergeMode mode) =>
        mode == LatticeMergeMode.OrMap ? ReplayOrMapTreeId : ReplayTreeId;

    [Test]
    [TestCase(LatticeMergeMode.OrSet)]
    [TestCase(LatticeMergeMode.PnCounter)]
    [TestCase(LatticeMergeMode.VersionVector)]
    [TestCase(LatticeMergeMode.MvRegister)]
    [TestCase(LatticeMergeMode.OrFlag)]
    [TestCase(LatticeMergeMode.RwFlag)]
    [TestCase(LatticeMergeMode.Sequence)]
    [TestCase(LatticeMergeMode.OrMap)]
    public async Task Replay_through_stripping_encoder_reconstructs_foreground_state(LatticeMergeMode mode)
    {
        var registry = BuildReplayRegistry();
        var treeId = TreeForMode(mode);
        var commitLog = new FakeCommitLogWriter();
        var foreground = CreateReplayLeaf(registry, treeId, out _, commitLog, replicaId: "leaf-fg-" + mode);

        // Two deltas folded into the same key, plus a single delta on a
        // second key so the replay spans more than one entry.
        await foreground.ApplyCrdtDeltaAsync("k1", mode, ReplayDeltaBytes(mode, 1));
        await foreground.ApplyCrdtDeltaAsync("k1", mode, ReplayDeltaBytes(mode, 2));
        await foreground.ApplyCrdtDeltaAsync("k2", mode, ReplayDeltaBytes(mode, 1));

        var fgK1 = await foreground.GetAsync("k1");
        var fgK2 = await foreground.GetAsync("k2");
        Assert.That(fgK1, Is.Not.Null);
        Assert.That(fgK2, Is.Not.Null);

        // Every appended record must be delta-only on the wire for this
        // mode (the producer never carries the post-merge state row).
        foreach (var record in commitLog.Appended)
        {
            Assert.That(record.Mode, Is.EqualTo(mode));
            Assert.That(record.Delta, Is.Not.Null);
            Assert.That(record.Value, Is.Null, "producer must not materialise the post-merge state row onto the WAL record");
        }

        var replay = CreateReplayLeaf(registry, treeId, out _, commitLog: null, replicaId: "leaf-rp-" + mode);
        ReplayThroughStrippingEncoder(replay, commitLog.Appended);

        var rpK1 = await replay.GetAsync("k1");
        var rpK2 = await replay.GetAsync("k2");
        Assert.That(rpK1, Is.EqualTo(fgK1), "replay must reconstruct byte-identical post-fold state for the folded key");
        Assert.That(rpK2, Is.EqualTo(fgK2), "replay must reconstruct byte-identical post-fold state for the single-delta key");
    }

    [Test]
    public async Task Replay_through_stripping_encoder_handles_interleaved_keys_and_tombstone()
    {
        const LatticeMergeMode mode = LatticeMergeMode.OrSet;
        var registry = BuildReplayRegistry();
        var commitLog = new FakeCommitLogWriter();
        var foreground = CreateReplayLeaf(registry, ReplayTreeId, out _, commitLog, replicaId: "leaf-fg-interleave");

        await foreground.ApplyCrdtDeltaAsync("a", mode, ReplayDeltaBytes(mode, 1));
        await foreground.ApplyCrdtDeltaAsync("b", mode, ReplayDeltaBytes(mode, 1));
        await foreground.ApplyCrdtDeltaAsync("a", mode, ReplayDeltaBytes(mode, 2));
        // Tombstone an existing CRDT key via an LWW delete; replay must
        // observe the Delete record and reap the key.
        await foreground.DeleteAsync("b");
        await foreground.ApplyCrdtDeltaAsync("c", mode, ReplayDeltaBytes(mode, 1));

        var fgA = await foreground.GetAsync("a");
        var fgB = await foreground.GetAsync("b");
        var fgC = await foreground.GetAsync("c");
        Assert.That(fgA, Is.Not.Null);
        Assert.That(fgB, Is.Null, "deleted key must read back as null in the foreground");
        Assert.That(fgC, Is.Not.Null);

        var replay = CreateReplayLeaf(registry, ReplayTreeId, out _, commitLog: null, replicaId: "leaf-rp-interleave");
        ReplayThroughStrippingEncoder(replay, commitLog.Appended);

        Assert.That(await replay.GetAsync("a"), Is.EqualTo(fgA));
        Assert.That(await replay.GetAsync("b"), Is.Null, "replay must preserve the tombstone for the deleted key");
        Assert.That(await replay.GetAsync("c"), Is.EqualTo(fgC));
    }

    [Test]
    public async Task CrdtApply_with_writer_present_defers_row_and_materialises_on_get()
    {
        // The writer-path gate: with a commit-log writer wired, the apply
        // still defers the O(state) row materialisation. The WAL record is
        // appended delta-only and GetAsync materialises canonical bytes.
        var registry = BuildReplayRegistry();
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateReplayLeaf(registry, ReplayTreeId, out _, commitLog, replicaId: "leaf-writer-defer");

        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, ReplayDeltaBytes(LatticeMergeMode.OrSet, 1));

        Assert.That(commitLog.AppendCount, Is.EqualTo(1));
        Assert.That(commitLog.Appended[0].Value, Is.Null);

        var bytes = await grain.GetAsync("k");
        Assert.That(bytes, Is.Not.Null);
        Assert.That(grain.TryGetTypedShadowForTest<OrSet>("k", out var shadow), Is.True);
        Assert.That(bytes, Is.EqualTo(JsonLatticeSerializer<OrSet>.Default.Serialize(shadow)));
    }

    [Test]
    public async Task CrdtApply_with_writer_present_keeps_incremental_digest_consistent()
    {
        // Re-affirm digest byte-identity (streaming SerializeStateInto ==
        // array SerializeState) under the writer-path gate: with a writer
        // wired the deferred fold still feeds the incremental projection
        // hash from the streaming buffer, which must equal the from-scratch
        // recompute over the materialised rows.
        var registry = BuildReplayRegistry();
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateReplayLeaf(registry, ReplayTreeId, out var state, commitLog, replicaId: "leaf-writer-digest");

        for (var i = 1; i <= 6; i++)
        {
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, ReplayDeltaBytes(LatticeMergeMode.OrSet, i));
        }
        await grain.ApplyCrdtDeltaAsync("k2", LatticeMergeMode.OrSet, ReplayDeltaBytes(LatticeMergeMode.OrSet, 1));

        Assert.That(state.State.ProjectionHash, Is.Not.Null);
        Assert.That(state.State.ProjectionHash, Is.EqualTo(grain.ComputeFullProjectionHashFromState()),
            "incremental digest under the writer-path gate must equal the from-scratch recompute");
    }
}
