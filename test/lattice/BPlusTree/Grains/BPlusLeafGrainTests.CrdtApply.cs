using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit-level coverage for the producer-side typed CRDT delta-apply
/// seam on the leaf grain (<see cref="BPlusLeafGrain.ApplyCrdtDeltaAsync"/>).
/// Complements the cluster-hosted
/// <c>CrdtApplyIntegrationTests</c>: these tests instantiate the leaf
/// directly through the existing <c>CreateGrain</c> factory and
/// exercise the in-grain merge / state / WAL effects with no Orleans
/// runtime in scope, so each failure points at a single source file.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static BPlusLeafGrain CreateCrdtGrain(
        FakePersistentState<LeafNodeState>? state = null,
        ICommitLogWriter? commitLog = null)
    {
        state ??= new FakePersistentState<LeafNodeState>();
        // CrdtShapeRegistry.TryGet rejects an empty tree id, and in
        // production every leaf activation lands with a non-empty
        // TreeId; tests that route through ApplyCrdtDeltaAsync must
        // therefore seed one before driving the grain.
        if (string.IsNullOrEmpty(state.State.TreeId))
            state.State.TreeId = "test-tree";
        return CreateGrain(state, commitLog: commitLog);
    }

    // ── reject paths ───────────────────────────────────────────

    [Test]
    public void CrdtApply_null_key_throws()
    {
        var grain = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await grain.ApplyCrdtDeltaAsync(null!, LatticeMergeMode.OrSet, new byte[] { 0x00 }));
    }

    [Test]
    public void CrdtApply_null_delta_throws()
    {
        var grain = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, null!));
    }

    [Test]
    public void CrdtApply_LwwRegister_mode_is_rejected()
    {
        var grain = CreateGrain();
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, new byte[] { 0x00 }));
        Assert.That(ex!.ParamName, Is.EqualTo("mode"));
        Assert.That(ex.Message, Does.Contain("LwwRegister"));
    }

    [Test]
    public void CrdtApply_OrMap_without_registered_shape_throws()
    {
        // No DI registry, no per-tree OR-Map registration -> the
        // fallback registry has no OR-Map entry and the leaf must
        // surface a configuration error rather than silently
        // mis-dispatching the typed delta.
        var grain = CreateCrdtGrain();
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrMap, Encoding.UTF8.GetBytes("{}")));
        Assert.That(ex!.Message, Does.Contain("CrdtShape"));
        Assert.That(ex!.Message, Does.Contain("OrMap"));
    }

    // ── happy paths ────────────────────────────────────────────

    [Test]
    public async Task CrdtApply_OrSet_stores_post_merge_state_in_legacy_row()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateCrdtGrain(state);

        var delta = new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("apple"), ReplicaId = "r1", Counter = 1 },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var bytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);

        var result = await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, bytes);

        Assert.That(result.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
        Assert.That(result.Split, Is.Null);
        Assert.That(grain.EntriesForTest.ContainsKey("k"), Is.True);
        var row = grain.EntriesForTest["k"];
        Assert.That(row.IsTombstone, Is.False);
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(row.Value!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("apple")), Is.True);
    }

    [Test]
    public async Task CrdtApply_sequential_OrSet_deltas_fold_into_existing_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateCrdtGrain(state);

        var d1 = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("a"), ReplicaId = "r1", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var d2 = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("b"), ReplicaId = "r2", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, JsonLatticeSerializer<OrSetDelta>.Default.Serialize(d1));
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, JsonLatticeSerializer<OrSetDelta>.Default.Serialize(d2));

        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(grain.EntriesForTest["k"].Value!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("a")), Is.True);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("b")), Is.True);
    }

    [Test]
    public async Task CrdtApply_on_tombstoned_key_starts_from_empty_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateCrdtGrain(state);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("legacy"));
        await grain.DeleteAsync("k");
        Assert.That(grain.EntriesForTest["k"].IsTombstone, Is.True);

        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 7 },
            Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
        };
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.PnCounter, JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta));

        var observed = JsonLatticeSerializer<PnCounter>.Default.Deserialize(grain.EntriesForTest["k"].Value!);
        Assert.That(observed.Value, Is.EqualTo(7));
        Assert.That(grain.EntriesForTest["k"].IsTombstone, Is.False);
    }

    [Test]
    public async Task CrdtApply_advances_HLC_version_and_per_replica_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateCrdtGrain(state);
        var before = state.State.Version.Clone();

        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 1 },
            Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
        };
        var result = await grain.ApplyCrdtDeltaAsync("c", LatticeMergeMode.PnCounter, JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta));

        Assert.That(result.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
        Assert.That(state.State.Version.DominatesOrEquals(before), Is.True);
        Assert.That(before.DominatesOrEquals(state.State.Version), Is.False);
    }

    [Test]
    public async Task CrdtApply_appends_WAL_record_carrying_mode_and_delta_payload()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateCrdtGrain(state, commitLog: commitLog);

        var delta = new VersionVectorDelta
        {
            Entries = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                ["r1"] = new HybridLogicalClock { WallClockTicks = 1, Counter = 1 },
            },
        };
        var deltaBytes = JsonLatticeSerializer<VersionVectorDelta>.Default.Serialize(delta);

        await grain.ApplyCrdtDeltaAsync("v", LatticeMergeMode.VersionVector, deltaBytes);

        Assert.That(commitLog.AppendCount, Is.EqualTo(1));
        var record = commitLog.Appended[0];
        Assert.That(record.Op, Is.EqualTo(MutationKind.Set));
        Assert.That(record.Key, Is.EqualTo("v"));
        Assert.That(record.Mode, Is.EqualTo(LatticeMergeMode.VersionVector));
        Assert.That(record.Delta, Is.EqualTo(deltaBytes));
        Assert.That(record.Value, Is.Not.Null, "CRDT WAL record must also carry the post-merge snapshot so projection rebuild does not require a separate state row");
    }

    [Test]
    public async Task CrdtApply_with_no_commit_log_writer_still_persists_in_memory_state()
    {
        // Test fixture without ICommitLogWriter (single-cluster /
        // non-replicated config). The leaf must still produce a
        // correct post-merge byte[] row even though no WAL append
        // occurs.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateCrdtGrain(state);

        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("x"), ReplicaId = "r1", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var result = await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta));

        Assert.That(result.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(grain.EntriesForTest["k"].Value!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("x")), Is.True);
    }

    [Test]
    public async Task CrdtApply_MvRegister_writes_dot_tagged_entry_to_legacy_row()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateCrdtGrain(state);
        var delta = new MvRegisterDelta
        {
            Entries = new[]
            {
                new MvRegisterEntry { ReplicaId = "r1", Counter = 1, Value = Encoding.UTF8.GetBytes("hello") },
            },
            Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 1 },
        };

        await grain.ApplyCrdtDeltaAsync("m", LatticeMergeMode.MvRegister, JsonLatticeSerializer<MvRegisterDelta>.Default.Serialize(delta));

        var observed = JsonLatticeSerializer<MvRegister>.Default.Deserialize(grain.EntriesForTest["m"].Value!);
        Assert.That(observed.Entries, Has.Count.EqualTo(1));
        Assert.That(observed.Entries[0].ReplicaId, Is.EqualTo("r1"));
        Assert.That(observed.Entries[0].Value, Is.EqualTo(Encoding.UTF8.GetBytes("hello")));
    }

    [Test]
    public async Task CrdtApply_publishes_mutation_with_typed_delta_payload_on_observers()
    {
        // The accessor migration relies on the leaf grain stamping
        // LatticeMutation.Delta with the typed delta bytes so mutation
        // observers (and the change-feed adapter that builds on top of
        // them) keep seeing the producer's typed delta rather than the
        // post-merge byte[] state.
        var observer = new RecordingMutationObserver();
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-crdt-delta";
        var grain = CreateGrainWithObserver(observer, state, "tree-crdt-delta");

        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("z"), ReplicaId = "r1", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);

        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, deltaBytes);

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        var m = observer.Mutations[0];
        Assert.That(m.Kind, Is.EqualTo(MutationKind.Set));
        Assert.That(m.Delta, Is.EqualTo(deltaBytes), "leaf must publish the producer's typed delta payload, not the post-merge state row");
    }
}
