using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the unseeded-leaf CRDT paths. A leaf that reaches a
/// CRDT apply (or a prepared CRDT terminal-commit fold) before the owning shard
/// root attached it with a tree id used to coalesce the unset id to
/// <see cref="string.Empty"/> and hand it to
/// <c>CrdtShapeRegistry.TryGet</c>, whose
/// <c>ArgumentException.ThrowIfNullOrEmpty</c> guard then raised an opaque
/// "The value cannot be an empty string. (Parameter 'treeId')" naming neither
/// the grain, the key, nor the mode - and made the informative
/// <see cref="LatticeCrdtShapeNotRegisteredException"/> written directly beneath
/// it unreachable.
/// <para>
/// The leaf now short-circuits before consulting the registry, so both paths
/// surface the typed, actionable exception the API bindings already map to a
/// client-side precondition error. See issue #1740.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearCrdtUnseededLeafAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
        LatticeDeltaContext.Current = null;
    }

    /// <summary>
    /// Stub merge-mode resolver that reports a fixed CRDT mode for every tree,
    /// so a prepared write on an unattached leaf still stages a typed delta and
    /// drives the terminal-commit fold.
    /// </summary>
    private sealed class FixedMergeModeResolver(LatticeMergeMode mode) : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => mode;
    }

    private static byte[] SingleAddOrSetDelta(string element, string replicaId = "r1") =>
        JsonLatticeSerializer<OrSetDelta>.Default.Serialize(new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot
                {
                    Element = Encoding.UTF8.GetBytes(element),
                    ReplicaId = replicaId,
                    Counter = 1,
                },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        });

    // ── producer-side apply ────────────────────────────────────

    [Test]
    public void CrdtApply_on_unseeded_leaf_throws_typed_shape_exception_not_opaque_ArgumentException()
    {
        // No SetTreeIdAsync has run, so state.TreeId is null. A closed-shape
        // mode is used deliberately: the global registry fallback would resolve
        // OrSet for any real tree, so the only reason this can fault is the
        // missing tree id - which previously surfaced as an ArgumentException
        // from inside CrdtShapeRegistry.TryGet.
        var grain = CreateGrain();

        var ex = Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(async () =>
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, SingleAddOrSetDelta("apple")));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.Not.InstanceOf<ArgumentException>(),
                "The unseeded leaf must not surface the registry's opaque empty-string ArgumentException.");
            Assert.That(ex!.TreeId, Is.Empty);
            Assert.That(ex.Message, Does.Contain("no tree id bound"));
            Assert.That(ex.Message, Does.Contain("test-leaf"), "the fault must name the grain");
            Assert.That(ex.Message, Does.Contain("'k'"), "the fault must name the key");
            Assert.That(ex.Message, Does.Contain("OrSet"), "the fault must name the merge mode");
        });
    }

    [Test]
    public void CrdtApply_on_unseeded_leaf_faults_for_OrMap_too()
    {
        var grain = CreateGrain();

        var ex = Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(async () =>
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrMap, Encoding.UTF8.GetBytes("{}")));

        Assert.That(ex!.Message, Does.Contain("no tree id bound"));
    }

    [Test]
    public void CrdtApply_on_unseeded_leaf_faults_before_mutating_the_leaf()
    {
        // Fail-closed: the guard runs before any state is folded, so the key
        // must not exist afterwards.
        var grain = CreateGrain();

        Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(async () =>
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, SingleAddOrSetDelta("apple")));

        Assert.That(grain.EntriesForTest.ContainsKey("k"), Is.False);
    }

    [Test]
    public async Task CrdtApply_on_seeded_leaf_still_resolves_the_global_closed_shape()
    {
        // Positive control: the guard must not have made a normally-attached
        // leaf reject a closed-shape apply.
        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = "attached-tree" } };
        var grain = CreateGrain(state);

        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, SingleAddOrSetDelta("apple"));

        var row = grain.EntriesForTest["k"];
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(row.Value!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("apple")), Is.True);
    }

    // ── prepared-saga terminal-commit fold ─────────────────────

    [Test]
    public async Task PreparedCrdtFold_on_unseeded_leaf_throws_typed_shape_exception()
    {
        // Stage a prepared CRDT-mode write (delta context + prepared scope) on
        // a leaf the shard root never attached, then drive the terminal so the
        // drain routes the key through FoldPreparedCrdtDelta.
        var grain = CreateGrain(mergeModeResolver: new FixedMergeModeResolver(LatticeMergeMode.OrSet));
        var txid = Guid.NewGuid();

        await PrepareCrdtWriteAsync(grain, txid, "k", SingleAddOrSetDelta("apple"));

        var ex = Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(async () =>
            await grain.ApplyTxTerminalAsync(txid, committed: true));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.Not.InstanceOf<ArgumentException>());
            Assert.That(ex!.TreeId, Is.Empty);
            Assert.That(ex.Message, Does.Contain("no tree id bound"));
            Assert.That(ex.Message, Does.Contain("terminal-commit fold"));
            Assert.That(ex.Message, Does.Contain("'k'"));
        });
    }

    [Test]
    public async Task PreparedCrdtFold_on_seeded_leaf_still_folds_the_typed_delta()
    {
        // Positive control for the same drain path on an attached leaf.
        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = "attached-tree" } };
        var grain = CreateGrain(state, mergeModeResolver: new FixedMergeModeResolver(LatticeMergeMode.OrSet));
        var txid = Guid.NewGuid();

        await PrepareCrdtWriteAsync(grain, txid, "k", SingleAddOrSetDelta("apple"));
        await grain.ApplyTxTerminalAsync(txid, committed: true);

        var row = grain.EntriesForTest["k"];
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(row.Value!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("apple")), Is.True);
    }

    private static async Task PrepareCrdtWriteAsync(
        BPlusLeafGrain grain, Guid txid, string key, byte[] delta)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticeDeltaContext.With(delta))
            using (LatticePreparedContext.BeginScope())
            {
                await grain.SetAsync(key, Encoding.UTF8.GetBytes("staged"));
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }
}
