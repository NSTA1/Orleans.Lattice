using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit-level coverage for the leaf-grain post-merge observer seam
/// (<see cref="ILatticeMergeObserver"/> wiring in
/// <c>BPlusLeafGrain.MergeObserver.cs</c>). Instantiates the leaf directly with
/// a stub <see cref="IServiceProvider"/> that returns a scripted observer, so
/// each failure points at a single source file with no Orleans runtime in
/// scope. Verifies the LWW transform/annotate paths, the CRDT non-mutation
/// invariant (AcceptTransformed rejected for a non-LWW record), and the
/// zero-cost null default.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>A scripted merge observer that records every context it sees and returns a fixed outcome.</summary>
    private sealed class RecordingMergeObserver(Func<LatticeMergeMode, LatticeMergeOutcome> outcome) : ILatticeMergeObserver
    {
        public List<(string Key, LatticeMergeMode Mode, byte[]? Local, byte[]? Incoming, byte[] Merged)> Calls { get; } = new();

        public ValueTask<LatticeMergeOutcome> OnMergedAsync(in LatticeMergeContext ctx, CancellationToken ct)
        {
            Calls.Add((ctx.Key, ctx.Mode, ctx.LocalValue, ctx.IncomingValue, ctx.MergedValue));
            return new ValueTask<LatticeMergeOutcome>(outcome(ctx.Mode));
        }
    }

    private static BPlusLeafGrain CreateObserverGrain(
        ILatticeMergeObserver observer,
        ILatticeValueDecoder? decoder = null,
        FakePersistentState<LeafNodeState>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        state ??= new FakePersistentState<LeafNodeState>();
        if (string.IsNullOrEmpty(state.State.TreeId))
            state.State.TreeId = "test-tree";

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ILatticeMergeObserver)).Returns(observer);
        if (decoder is not null)
            services.GetService(typeof(ILatticeValueDecoder)).Returns(decoder);
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        return new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
    }

    // ── LWW transform / annotate ───────────────────────────────

    [Test]
    public async Task Set_with_transforming_observer_stores_transformed_bytes()
    {
        var transformed = Encoding.UTF8.GetBytes("normalised");
        var observer = new RecordingMergeObserver(_ => LatticeMergeOutcome.AcceptTransformed(transformed));
        var grain = CreateObserverGrain(observer);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("raw"));

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(transformed));
        Assert.That(observer.Calls, Has.Count.EqualTo(1));
        Assert.That(observer.Calls[0].Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        Assert.That(observer.Calls[0].Incoming, Is.EqualTo(Encoding.UTF8.GetBytes("raw")));
    }

    [Test]
    public async Task Set_with_annotating_observer_keeps_bytes_verbatim()
    {
        var observer = new RecordingMergeObserver(_ => LatticeMergeOutcome.AcceptWithEvent("looks-fine"));
        var grain = CreateObserverGrain(observer);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("raw"));

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(Encoding.UTF8.GetBytes("raw")));
        Assert.That(observer.Calls, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Set_with_accepting_observer_keeps_bytes_verbatim()
    {
        var observer = new RecordingMergeObserver(_ => LatticeMergeOutcome.Accept());
        var grain = CreateObserverGrain(observer);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("raw"));

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(Encoding.UTF8.GetBytes("raw")));
        Assert.That(observer.Calls, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Set_observer_sees_prior_value_as_local_input()
    {
        var observer = new RecordingMergeObserver(_ => LatticeMergeOutcome.Accept());
        var grain = CreateObserverGrain(observer);

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("first"));
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("second"));

        // Second call's context carries the first value as the local input.
        Assert.That(observer.Calls, Has.Count.EqualTo(2));
        Assert.That(observer.Calls[0].Local, Is.Null);
        Assert.That(observer.Calls[1].Local, Is.EqualTo(Encoding.UTF8.GetBytes("first")));
    }

    // ── CRDT non-mutation invariant ────────────────────────────

    [Test]
    public void CrdtApply_with_transforming_observer_throws_for_non_LWW_record()
    {
        var observer = new RecordingMergeObserver(_ =>
            LatticeMergeOutcome.AcceptTransformed(Encoding.UTF8.GetBytes("nope")));
        var grain = CreateObserverGrain(observer);

        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("a"), ReplicaId = "r1", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var bytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, bytes));
        Assert.That(ex!.Message, Does.Contain("AcceptTransformed"));
        Assert.That(ex.Message, Does.Contain("OrSet"));
    }

    [Test]
    public async Task CrdtApply_with_accepting_observer_succeeds_and_is_invoked_with_crdt_mode()
    {
        var observer = new RecordingMergeObserver(_ => LatticeMergeOutcome.Accept());
        var grain = CreateObserverGrain(observer);

        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("a"), ReplicaId = "r1", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var bytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);

        var result = await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, bytes);

        Assert.That(result.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
        Assert.That(observer.Calls, Has.Count.EqualTo(1));
        Assert.That(observer.Calls[0].Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        // Incoming is null for a delta-based CRDT apply; the merged result is
        // the post-merge OrSet state bytes.
        Assert.That(observer.Calls[0].Incoming, Is.Null);
        Assert.That(observer.Calls[0].Merged, Is.Not.Null);
    }

    [Test]
    public async Task CrdtApply_with_annotating_observer_succeeds()
    {
        var observer = new RecordingMergeObserver(_ => LatticeMergeOutcome.AcceptWithEvent("ok"));
        var grain = CreateObserverGrain(observer);

        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("a"), ReplicaId = "r1", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var bytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);

        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, bytes);

        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(grain.EntriesForTest["k"].Value!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("a")), Is.True);
    }

    // ── zero-cost null default ─────────────────────────────────

    [Test]
    public async Task Set_with_null_default_observer_stores_bytes_verbatim()
    {
        // The null-default observer resolves inactive: no observer call, no
        // transform - the write is byte-for-byte identical to the pre-seam path.
        var grain = CreateObserverGrain(new NullLatticeMergeObserver());

        await grain.SetAsync("k", Encoding.UTF8.GetBytes("raw"));

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(Encoding.UTF8.GetBytes("raw")));
    }
}
