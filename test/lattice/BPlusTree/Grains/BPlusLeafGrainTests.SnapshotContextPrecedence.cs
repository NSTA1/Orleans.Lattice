using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2325 (item 1). Pins the mechanism that actually delivers
/// point-in-time snapshot consistency to a cursor's <c>Next*Async</c>
/// step: while a <see cref="LatticeRegistrySnapshotContext"/> scope is in
/// force the leaf answers pending-saga status from the captured snapshot
/// and does <b>not</b> consult the per-tree <see cref="ITxRegistryGrain"/>
/// at all.
/// <para>
/// Both short-circuit sites are covered, because they are separate code:
/// <c>ResolvePendingStatusAsync</c> on the single-key path reached through
/// <see cref="IBPlusLeafGrain.GetAsync"/>, and the batched resolution on
/// the multi-key path reached through
/// <see cref="IBPlusLeafGrain.GetManyAsync"/>. Removing either one leaves
/// the other's test green, so one test per site is the minimum that fails
/// on either regression.
/// </para>
/// <para>
/// This needs its own fixture because the guarantee was documented as
/// coming from <c>SnapshotPin</c>, which cannot deliver it: a pin only
/// exempts a txid from registry-side tombstone pruning, and on this path
/// no registry lookup happens for that exemption to affect. The sibling
/// <c>PendingReadCoverage</c> fixture already scopes an ambient, but only
/// as a stand-in for a registry it never wires, so it cannot tell the
/// short-circuit from a registry that happens to agree - it stays green if
/// the short-circuit is deleted.
/// </para>
/// <para>
/// Each pair is matched on purpose. The unscoped test proves the
/// substitute registry really is reachable from this grain and really does
/// change the observed value, which is what stops the scoped test's
/// <c>DidNotReceive</c> from passing vacuously against a registry that was
/// never wired up in the first place.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string SnapshotPrecedenceTreeId = "precedence-tree";

    /// <summary>
    /// Builds a leaf whose registry substitute answers every status query -
    /// single and batched - with <paramref name="registryAnswer"/>.
    /// </summary>
    private static (BPlusLeafGrain Grain, ITxRegistryGrain Registry) BuildLeafWithRegistry(TxStatus registryAnswer)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(registryAnswer));
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>()).Returns(call =>
        {
            var answers = new Dictionary<Guid, TxStatus>();
            foreach (var t in call.Arg<IReadOnlyList<Guid>>()) answers[t] = registryAnswer;
            return Task.FromResult(answers);
        });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);

        var state = new FakePersistentState<LeafNodeState>();
        // A non-empty TreeId is what makes the registry fall-back reachable at
        // all; without it both resolution paths short-circuit before asking the
        // factory for a registry grain, and the unscoped controls below could
        // not distinguish "the registry answered" from "the registry was never
        // consulted", which is the whole distinction under test.
        state.State.TreeId = SnapshotPrecedenceTreeId;

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "precedence-leaf"));
        context.ActivationServices.Returns(new ServiceCollection().BuildServiceProvider());

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
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

        return (grain, registry);
    }

    /// <summary>
    /// Seeds "k" with the pre-saga value 1 and buckets a prepared write of 2
    /// under <paramref name="txid"/>, so the value a reader returns names the
    /// status it resolved: 2 means Committed, 1 means InFlight.
    /// </summary>
    private static async Task SeedPreparedOverrideAsync(BPlusLeafGrain grain, Guid txid)
    {
        await grain.SetAsync("k", [1]);
        await PreparePendingSetAsync(grain, txid, "k", [2, 2]);
    }

    // ---- single-key path (BPlusLeafGrain.ResolvePendingStatusAsync) ----

    [Test]
    public async Task GetAsync_unscoped_resolves_the_pending_saga_through_the_registry()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var value = await grain.GetAsync("k");

        Assert.That(value, Is.EqualTo(new byte[] { 2, 2 }),
            "unscoped, the leaf must take the registry's Committed reading and surface the prepared value");
        await registry.Received().GetStatusAsync(txid);
    }

    [Test]
    public async Task GetAsync_under_a_snapshot_scope_answers_from_the_snapshot_without_consulting_the_registry()
    {
        LatticeRegistrySnapshotContext.Current = null;
        // InFlight is exactly the reading a cursor must never see for a saga its
        // snapshot captured as terminal: it is what the registry returns once
        // the tombstone has been forgotten and pruned.
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.InFlight);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        using (LatticeRegistrySnapshotContext.BeginScope(
            new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed }))
        {
            var value = await grain.GetAsync("k");

            Assert.That(value, Is.EqualTo(new byte[] { 2, 2 }),
                "the scoped read must take the snapshot's captured Committed reading, not the registry's InFlight one");
        }

        await registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
        await registry.DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
    }

    // ---- batched path (BPlusLeafGrain pending-read snapshot build) ----

    [Test]
    public async Task GetManyAsync_unscoped_resolves_the_pending_saga_through_the_registry()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var result = await grain.GetManyAsync(["k"]);

        Assert.That(result["k"], Is.EqualTo(new byte[] { 2, 2 }),
            "unscoped, the leaf must take the registry's Committed reading and surface the prepared value");
        await registry.Received().GetStatusManyAsync(Arg.Is<IReadOnlyList<Guid>>(ts => ts.Contains(txid)));
    }

    [Test]
    public async Task GetManyAsync_under_a_snapshot_scope_answers_from_the_snapshot_without_consulting_the_registry()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.InFlight);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        using (LatticeRegistrySnapshotContext.BeginScope(
            new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed }))
        {
            var result = await grain.GetManyAsync(["k"]);

            Assert.That(result["k"], Is.EqualTo(new byte[] { 2, 2 }),
                "the scoped scan must take the snapshot's captured Committed reading, not the registry's InFlight one");
        }

        await registry.DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
        await registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task GetManyAsync_under_a_snapshot_scope_treats_a_txid_absent_from_the_snapshot_as_in_flight()
    {
        LatticeRegistrySnapshotContext.Current = null;
        // The registry would say Committed; strict isolation says a saga the
        // snapshot never captured is invisible to this step regardless, and the
        // snapshot is what decides.
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus>()))
        {
            var result = await grain.GetManyAsync(["k"]);

            Assert.That(result["k"], Is.EqualTo(new byte[] { 1 }),
                "a saga absent from the snapshot must fall through to the pre-saga value");
        }

        await registry.DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
        await registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }
}
