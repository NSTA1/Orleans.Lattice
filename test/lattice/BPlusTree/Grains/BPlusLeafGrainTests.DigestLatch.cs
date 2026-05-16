using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the one-way digest latch path: when a leaf grain
/// observes its first mutation while the resolved
/// <see cref="LatticeOptions.MaintainProjectionDigest"/> is <c>false</c>,
/// it stamps
/// <see cref="State.TreeRegistryEntry.ProjectionDigestPermanentlyDisabled"/>
/// to <c>true</c> via the registry so a later config flip cannot expose
/// a stale digest. System trees and trees with empty / null tree ids
/// skip the stamp.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Builds a leaf grain wired to an <see cref="ILatticeRegistry"/>
    /// mock so the latch path can be observed. Returns the mock for
    /// per-test assertions and the grain for issuing mutations.
    /// </summary>
    private static (BPlusLeafGrain Grain, ILatticeRegistry Registry, FakePersistentState<LeafNodeState> State) CreateGrainWithRegistry(
        string treeId,
        LatticeOptions options,
        TreeRegistryEntry? registryEntry = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        var state = new FakePersistentState<LeafNodeState>
        {
            State = new LeafNodeState { TreeId = treeId },
        };

        var grainFactory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var pinned = registryEntry ?? new TreeRegistryEntry
        {
            MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = LatticeConstants.DefaultShardCount,
        };
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(pinned));

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var resolver = new LatticeOptionsResolver(grainFactory, monitor);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        return (grain, registry, state);
    }

    [Test]
    public async Task First_mutation_with_digest_disabled_stamps_the_latch_once()
    {
        var (grain, registry, _) = CreateGrainWithRegistry(
            treeId: "user-tree",
            options: new LatticeOptions { MaintainProjectionDigest = false });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await registry.Received(1).LatchProjectionDigestPermanentlyDisabledAsync("user-tree");
    }

    [Test]
    public async Task Subsequent_mutations_skip_repeat_latch_stamps_in_same_activation()
    {
        var (grain, registry, _) = CreateGrainWithRegistry(
            treeId: "user-tree",
            options: new LatticeOptions { MaintainProjectionDigest = false });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        await grain.DeleteAsync("k1");

        // The activation guard means the registry is hit at most once.
        // The registry-side method is itself idempotent, but skipping
        // the cross-grain hop avoids churn.
        await registry.Received(1).LatchProjectionDigestPermanentlyDisabledAsync("user-tree");
    }

    [Test]
    public async Task Mutations_with_digest_enabled_never_stamp_the_latch()
    {
        var (grain, registry, _) = CreateGrainWithRegistry(
            treeId: "user-tree",
            options: new LatticeOptions { MaintainProjectionDigest = true });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        await registry.DidNotReceive().LatchProjectionDigestPermanentlyDisabledAsync(Arg.Any<string>());
    }

    [Test]
    public async Task System_tree_mutations_never_stamp_the_latch()
    {
        // System trees bypass the registry entirely; the resolver also
        // forces MaintainProjectionDigest = false for them, so the latch
        // path would otherwise fire on every mutation. The trimmed path
        // must short-circuit on the SystemTreePrefix check.
        var (grain, registry, _) = CreateGrainWithRegistry(
            treeId: LatticeConstants.SystemTreePrefix + "demo",
            options: new LatticeOptions { MaintainProjectionDigest = true });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await registry.DidNotReceive().LatchProjectionDigestPermanentlyDisabledAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Latch_stamp_failure_does_not_fail_the_mutation()
    {
        var (grain, registry, state) = CreateGrainWithRegistry(
            treeId: "user-tree",
            options: new LatticeOptions { MaintainProjectionDigest = false });
        registry
            .When(r => r.LatchProjectionDigestPermanentlyDisabledAsync(Arg.Any<string>()))
            .Do(_ => throw new InvalidOperationException("simulated registry failure"));

        // The user-visible mutation must still succeed despite the
        // registry hop failing. The latch is best-effort; on a failure
        // the activation guard is reset so a later mutation can retry.
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
    }
}

