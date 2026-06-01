using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the "persisted / in-memory divergence on
/// <c>WriteStateAsync</c> failure" anti-pattern (bug-hunter Class B,
/// idempotency-guarded shape) on <see cref="ShardRootGrain"/>'s
/// private <c>EnsureRootAsync</c> initialise-once path.
///
/// <para>
/// <c>EnsureRootAsync</c> is the canonical first-touch entrypoint on a
/// fresh shard: every public operation runs through
/// <c>PrepareForOperationAsync</c>, which calls <c>EnsureRootAsync</c>
/// before touching the tree. The method is guarded with
/// <c>if (state.State.RootNodeId is not null) return;</c> - so a
/// transient <c>WriteStateAsync</c> failure that leaves <c>RootNodeId</c>
/// in memory (but not on disk) turns into a permanent split-brain for
/// the activation's lifetime: every retry hits the guard and short-circuits,
/// the activation continues serving against a root grain id that no
/// peer believes owns this shard. The <c>IsRegistered</c> mutation is
/// similarly load-bearing - if it's dirty in memory but unwritten,
/// future calls skip the registry registration even though storage
/// still says the tree is unregistered.
/// </para>
///
/// <para>
/// Class B sibling sites already fixed: cycle 2 (<c>BPlusInternalGrain</c>
/// quad) and cycle 3 (<c>TreeDeletionGrain</c> + <c>ShardRootGrain</c>
/// lifecycle). This cycle bundles <c>EnsureRootAsync</c> with the
/// <c>BPlusLeafGrain</c> leaf-init quad - they form a cohesive leaf-init
/// surface that the shard root activates atomically.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainEnsureRootTests
{
    private const string TreeId = "ensureroot-tree";
    private const string ShardKey = TreeId + "/0";

    private static (ShardRootGrain Grain, FakePersistentState<ShardRootState> State) CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        // Test precondition: a freshly created shard root with no
        // registered tree, no root node id, and no recovery breadcrumbs.
        // Any non-default field here would bypass the EnsureRootAsync
        // mutations we're trying to observe. RootIsLeaf defaults to true
        // (see ShardRootState.cs:17), so the EnsureRootAsync assignment
        // `state.State.RootIsLeaf = true;` is a no-op transition - the
        // load-bearing mutations are IsRegistered (false -> true) and
        // RootNodeId (null -> <deterministic id>).
        Assert.That(state.State.RootNodeId, Is.Null);
        Assert.That(state.State.IsRegistered, Is.False);
        Assert.That(state.State.IsDeleted, Is.False);

        var factory = Substitute.For<IGrainFactory>();

        // Stub the registry: RegisterAsync succeeds for our tree id.
        // We deliberately want this to succeed so the only failure
        // point in the EnsureRootAsync sequence is the outer
        // state.WriteStateAsync() call - that's the mutate-then-persist
        // failure mode under test.
        var registry = Substitute.For<ILatticeRegistry>();
        registry.RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // Stub the freshly-created root leaf: SetTreeIdAsync /
        // SetShardIndexAsync succeed. Both are cross-grain calls that
        // run BEFORE the outer state.WriteStateAsync() in
        // EnsureRootAsync - they are idempotent on retry by the cycle-1
        // leaf-init fix, so leaving them as-is across a thrown
        // WriteStateAsync is safe.
        //
        // EnsureRootAsync also calls leafGrain.GetGrainId() (the
        // IAddressable extension method) to capture the freshly-created
        // leaf's identity. The extension dispatches on `is IGrainBase`
        // / `is Grain` and falls through to `throw` for unrecognised
        // proxies; we satisfy it by substituting `IBPlusLeafGrain + IGrainBase`
        // together and wiring IGrainBase.GrainContext.GrainId to the
        // deterministic id ShardRootGrain.DeterministicGuid produces
        // (or any non-default GrainId - the test only asserts the
        // value reverts on failure, not that it matches a specific id).
        var leafGrainContext = Substitute.For<IGrainContext>();
        var leafGrainId = GrainId.Create("leaf", "ensureroot-test-leaf");
        leafGrainContext.GrainId.Returns(leafGrainId);
        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)leaf).GrainContext.Returns(leafGrainContext);
        leaf.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        leaf.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            factory: factory);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, state);
    }

    [Test]
    public void EnsureRoot_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: fresh shard root, all three EnsureRootAsync-mutated
        // fields at their default values.
        var (grain, state) = CreateGrain();

        var isRegisteredBefore = state.State.IsRegistered;
        var rootNodeIdBefore = state.State.RootNodeId;

        // Arrange: the outer state.WriteStateAsync() inside
        // EnsureRootAsync will throw - the cross-grain RegisterAsync /
        // SetTreeIdAsync / SetShardIndexAsync calls succeed first
        // (their results are durably persisted on the registry +
        // leaf grains, idempotently re-runnable on retry by the
        // already-fixed leaf-init guards).
        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        // Act: any public operation routes through PrepareForOperationAsync
        // which calls EnsureRootAsync. MergeManyAsync with an empty
        // dictionary is the cheapest such trigger.
        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>()));

        // Assert: every field that EnsureRootAsync mutated in memory
        // must be back at its pre-call value, because storage rejected
        // the write. If any of these survives, the activation will
        // short-circuit at the `if (state.State.RootNodeId is not null) return;`
        // guard on every subsequent call - and route to a leaf
        // grain id (or skip registry registration) that storage will
        // never confirm.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.IsRegistered, Is.EqualTo(isRegisteredBefore),
                "IsRegistered mutated in memory survived a failing "
                + "WriteStateAsync; the shard now believes the tree is "
                + "registered while the registry never recorded it.");
            Assert.That(state.State.RootNodeId, Is.EqualTo(rootNodeIdBefore),
                "RootNodeId mutated in memory survived a failing "
                + "WriteStateAsync; the EnsureRootAsync guard now "
                + "short-circuits every retry, leaving the activation "
                + "routing against a root id storage never accepted.");
        });
    }

    [Test]
    public async Task EnsureRoot_adopts_persisted_topology_revealed_on_reread_without_seeding()
    {
        // Regression for the reactivation topology-clobber bug: a shard
        // root that reactivates against not-yet-visible (empty) in-memory
        // state must NOT seed a single-leaf root over a live persisted
        // topology. EnsureRootAsync re-reads storage before seeding; this
        // test simulates storage revealing a promoted internal-root
        // topology on that re-read.
        var (grain, state) = CreateGrain();

        var persistedRoot = GrainId.Create("internal", "persisted-internal-root");
        // The grain activates believing the shard is brand new
        // (RootNodeId null). Storage, however, already holds a promoted
        // internal root + populated leaf chain. The defensive re-read in
        // EnsureRootAsync surfaces it.
        state.OnReadState = s =>
        {
            s.State.RootNodeId = persistedRoot;
            s.State.RootIsLeaf = false;
            s.State.IsRegistered = true;
        };

        // Act: trigger EnsureRootAsync via the cheapest public operation.
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.RootNodeId, Is.EqualTo(persistedRoot),
                "EnsureRootAsync overwrote the persisted internal root "
                + "with a freshly-seeded leaf id - the exact topology-loss "
                + "mode that drops every key under the rest of the tree.");
            Assert.That(state.State.RootIsLeaf, Is.False,
                "EnsureRootAsync reset RootIsLeaf to true, collapsing a "
                + "promoted internal root back to a single leaf.");
            Assert.That(state.ReadCount, Is.GreaterThanOrEqualTo(1),
                "EnsureRootAsync did not re-read storage before deciding "
                + "to seed, so it cannot have observed the live topology.");
            Assert.That(state.WriteCount, Is.Zero,
                "EnsureRootAsync persisted a fresh root over an already-"
                + "populated shard; adopting the persisted topology must "
                + "not write.");
        });
    }

    [Test]
    public async Task EnsureRoot_seeds_single_leaf_when_storage_is_genuinely_empty()
    {
        // Counterpart to the adoption test: when the re-read confirms
        // storage is genuinely empty (no prior root), EnsureRootAsync
        // proceeds to seed the deterministic single-leaf root exactly as
        // before. The defensive re-read must not suppress legitimate
        // first-touch initialisation.
        var (grain, state) = CreateGrain();

        // Re-read finds nothing (default no-op hook leaves State empty).
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.RootNodeId, Is.Not.Null,
                "EnsureRootAsync failed to seed a root on a genuinely "
                + "empty shard; first-touch initialisation regressed.");
            Assert.That(state.State.RootIsLeaf, Is.True,
                "A freshly-seeded root must be a leaf.");
            Assert.That(state.State.IsRegistered, Is.True,
                "EnsureRootAsync did not register the tree on first touch.");
            Assert.That(state.WriteCount, Is.EqualTo(1),
                "First-touch seed must persist exactly one shard-root write.");
        });
    }

    [Test]
    public async Task EnsureRoot_under_concurrent_first_touch_seeds_exactly_one_root()
    {
        // The seed sequence runs behind a per-activation init gate so two
        // interleaved [AlwaysInterleave] turns that both observe a null
        // in-memory RootNodeId cannot both create-and-persist a leaf root
        // (the second write would orphan the first turn's leaf). Drive two
        // concurrent first-touch operations and assert a single seed.
        var (grain, state) = CreateGrain();

        var t1 = grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());
        var t2 = grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());
        await Task.WhenAll(t1, t2);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.RootNodeId, Is.Not.Null,
                "Concurrent first-touch left the shard with no root.");
            Assert.That(state.WriteCount, Is.EqualTo(1),
                "Concurrent first-touch seeded the root more than once; "
                + "the init gate did not serialise the seed sequence.");
        });
    }
}
