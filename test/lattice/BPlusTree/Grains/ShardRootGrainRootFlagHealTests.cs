using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the HEAL half of issue 899 / issue 1883: correcting a
/// persisted <c>RootIsLeaf</c> flag that is baked <c>true</c> over a root that is
/// actually a <c>BPlusInternalGrain</c>.
/// <para>
/// The population exists because <c>ShardRootState.RootIsLeaf</c> carried a
/// <c>= true</c> initializer over a member whose "off" value is the CLR type
/// default. The grain-storage serializer omits type defaults, so a correctly
/// written <c>false</c> was dropped from the blob and resurrected as <c>true</c> on
/// load; the next state write then persisted that <c>true</c> literally.
/// <c>PersistedStateDefaultInitializerContractTests</c> pins the cure. This fixture
/// pins the repair of the shards that were already re-saved, whose blobs literally
/// contain <c>"RootIsLeaf":true</c> and for which no round trip reconstructs
/// anything.
/// </para>
/// <para>
/// <b>Every assertion here is worthless unless the leaf-grain-type probe is live.</b>
/// <c>IsLeafGrainId</c> answers <see langword="true"/> for every id when the leaf
/// grain type cannot be resolved, so under a plain substitute factory
/// <c>RootIsLeafTyped</c> collapses to the raw flag and the repair's predicate can
/// never be satisfied - every test below would pass vacuously against a completely
/// absent implementation. <see cref="Harness_resolves_the_leaf_grain_type_so_the_type_guard_is_live"/>
/// is the leg that fails when that happens.
/// </para>
/// </summary>
public sealed class ShardRootGrainRootFlagHealTests
{
    private const string TreeId = "tree";
    private const int ShardIndex = 0;

    private static readonly GrainId InternalRootId = GrainId.Create("internal", "promoted-root");
    private static readonly GrainId LeafRootId = GrainId.Create("leaf", "flat-root");

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusInternalGrain Internal { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }

        /// <summary>
        /// The <c>RootIsLeaf</c> values captured at each successful write, so a test
        /// can assert what was actually PERSISTED rather than only what the
        /// in-memory object ended up holding. The distinction is the whole point of
        /// this defect: the pre-fix code wrote the correct value and the round trip
        /// destroyed it.
        /// </summary>
        public required List<bool> PersistedRootIsLeaf { get; init; }

        public Task ActivateAsync() => ((IGrainBase)Grain).OnActivateAsync(CancellationToken.None);
    }

    /// <summary>
    /// Builds a shard-root harness whose grain factory resolves a real
    /// <see cref="GrainType"/> for leaf grains, so the production
    /// <c>IsLeafGrainId</c> type guard genuinely discriminates
    /// <c>GrainId.Create("leaf", ...)</c> from <c>GrainId.Create("internal", ...)</c>.
    /// The leaf substitute also implements <see cref="IGrainBase"/>, which is the
    /// shape Orleans' <c>GetGrainId()</c> extension accepts for a
    /// non-<c>Grain</c> implementation; substituting <see cref="IGrainContext"/>
    /// alone is NOT sufficient and leaves the probe dead.
    /// </summary>
    /// <param name="liveLeafTypeProbe">
    /// When <see langword="false"/>, the leaf reference is a plain substitute whose
    /// <c>GetGrainId()</c> throws, modelling the non-runtime factory under which the
    /// whole issue-899 guard family degrades to a no-op.
    /// </param>
    private static Harness CreateHarness(bool liveLeafTypeProbe = true)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        var persisted = new List<bool>();
        state.OnWriteState = s => persisted.Add(s.RootIsLeaf);

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var leaf = liveLeafTypeProbe
            ? Substitute.For<IBPlusLeafGrain, IGrainBase>()
            : Substitute.For<IBPlusLeafGrain>();
        if (liveLeafTypeProbe)
        {
            var leafContext = Substitute.For<IGrainContext>();
            leafContext.GrainId.Returns(GrainId.Create("leaf", "leaf-type-probe"));
            ((IGrainBase)leaf).GrainContext.Returns(leafContext);
        }

        leaf.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(leaf);

        var @internal = Substitute.For<IBPlusInternalGrain>();
        @internal.GetRoutingTableAsync().Returns(LeafBearingRoutingTable());
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(@internal);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>(), Arg.Any<string>()).Returns(@internal);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(@internal);

        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var shadowTarget = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());

        return new Harness
        {
            Grain = grain,
            Internal = @internal,
            State = state,
            PersistedRootIsLeaf = persisted,
        };
    }

    private static RoutingTableSnapshot LeafBearingRoutingTable() => new()
    {
        SeparatorKeys = [null],
        ChildIds = [GrainId.Create("leaf", "existing-leaf")],
        ChildrenAreLeaves = true,
    };

    /// <summary>
    /// Puts the harness into the exact baked state a census of a pristine production
    /// volume found on 96 shard roots: the persisted flag claims a flat leaf-rooted
    /// tree while <c>RootNodeId</c> addresses an internal grain.
    /// </summary>
    private static Harness CreateBakedHarness()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = InternalRootId;
        h.State.State.RootIsLeaf = true;
        return h;
    }

    /// <summary>
    /// Probe-liveness leg. Every other assertion in this fixture is only meaningful
    /// when the leaf-grain-type probe resolves; if it does not, <c>IsLeafGrainId</c>
    /// answers <see langword="true"/> for every id, <c>RootIsLeafTyped</c> collapses
    /// to the raw flag, and the repair under test can never fire - making the rest
    /// of the fixture pass vacuously against no implementation at all. This test
    /// fails loudly in that case by pinning the one behaviour observable ONLY when
    /// the probe is live: with <c>RootIsLeaf = true</c> over an INTERNAL root id, a
    /// read must not take the flat-tree fast path but must descend through the
    /// internal root's routing table.
    /// </summary>
    [Test]
    public async Task Harness_resolves_the_leaf_grain_type_so_the_type_guard_is_live()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = InternalRootId;
        h.State.State.RootIsLeaf = true;

        await h.Grain.GetAsync("k-any");

        await h.Internal.Received().GetRoutingTableAsync();
    }

    /// <summary>
    /// Primary regression. Activating a shard root whose persisted flag is baked
    /// <c>true</c> over an internal-typed root must correct the flag and PERSIST the
    /// correction - not merely fix it in memory, which the pre-fix serializer round
    /// trip would have thrown away on the next load.
    /// </summary>
    [Test]
    public async Task Activation_repairs_a_baked_RootIsLeaf_flag_over_an_internal_root()
    {
        var h = CreateBakedHarness();

        await h.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.False,
                "The baked flag claimed a leaf root over an internal-typed root node and must be corrected.");
            Assert.That(h.State.WriteCount, Is.EqualTo(1),
                "The repair must be persisted; an in-memory-only correction is lost on the next activation.");
            Assert.That(h.PersistedRootIsLeaf, Is.EqualTo(new[] { false }),
                "The value that reached storage must be false, not merely the value left in the live object.");
        });
    }

    /// <summary>
    /// Idempotency. The repair must be safe to run repeatedly: a second activation
    /// of an already-repaired shard observes a flag that agrees with the node type
    /// and must issue no further storage write.
    /// </summary>
    [Test]
    public async Task Repeated_activation_after_a_repair_issues_no_further_write()
    {
        var h = CreateBakedHarness();

        await h.ActivateAsync();
        await h.ActivateAsync();
        await h.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.False);
            Assert.That(h.State.WriteCount, Is.EqualTo(1),
                "The repair is one-shot per shard: once the flag agrees with the node type the condition can never "
                + "hold again, so the population drains monotonically rather than being re-written every activation.");
        });
    }

    /// <summary>
    /// A truthful flat tree - the flag claims a leaf root and the root id really does
    /// address a leaf grain - must be left completely alone. This is the shape of 681
    /// of the 841 shard roots on the surveyed volume, so a false positive here would
    /// mean a spurious storage write on the overwhelming majority of the fleet.
    /// </summary>
    [Test]
    public async Task Activation_leaves_a_truthful_leaf_rooted_shard_untouched()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = LeafRootId;
        h.State.State.RootIsLeaf = true;

        await h.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.True);
            Assert.That(h.State.WriteCount, Is.Zero);
        });
    }

    /// <summary>
    /// A shard whose blob never carried the member reconstructs with
    /// <c>RootIsLeaf == false</c> now that the property initializer is gone - it has
    /// already self-healed and there is nothing left to write. The repair must
    /// recognise that and stay silent, otherwise the drop-in upgrade would issue an
    /// unnecessary storage write for every internal-rooted shard in the fleet rather
    /// than only for the ones actually baked.
    /// </summary>
    [Test]
    public async Task Activation_writes_nothing_for_a_shard_that_already_self_healed_on_load()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = InternalRootId;
        // Exactly what a blob with no RootIsLeaf member deserialises to once the
        // non-default initializer has been removed.
        h.State.State.RootIsLeaf = new ShardRootState().RootIsLeaf;

        await h.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.False);
            Assert.That(h.State.WriteCount, Is.Zero,
                "A shard the POCO fix already healed needs no write; only a blob that literally carries "
                + "\"RootIsLeaf\":true does.");
        });
    }

    /// <summary>
    /// A shard with no root yet holds <c>RootIsLeaf</c> at whatever the default is and
    /// nothing consults it, because <c>RootNodeId</c> is <see langword="null"/>. The
    /// repair must not treat that as an inconsistency and must not write, or every
    /// cold shard activation would pay a storage write before it has any topology.
    /// </summary>
    [Test]
    public async Task Activation_writes_nothing_when_the_shard_has_no_root_yet()
    {
        var h = CreateHarness();
        h.State.State.RootNodeId = null;
        h.State.State.RootIsLeaf = true;

        await h.ActivateAsync();

        Assert.That(h.State.WriteCount, Is.Zero);
    }

    /// <summary>
    /// The repair is best-effort. A storage failure must neither fail activation -
    /// the shard is fully serviceable, because every path that could act on the flag
    /// is already decided by node type - nor leave the in-memory copy diverged from
    /// what storage still holds. The correction is simply retried on the next
    /// activation.
    /// </summary>
    [Test]
    public async Task A_storage_failure_during_repair_neither_fails_activation_nor_diverges_from_storage()
    {
        var h = CreateBakedHarness();
        h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.DoesNotThrowAsync(async () => await h.ActivateAsync());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.True,
                "On a failed write the in-memory value must be restored to what storage still holds, so the two do "
                + "not diverge.");
            Assert.That(h.PersistedRootIsLeaf, Is.Empty);
        });

        // The next activation retries and succeeds.
        await h.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.False);
            Assert.That(h.PersistedRootIsLeaf, Is.EqualTo(new[] { false }));
        });
    }

    /// <summary>
    /// After the repair, traffic must still route correctly: the read descends
    /// through the internal root's routing table rather than taking the flat-tree
    /// fast path and blind-casting the internal root to <c>IBPlusLeafGrain</c>. This
    /// is the invariant the repair must preserve - correcting the flag changes what
    /// the raw-flag consumers see, so it has to leave routing at least as correct as
    /// the typed guard already made it.
    /// </summary>
    [Test]
    public async Task Traversal_after_the_repair_still_descends_through_the_internal_root()
    {
        var h = CreateBakedHarness();

        await h.ActivateAsync();
        await h.Grain.GetAsync("k-any");

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.False);
            Assert.That(h.Internal.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IBPlusInternalGrain.GetRoutingTableAsync)),
                Is.True,
                "The read must route through the internal root, not through the flat-tree fast path.");
        });
    }

    /// <summary>
    /// Documents - and pins - the deliberate degradation. Under a grain factory that
    /// cannot yield a runtime-typed leaf reference, the leaf grain type is
    /// unresolvable, <c>IsLeafGrainId</c> answers <see langword="true"/> for every
    /// id, and the whole issue-899 guard family (this repair included) becomes a
    /// no-op rather than guessing. A repair that fired here would be correcting a
    /// flag it has no evidence about.
    /// <para>
    /// This is also the leg that explains why the defect survived so much green unit
    /// coverage, and why <see cref="Harness_resolves_the_leaf_grain_type_so_the_type_guard_is_live"/>
    /// has to exist.
    /// </para>
    /// </summary>
    [Test]
    public async Task Repair_is_skipped_when_the_leaf_grain_type_probe_cannot_resolve()
    {
        var h = CreateHarness(liveLeafTypeProbe: false);
        h.State.State.RootNodeId = InternalRootId;
        h.State.State.RootIsLeaf = true;

        await h.ActivateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootIsLeaf, Is.True);
            Assert.That(h.State.WriteCount, Is.Zero);
        });
    }

    /// <summary>
    /// Allocation pin for the path every activation in the fleet takes once the
    /// baked population has drained - which, the repair being one-shot, is
    /// permanently. The check is deliberately a REFERENCE IDENTITY comparison
    /// against the cached <see cref="Task.CompletedTask"/> singleton rather than a
    /// GC-counter measurement: it is exact, immune to tiered JIT and escape
    /// analysis, and fails the moment the entry point starts returning a freshly
    /// allocated <c>Task</c> per activation.
    /// <para>
    /// <b>What it does and does not prove.</b> It proves no <c>Task</c> is
    /// allocated. It does NOT prove the method is non-<c>async</c>:
    /// <c>AsyncTaskMethodBuilder</c> hands back the very same cached singleton when
    /// an <c>async Task</c> method completes synchronously, and it boxes no state
    /// machine in that case either - so both shapes are allocation-free and the
    /// comparison cannot tell them apart. That is why the vacuousness guard below
    /// forces the repairing activation to genuinely SUSPEND (via the fake's
    /// rendezvous-gated write) before asserting it returns a different task.
    /// Without that suspension the guard would pass against an implementation that
    /// does nothing at all, and every assertion here would be vacuous.
    /// </para>
    /// <para>
    /// Both no-repair shapes are pinned: a healthy internal-rooted shard (exits on
    /// the first branch without even resolving the leaf-type probe) and a healthy
    /// leaf-rooted shard (exits after the typed check). Between them they are the
    /// whole steady-state fleet.
    /// </para>
    /// </summary>
    [Test]
    public async Task Activation_allocates_no_task_when_there_is_nothing_to_repair()
    {
        var internalRooted = CreateHarness();
        internalRooted.State.State.RootNodeId = InternalRootId;
        internalRooted.State.State.RootIsLeaf = false;

        var leafRooted = CreateHarness();
        leafRooted.State.State.RootNodeId = LeafRootId;
        leafRooted.State.State.RootIsLeaf = true;

        var noRoot = CreateHarness();
        noRoot.State.State.RootNodeId = null;
        noRoot.State.State.RootIsLeaf = true;

        Assert.Multiple(() =>
        {
            Assert.That(
                ReferenceEquals(((IGrainBase)internalRooted.Grain).OnActivateAsync(CancellationToken.None), Task.CompletedTask),
                Is.True,
                "A healthy internal-rooted shard must return the cached completed task, not a fresh one.");
            Assert.That(
                ReferenceEquals(((IGrainBase)leafRooted.Grain).OnActivateAsync(CancellationToken.None), Task.CompletedTask),
                Is.True,
                "A healthy leaf-rooted shard must return the cached completed task, not a fresh one.");
            Assert.That(
                ReferenceEquals(((IGrainBase)noRoot.Grain).OnActivateAsync(CancellationToken.None), Task.CompletedTask),
                Is.True,
                "A shard with no root yet must return the cached completed task, not a fresh one.");
        });

        // Vacuousness guard. A repairing activation whose storage write genuinely
        // suspends must return a DIFFERENT task, otherwise the three identity
        // assertions above would hold for any implementation - including one that
        // returns Task.CompletedTask unconditionally and repairs nothing.
        var baked = CreateBakedHarness();
        baked.State.SimulateEtagChecks = true;

        var repairing = ((IGrainBase)baked.Grain).OnActivateAsync(CancellationToken.None);

        Assert.That(ReferenceEquals(repairing, Task.CompletedTask), Is.False,
            "A repairing activation that suspends on its storage write must not return the cached completed task; if "
            + "it does, the identity comparison is a constant and pins nothing.");

        await repairing;

        Assert.Multiple(() =>
        {
            Assert.That(baked.State.State.RootIsLeaf, Is.False,
                "Vacuousness guard: the repairing activation must actually have repaired something.");
            Assert.That(baked.State.WriteCount, Is.EqualTo(1));
        });
    }
}
