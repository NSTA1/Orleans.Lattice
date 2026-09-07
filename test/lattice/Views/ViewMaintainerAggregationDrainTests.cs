using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for the aggregation (grouped-reduce) drain path of the view
/// maintainer - <c>DrainAggregationAsync</c>, reached through
/// <see cref="IViewMaintainerGrain.DrainAsync"/> when the registration carries an
/// <see cref="ILatticeAggregationProjection"/>.
/// <para>
/// The fold itself is covered by the applier and projection fixtures. What is
/// exercised here is the drain's three <b>escalation to rebuild</b> arms, which
/// the applier tests cannot reach because they are properties of the drain loop
/// rather than of the fold: a source WAL that trimmed past the checkpoint
/// (<c>FellOffLog</c>), an atomic-staging buffer that blew its bound, and an
/// unconstrained range delete the projection cannot lower to exact retractions.
/// Each must abandon the pass without advancing the checkpoint or reporting a
/// cursor, because the rebuild - not the partial drain - is what re-establishes
/// both.
/// </para>
/// <para>
/// The grain is driven directly (the construction pattern established by
/// <see cref="ViewMaintainerSourceIdentityTests"/> and reused by
/// <see cref="ViewMaintainerDecommissionTests"/>) with a stub subscriber standing
/// in for the WAL, so each escalation is triggered deterministically without a
/// cluster.
/// </para>
/// </summary>
[TestFixture]
public class ViewMaintainerAggregationDrainTests
{
    private const string ViewName = "orders-agg";
    private const string SourceTreeId = "orders";

    /// <summary>
    /// Hand-written catalog rather than an NSubstitute lambda so a test can
    /// re-arm it mid-drain. Retiring the registration at the moment the drain
    /// escalates makes the ensuing <c>RebuildAsync</c> return at its own
    /// registration lookup, which keeps these tests scoped to the escalation
    /// decision instead of dragging the whole shadow-build path in behind it.
    /// The lookup count is what proves the rebuild was actually entered.
    /// </summary>
    private sealed class ArmableViewCatalog : IViewCatalog
    {
        public ViewRegistration? Current { get; set; }

        public int TryGetCount { get; private set; }

        public ViewRegistration? TryGet(string viewName)
        {
            TryGetCount++;
            return Current;
        }

        public void Register(ViewRegistration registration) => Current = registration;

        public void Remove(string viewName) => Current = null;

        public IReadOnlyCollection<ViewRegistration> All() =>
            Current is null ? [] : new[] { Current };
    }

    private sealed class StubAggregationProjection : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "agg-v1";

        public AggregationKind Aggregation => AggregationKind.Count;

        public required Func<LatticeMutation, IEnumerable<AggregationContribution>> Projector { get; init; }

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) => Projector(mutation);
    }

    private sealed record Harness(
        ViewMaintainerGrain Grain,
        ArmableViewCatalog Catalog,
        FakePersistentState<ViewCheckpointState> State,
        IWalSubscriber Subscriber,
        IWalCursorRegistry CursorRegistry);

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static LatticeMutation UserSet(string key, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
    };

    private static LatticeMutation PreparedSet(string key, Guid txId, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
        IsPrepared = true,
        TransactionId = txId,
    };

    /// <summary>
    /// A one-member atomic batch's prepared entry. <c>AtomicBatchSize</c> and
    /// <c>AtomicBatchIndex</c> are what make the staged batch reach prepare
    /// completeness so its terminal can flush it.
    /// </summary>
    private static LatticeMutation PreparedBatchMember(string key, Guid txId, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
        IsPrepared = true,
        TransactionId = txId,
        AtomicBatchSize = 1,
        AtomicBatchIndex = 0,
    };

    /// <summary>
    /// The batch's commit terminal. A non-null <c>CrossTreeOperationId</c> is what
    /// flags the staged batch for the joint cross-tree flip rather than an
    /// immediate single-tree one.
    /// </summary>
    private static LatticeMutation CommitTerminal(
        Guid txId,
        long ticks,
        string? crossTreeOperationId = null,
        IReadOnlyList<string>? participants = null) => new()
        {
            TreeId = SourceTreeId,
            Kind = MutationKind.TxCommit,
            Key = string.Empty,
            Timestamp = Clock(ticks),
            Category = MutationCategory.User,
            TransactionId = txId,
            CrossTreeOperationId = crossTreeOperationId,
            CrossTreeParticipants = participants,
        };

    private static Harness Create(
        Func<LatticeMutation, IEnumerable<AggregationContribution>>? projector = null,
        LatticeViewOptions? options = null,
        TimeSpan? sourceWalRetention = null)
    {
        var catalog = new ArmableViewCatalog();
        catalog.Register(new ViewRegistration(
            ViewName,
            SourceTreeId,
            Projection: null,
            AggregationProjection: new StubAggregationProjection
            {
                Projector = projector ?? (_ => []),
            }));

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Arg.Any<string>()).Returns(SourceTreeId);
        // Stubbed here rather than by TestOptionsResolver.Create: that helper
        // re-stubs GetGrain<ILatticeRegistry> with its OWN registry substitute,
        // which would silently discard the ResolveAsync above and leave the
        // maintainer resolving a null physical id - so every drain would treat the
        // source as re-aliased, heal, and return before the aggregation path.
        // ForFactory reuses this factory untouched.
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // Cached per tree id, and GetAsync must explicitly return null: NSubstitute
        // auto-returns an EMPTY ARRAY for a byte[]-returning member, which the
        // aggregation row codec then tries to decode and throws on. Null is what a
        // genuinely absent accumulator row looks like.
        var trees = new Dictionary<string, ILattice>(StringComparer.Ordinal);
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            var treeId = call.ArgAt<string>(0);
            if (!trees.TryGetValue(treeId, out var tree))
            {
                tree = Substitute.For<ILattice>();
                tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                    .Returns(Task.FromResult<byte[]?>(null));
                trees[treeId] = tree;
            }
            return tree;
        });

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));

        var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        viewOptions.Get(Arg.Any<string>()).Returns(options ?? new LatticeViewOptions());

        var latticeOptions = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        latticeOptions.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalRetention = sourceWalRetention ?? TimeSpan.Zero,
        });

        var replicationContext = Substitute.For<ILatticeReplicationContext>();
        replicationContext.IsReplicationEnabled.Returns(false);

        var subscriber = Substitute.For<IWalSubscriber>();
        var cursorRegistry = Substitute.For<IWalCursorRegistry>();
        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.BoundPhysicalTreeId = SourceTreeId;

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            reminderRegistry: null!,
            NullLogger<ViewMaintainerGrain>.Instance,
            catalog,
            commitLogReader: Substitute.For<ICommitLogReader>(),
            subscriber: subscriber,
            cursorRegistry: cursorRegistry,
            optionsResolver: TestOptionsResolver.ForFactory(factory, new LatticeOptions()),
            viewOptions,
            latticeOptions,
            replicationContext,
            saturationSignal: null,
            historyRowCodec: null!,
            state);

        return new Harness(grain, catalog, state, subscriber, cursorRegistry);
    }

    /// <summary>
    /// Arms the stub subscriber: surfaces <paramref name="entries"/> to the
    /// maintainer's handler, then returns <paramref name="result"/>.
    /// </summary>
    private static void ArmDrain(
        Harness h,
        WalDrainResult result,
        IEnumerable<LatticeMutation>? entries = null)
    {
        h.Subscriber
            .DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var handler = call.ArgAt<IWalSubscriptionHandler>(1);
                var offset = 0L;
                foreach (var mutation in entries ?? [])
                {
                    handler.OnEntry(new WalSubscriptionEntry(0, offset++, mutation));
                }

                return Task.FromResult(result);
            });
    }

    /// <summary>
    /// Asserts the pass abandoned the drain and reconverged through a rebuild. A
    /// DeriveLocally rebuild builds a fresh generation and swaps it in, so the
    /// active generation advancing (and the retired one being queued for reclaim)
    /// is the durable evidence that the escalation actually happened - rather than
    /// the drain merely returning zero because it found nothing to do.
    /// </summary>
    private static void AssertEscalatedToRebuild(Harness h)
    {
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1),
                "the escalation must swap in a freshly built generation");
            Assert.That(h.State.State.HasPendingReclaim, Is.True,
                "the retired generation must be queued for reclaim by the swap");
        });
    }

    /// <summary>
    /// Asserts the pass drained normally: no rebuild ran, so the view is still
    /// serving the generation it started on.
    /// </summary>
    private static void AssertNoRebuild(Harness h) =>
        Assert.That(h.State.State.ActiveGeneration, Is.Zero,
            "a clean pass must not escalate into a rebuild");

    // ------------------------------------------------------------ fell off log

    [Test]
    public async Task Aggregation_drain_rebuilds_when_the_source_WAL_trimmed_past_the_checkpoint()
    {
        var h = Create();
        ArmDrain(h, new WalDrainResult { FellOffLog = true });

        var applied = await h.Grain.DrainAsync();

        Assert.That(applied, Is.Zero, "a fall-off-log pass folds nothing");
        AssertEscalatedToRebuild(h);
    }

    // ------------------------------------------------------- range reconcile

    [Test]
    public async Task Aggregation_drain_rebuilds_when_an_unconstrained_range_delete_is_observed()
    {
        // A RangeReconcile cannot be lowered to exact per-key retractions, so the
        // affected range is reconciled by a full rebuild instead of being folded.
        var h = Create(projector: mutation =>
        [
            new AggregationContribution
            {
                Kind = AggregationContributionKind.RangeReconcile,
                GroupKey = mutation.Key,
                SourceKey = mutation.Key,
                Timestamp = mutation.Timestamp,
            },
        ]);
        ArmDrain(h, new WalDrainResult { EntriesRead = 1 }, entries: [UserSet("a", 5)]);

        var applied = await h.Grain.DrainAsync();

        Assert.That(applied, Is.Zero, "the pass escalates instead of folding contributions");
        AssertEscalatedToRebuild(h);
    }

    [Test]
    public async Task Aggregation_drain_folds_ordinary_contributions_without_escalating()
    {
        // The negative control for the test above: an ordinary Contribute must
        // fold and advance the checkpoint rather than trip the rebuild arm.
        var h = Create(projector: mutation =>
        [
            new AggregationContribution
            {
                Kind = AggregationContributionKind.Contribute,
                GroupKey = "g",
                SourceKey = mutation.Key,
                Numeric = 1,
                Timestamp = mutation.Timestamp,
            },
        ]);
        ArmDrain(
            h,
            new WalDrainResult
            {
                EntriesRead = 1,
                HighestTimestamp = Clock(5),
                AdvancedOffsets = new Dictionary<int, long> { [0] = 7 },
            },
            entries: [UserSet("a", 5)]);

        var applied = await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.EqualTo(1), "the single contribution must be folded");
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(7),
                "a clean pass advances the durable resume offset");
        });
        AssertNoRebuild(h);
    }

    // ------------------------------------------------------- staging backstop

    [Test]
    public async Task Aggregation_drain_rebuilds_when_the_atomic_staging_buffer_exceeds_its_bound()
    {
        // Two un-terminated atomic batches against a one-transaction cap: the
        // buffer cannot hold them, so the pass abandons the staged prepares and
        // reconverges from committed source state.
        var h = Create(
            projector: _ => [],
            options: new LatticeViewOptions { MaxStagedTransactions = 1 });
        ArmDrain(
            h,
            new WalDrainResult { EntriesRead = 2 },
            entries:
            [
                PreparedSet("a", Guid.NewGuid(), 5),
                PreparedSet("b", Guid.NewGuid(), 6),
            ]);

        var applied = await h.Grain.DrainAsync();

        Assert.That(applied, Is.Zero, "a tripped backstop folds nothing");
        AssertEscalatedToRebuild(h);
    }

    [Test]
    public async Task Aggregation_drain_stages_an_un_terminated_batch_without_tripping_the_backstop()
    {
        // The negative control: one un-terminated batch is inside the bound, so it
        // stays staged for a later pass rather than forcing a rebuild.
        var h = Create(
            projector: _ => [],
            options: new LatticeViewOptions { MaxStagedTransactions = 8 });
        ArmDrain(
            h,
            new WalDrainResult { EntriesRead = 1 },
            entries: [PreparedSet("a", Guid.NewGuid(), 5)]);

        await h.Grain.DrainAsync();

        AssertNoRebuild(h);
    }

    // ------------------------------------------------- cross-tree batch flip

    [Test]
    public async Task Aggregation_drain_flips_a_completed_cross_tree_batch_and_evicts_it_from_staging()
    {
        // A cross-tree atomic batch stamps a coordinator key on its terminal, so
        // the completed batch takes the joint-flip path rather than an immediate
        // single-tree flip. With only this view present in the wait set the joint
        // flip degenerates to a local one, which must still resolve the batch:
        // fold its captured slice, release the staged keys, and evict it from the
        // staging buffer so a later drain does not re-flush it.
        var h = Create(projector: mutation =>
        [
            new AggregationContribution
            {
                Kind = AggregationContributionKind.Contribute,
                GroupKey = "g",
                SourceKey = mutation.Key,
                Numeric = 1,
                Timestamp = mutation.Timestamp,
            },
        ]);
        var txId = Guid.NewGuid();
        ArmDrain(
            h,
            new WalDrainResult { EntriesRead = 2, HighestTimestamp = Clock(6) },
            entries:
            [
                PreparedBatchMember("a", txId, 5),
                CommitTerminal(txId, 6, crossTreeOperationId: "xt-op-1"),
            ]);

        var applied = await h.Grain.DrainAsync();

        Assert.That(applied, Is.GreaterThan(0),
            "the resolved cross-tree batch must contribute its captured row writes to the applied count");
        AssertNoRebuild(h);

        // Re-draining with nothing new must not re-apply the batch: a resolved
        // batch is evicted from staging, not left to flush again.
        ArmDrain(h, new WalDrainResult());
        Assert.That(await h.Grain.DrainAsync(), Is.Zero,
            "an evicted batch must not be flushed a second time");
    }
}
