using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for #5294: the replication full-state CRDT fold (the bootstrap
/// committed-projection row, which carries the whole CRDT state and no typed
/// delta) must read and write on the apply seam under the replication-apply scope,
/// so the coordinated-restore copy receive fence (#4593) and the bootstrap-hold
/// routing (#4791) apply to it exactly as to every other replicated write.
/// <para>
/// The fold used to read through the unscoped stored-form read and write through
/// the unscoped version-conditioned set, so it never consulted the copy receive
/// fence and never followed a bootstrap hold onto its held copy: a row admitted on
/// a stale "open" receive-gate answer landed on a restored copy a coordinated
/// restore still held closed, or on one whose admission floor post-dated the row,
/// and a dead-letter replay during a hold folded against, and wrote to, the
/// original tree instead of the held copy.
/// </para>
/// <para>
/// Each case drives a full-state PN-counter row and a full-state OR-Map row through
/// the real canonical applier and the real grains. The closed and floor cases close
/// the tree's own copy fence directly, as the snapshot-bootstrap case in
/// <see cref="CoordinatedRestoreCopyReceiveFenceTests"/> does: the seam checks the
/// copy every routing resolution reaches, whatever made it a restored copy.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class FullStateCopyReceiveFenceIntegrationTests
{
    private const string Origin = "fsf-origin";
    private const string ImportOrigin = "fsf-import";
    private const string Key = "row";
    private const string PnPrefix = "fsf-pn-";
    private const string OrMapPrefix = "fsf-ormap-";
    private const string PnClosedTree = PnPrefix + "closed";
    private const string PnFloorTree = PnPrefix + "floor";
    private const string PnHoldTree = PnPrefix + "hold";
    private const string OrMapClosedTree = OrMapPrefix + "closed";
    private const string OrMapFloorTree = OrMapPrefix + "floor";
    private const string OrMapHoldTree = OrMapPrefix + "hold";

    private TestCluster _cluster = null!;
    private IServiceProvider _services = null!;
    private IGrainFactory _grains = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _grains = _services.GetRequiredService<IGrainFactory>();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public Task Full_state_pn_counter_row_routed_to_a_closed_restored_copy_is_deferred_not_applied() =>
        AssertClosedCopyDefersAsync(PnClosedTree, PnRow(PnClosedTree, "site-b", 3, ticks: 1_000));

    [Test]
    public Task Full_state_or_map_row_routed_to_a_closed_restored_copy_is_deferred_not_applied() =>
        AssertClosedCopyDefersAsync(OrMapClosedTree, OrMapRow(OrMapClosedTree, "orders-b", "site-b", 3, ticks: 1_000));

    [Test]
    public Task Full_state_pn_counter_row_admitted_below_the_copy_floor_is_deferred_not_applied() =>
        AssertFloorDefersAsync(PnFloorTree, PnRow(PnFloorTree, "site-b", 3, ticks: 1_000));

    [Test]
    public Task Full_state_or_map_row_admitted_below_the_copy_floor_is_deferred_not_applied() =>
        AssertFloorDefersAsync(OrMapFloorTree, OrMapRow(OrMapFloorTree, "orders-b", "site-b", 3, ticks: 1_000));

    [Test]
    public async Task Dead_letter_replayed_full_state_pn_counter_row_under_a_bootstrap_hold_lands_on_the_held_copy()
    {
        var merged = await ReplayUnderHoldAsync(
            PnHoldTree,
            imported: PnRow(PnHoldTree, "site-a", 5, ticks: 1_000) with { OriginClusterId = ImportOrigin },
            replayed: PnRow(PnHoldTree, "site-b", 3, ticks: 2_000));

        var counter = JsonLatticeSerializer<PnCounter>.Default.Deserialize(merged);
        Assert.Multiple(() =>
        {
            Assert.That(counter.Increments.GetValueOrDefault("site-a"), Is.EqualTo(5),
                "the replayed row folded against the held copy's imported state");
            Assert.That(counter.Increments.GetValueOrDefault("site-b"), Is.EqualTo(3), "the replayed row is applied");
        });
    }

    [Test]
    public async Task Dead_letter_replayed_full_state_or_map_row_under_a_bootstrap_hold_lands_on_the_held_copy()
    {
        var merged = await ReplayUnderHoldAsync(
            OrMapHoldTree,
            imported: OrMapRow(OrMapHoldTree, "orders-a", "site-a", 5, ticks: 1_000) with { OriginClusterId = ImportOrigin },
            replayed: OrMapRow(OrMapHoldTree, "orders-b", "site-b", 3, ticks: 2_000));

        var map = CrdtShape.ForOrMap<string, PnCounter>().DeserializeState(merged) as OrMap<string, PnCounter>;
        Assert.That(map, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(map!.ContainsKey("orders-a"), Is.True, "the replayed row folded against the held copy's imported state");
            Assert.That(map.ContainsKey("orders-b"), Is.True, "the replayed row is applied");
        });
    }

    private async Task AssertClosedCopyDefersAsync(string tree, WalRecord row)
    {
        const string saga = "fsf-closed-saga";
        var fence = _grains.GetGrain<ICopyReceiveFenceGrain>(tree);
        await fence.CloseAsync(saga, 0);

        // The real receive gate: the tree's own receive fence is not paused, so the
        // gate admits the row, exactly as a cached "open" answer does after a pause.
        var whileClosed = await CanonicalApplier().ApplyAsync(row);
        var storedWhileClosed = await StoredAsync(tree);

        await fence.OpenAsync(saga);
        var afterOpen = await CanonicalApplier().ApplyAsync(row);

        Assert.Multiple(async () =>
        {
            Assert.That(whileClosed.Deferred, Is.True, "a full-state row routed to a closed copy is deferred for the sender to re-ship");
            Assert.That(whileClosed.Applied, Is.False);
            Assert.That(storedWhileClosed, Is.Null, "a closed restored copy never receives the full-state fold's write");
            Assert.That(afterOpen.Applied, Is.True, "the re-ship lands once the fence opens the copy");
            Assert.That(await StoredAsync(tree), Is.EqualTo(row.Value));
        });
    }

    private async Task AssertFloorDefersAsync(string tree, WalRecord row)
    {
        const string saga = "fsf-floor-saga";
        const long floor = 5;
        var fence = _grains.GetGrain<ICopyReceiveFenceGrain>(tree);
        await fence.CloseAsync(saga, floor);
        await fence.OpenAsync(saga);
        Assert.That((await fence.GetStatusAsync()).Closed, Is.False, "precondition: the copy is open, with its floor kept");

        // Admitted by the tree's receive fence under an epoch older than the copy's
        // floor: a stale gate answer from before the restore's pause.
        var stale = await ApplierWithGate(floor - 1).ApplyAsync(row);
        var storedAfterStale = await StoredAsync(tree);
        var fresh = await ApplierWithGate(floor).ApplyAsync(row);

        Assert.Multiple(async () =>
        {
            Assert.That(stale.Deferred, Is.True, "a full-state row admitted before the restore's pause is deferred");
            Assert.That(stale.Applied, Is.False);
            Assert.That(storedAfterStale, Is.Null, "a row admitted below the copy's floor never lands on it");
            Assert.That(fresh.Applied, Is.True, "a re-ship admitted at the floor lands");
            Assert.That(await StoredAsync(tree), Is.EqualTo(row.Value));
        });
    }

    /// <summary>
    /// Opens a bootstrap hold on <paramref name="tree"/>, imports
    /// <paramref name="imported"/> onto the held copy as the bootstrap drain does,
    /// then replays <paramref name="replayed"/> from the replication dead-letter
    /// queue - a delivery outside the import - and asserts it landed on the held
    /// copy and not on the original. Returns the cut-over tree's stored row.
    /// </summary>
    private async Task<byte[]> ReplayUnderHoldAsync(string tree, WalRecord imported, WalRecord replayed)
    {
        // No committed row is seeded before the hold: the hold's copy pass would
        // migrate it onto the held copy, which is outside this defect.
        var lattice = _grains.GetGrain<ILattice>(tree);
        var resize = _grains.GetGrain<ITreeResizeGrain>(tree);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True, "precondition: the hold is active");
        var held = await resize.GetBootstrapCopyTreeIdAsync();
        Assert.That(held, Is.Not.Null.And.Not.EqualTo(tree));

        ApplyResult importResult;
        using (LatticeBootstrapShadowRouteContext.BeginScope(tree, held!))
        {
            importResult = await CanonicalApplier().ApplyAsync(imported);
        }

        Assert.That(importResult.Applied, Is.True, "precondition: the import landed on the held copy");

        var deadLetters = _grains.GetGrain<IReplicationDeadLetterGrain>(tree);
        var entryId = await deadLetters.EnqueueAsync(
            replayed, "parked for replay", retryCount: 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        var replay = await _services.GetRequiredService<ILatticeReplicationDeadLetters>().ReplayAsync(tree, entryId);

        Assert.That(replay, Is.Not.Null);
        Assert.That(replay!.Value.Applied, Is.True, "the replayed row applies");
        Assert.That(await lattice.GetAsync(Key), Is.Null,
            "during a bootstrap hold a replicated write lands on the held copy, never on the original tree readers use");

        await resize.CompleteBootstrapCopyAsync(operationId);
        var cutOver = await lattice.GetAsync(Key);
        Assert.That(cutOver, Is.Not.Null, "the held copy, published by the cutover, holds the replayed row");
        return cutOver!;
    }

    private Task<byte[]?> StoredAsync(string tree) => _grains.GetGrain<ILattice>(tree).GetAsync(Key);

    private ReplicationApplier CanonicalApplier() => _services.GetRequiredService<ReplicationApplier>();

    private ReplicationApplier ApplierWithGate(long epoch) =>
        new(
            _grains,
            _services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            crdtShapes: _services.GetRequiredService<CrdtShapeRegistry>(),
            replicationContext: _services.GetService<ILatticeReplicationContext>(),
            receiveGate: new FrozenReceiveGate(new ReceiveFenceObservation { Paused = false, Epoch = epoch }));

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static WalRecord PnRow(string tree, string site, long increment, long ticks)
    {
        var counter = new PnCounter();
        counter.Increment(site, increment);
        return FullStateRow(tree, LatticeMergeMode.PnCounter, JsonLatticeSerializer<PnCounter>.Default.Serialize(counter), ticks);
    }

    private static WalRecord OrMapRow(string tree, string mapKey, string site, long increment, long ticks)
    {
        var counter = new PnCounter();
        counter.Increment(site, increment);
        var map = new OrMap<string, PnCounter>();
        map.Set(mapKey, site, counter);
        return FullStateRow(tree, LatticeMergeMode.OrMap, CrdtShape.ForOrMap<string, PnCounter>().SerializeState(map), ticks);
    }

    private static WalRecord FullStateRow(string tree, LatticeMergeMode mode, byte[] state, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = Key,
        Mode = mode,
        Value = state,
        Delta = null,
        Timestamp = Hlc(ticks),
        OriginClusterId = Origin,
    };

    /// <summary>A receive gate that always answers with one fixed observation, as a stale cache does.</summary>
    private sealed class FrozenReceiveGate(ReceiveFenceObservation observation) : IReplicationReceiveGate
    {
        public ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default) =>
            new(observation.Paused);

        public ValueTask<ReceiveFenceObservation> ObserveAsync(string treeId, CancellationToken cancellationToken = default) =>
            new(observation);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "fsf-receiver";
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.AddOrMapShape<string, PnCounter>(OrMapClosedTree);
            siloBuilder.AddOrMapShape<string, PnCounter>(OrMapFloorTree);
            siloBuilder.AddOrMapShape<string, PnCounter>(OrMapHoldTree);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, ModeResolver>();
        }
    }

    private sealed class ModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(PnPrefix, StringComparison.Ordinal) ? LatticeMergeMode.PnCounter
            : treeId.StartsWith(OrMapPrefix, StringComparison.Ordinal) ? LatticeMergeMode.OrMap
            : LatticeMergeMode.LwwRegister;
    }
}
