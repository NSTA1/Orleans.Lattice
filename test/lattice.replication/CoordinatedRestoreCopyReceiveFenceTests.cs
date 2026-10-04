using System.Reflection;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4593 end to end over the real restore engine, the real
/// <see cref="RestoreParticipant"/>, the real durable write-fence grain and the
/// real tree: a peer write that passed the cached receive gate before the
/// coordinated restore paused receiving must never land on the restored copy.
/// <para>
/// The receive gate is consulted once, before the apply, and fronted by a short
/// per-silo cache, so an entry can pass it before the pause and reach the tree's
/// apply seam after the alias swap. Calling the apply seam directly after the
/// commit is exactly that interleaving with the gate check already behind it.
/// The restored copy is born closed (the fence closes it before the swap) and the
/// seam re-checks the copy on every routing resolution, so the write is refused
/// and the applier defers it until the fence lifts.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoordinatedRestoreCopyReceiveFenceTests
{
    private const string Origin = "site-peer";

    private static readonly string[] CutKeys = ["cut/1", "cut/2", "cut/3"];

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new CoordinatedRestoreClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_peer_write_that_passed_the_receive_gate_before_the_pause_never_lands_on_the_restored_copy()
    {
        const string tree = "fence-detector";
        var (participant, request) = await PrepareRestoreAsync(tree, "saga-detector");
        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);

        // A peer write applied before the restore lands on the pre-restore copy and
        // leaves this tree's routing activation resolved onto that copy.
        await seam.ApplySetAsync("peer/warm", Bytes("warm"), Hlc(1), Origin, null, 0);

        await participant.CommitAsync(request);

        // The pre-cutover write arrives after the swap, its gate check behind it.
        var refused = Assert.ThrowsAsync<CopyReceiveFencedException>(
            () => seam.ApplySetAsync("peer/pre-cutover", Bytes("pre-cutover"), Hlc(2), Origin, null, 0));

        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await lattice.GetAsync("peer/pre-cutover"), Is.Null,
                "the restored copy must not hold a write from before the cutover");
            Assert.That(await lattice.GetAsync("peer/warm"), Is.Null, "the restore reverted to the cut");
            Assert.That(await lattice.CountAsync(), Is.EqualTo(CutKeys.Length), "the restored copy holds exactly the cut");
            Assert.That(refused!.TreeId, Is.EqualTo(tree));
        });
    }

    [Test]
    public async Task A_routing_activation_that_resolved_the_restored_copy_for_a_local_read_still_refuses_the_apply()
    {
        const string tree = "fence-warm-read";
        var (participant, request) = await PrepareRestoreAsync(tree, "saga-warm-read");
        await participant.CommitAsync(request);

        // Local reads and writes resume after the swap, so a routing activation
        // can cache the restored copy outside any apply; the apply that then
        // takes the cached fast path must still check the copy.
        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        Assert.That(await lattice.GetAsync(CutKeys[0]), Is.Not.Null);

        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);
        Assert.ThrowsAsync<CopyReceiveFencedException>(
            () => seam.ApplySetAsync("peer/warm-read", Bytes("warm-read"), Hlc(9), Origin, null, 0));
        Assert.That(await lattice.GetAsync("peer/warm-read"), Is.Null);
    }

    [Test]
    public async Task An_apply_admitted_before_the_restore_paused_receiving_is_refused_after_the_copy_opens()
    {
        const string tree = "fence-epoch-live";
        const string sagaId = "saga-epoch-live";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);

        // The entry is admitted (its gate observation is read) before the pause...
        var admittedUnder = await CurrentEpochAsync(tree);
        await participant.CommitAsync(request);
        _fixture.Completion.Complete = true;
        await _fixture.Fence(sagaId).PollResumeAsync();
        var copy = await PhysicalCopyAsync(tree);
        Assert.That((await CopyFence(copy).GetStatusAsync()).Closed, Is.False, "the lift opened the restored copy");

        // ...and only reaches the tree after the lift opened the restored copy.
        var refused = Assert.ThrowsAsync<CopyReceiveFencedException>(() => StampedAsync(tree, admittedUnder,
            () => seam.ApplySetAsync("peer/stale-admission", Bytes("stale"), Hlc(11), Origin, null, 0)));
        Assert.ThrowsAsync<CopyReceiveFencedException>(
            () => seam.ApplySetAsync("peer/unstamped", Bytes("unstamped"), Hlc(11), Origin, null, 0),
            "an apply with no admission stamp fails closed against a restored copy");

        // An apply admitted after the pause lands: the restored copy receives again.
        await StampedAsync(tree, await CurrentEpochAsync(tree),
            () => seam.ApplySetAsync("peer/fresh-admission", Bytes("fresh"), Hlc(12), Origin, null, 0));

        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(refused!.AdmittedBeforeRestore, Is.True);
            Assert.That(await lattice.GetAsync("peer/stale-admission"), Is.Null,
                "a write admitted before the restore's pause never lands on the restored copy");
            Assert.That(await lattice.GetAsync("peer/unstamped"), Is.Null);
            Assert.That(await lattice.GetAsync("peer/fresh-admission"), Is.Not.Null);
        });
    }

    [Test]
    public async Task The_applier_defers_an_entry_its_stale_gate_admitted_before_the_pause_after_the_lift()
    {
        const string tree = "fence-epoch-applier";
        const string sagaId = "saga-epoch-applier";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        var stale = new FrozenReceiveGate(await _fixture.GrainFactory.GetGrain<ITreeReceiveFenceGrain>(tree).ObserveAsync());
        await participant.CommitAsync(request);
        _fixture.Completion.Complete = true;
        await _fixture.Fence(sagaId).PollResumeAsync();

        var staleResult = await NewApplier(stale).ApplyAsync(Entry(tree, "peer/stale-gate", Hlc(13)));
        var freshResult = await NewApplier(
                new FrozenReceiveGate(await _fixture.GrainFactory.GetGrain<ITreeReceiveFenceGrain>(tree).ObserveAsync()))
            .ApplyAsync(Entry(tree, "peer/fresh-gate", Hlc(14)));

        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(staleResult.Deferred, Is.True, "a refused stale admission is deferred for the sender to re-ship");
            Assert.That(await lattice.GetAsync("peer/stale-gate"), Is.Null);
            Assert.That(freshResult.Applied, Is.True, "the re-ship, admitted afresh, lands");
            Assert.That(await lattice.GetAsync("peer/fresh-gate"), Is.Not.Null);
        });
    }

    [Test]
    public async Task A_causal_buffer_entry_parked_before_the_restore_is_discarded_and_never_lands()
    {
        const string tree = "fence-epoch-parked";
        const string sagaId = "saga-epoch-parked";
        const string dependency = "site-dependency";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        var buffer = _fixture.GrainFactory.GetGrain<ICausalApplyBufferGrain>(tree);

        // Parked before the pause, behind a dependency that has not arrived.
        var preCut = Entry(tree, "peer/parked-pre-cut", Hlc(15)) with { VectorClock = Vector(dependency, Hlc(100)) };
        Assert.That(await buffer.ParkAsync(preCut, await CurrentEpochAsync(tree)), Is.EqualTo(1));

        await participant.CommitAsync(request);
        _fixture.Completion.Complete = true;
        await _fixture.Fence(sagaId).PollResumeAsync();

        // Parked after the lift, behind the same dependency.
        var postCut = Entry(tree, "peer/parked-post-cut", Hlc(16)) with { VectorClock = Vector(dependency, Hlc(100)) };
        Assert.That(await buffer.ParkAsync(postCut, await CurrentEpochAsync(tree)), Is.EqualTo(2));

        // The dependency arrives; the drain runs over the open restored copy.
        await _fixture.GrainFactory.GetGrain<IReplicationHighWaterMarkGrain>(tree).TryAdvanceAsync(dependency, Hlc(100));
        var remaining = await buffer.DrainAsync();

        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(remaining, Is.Zero, "the pre-cutover entry is discarded, not left parked");
            Assert.That(await lattice.GetAsync("peer/parked-pre-cut"), Is.Null,
                "an entry parked before the restore never lands on the restored copy");
            Assert.That(await lattice.GetAsync("peer/parked-post-cut"), Is.Not.Null);
        });
    }

    [Test]
    public async Task A_park_while_the_receive_fence_is_paused_is_deferred_not_parked()
    {
        const string tree = "fence-park-paused";
        const string sagaId = "saga-park-paused";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        await participant.CommitAsync(request);

        // A stale gate admits the entry while the fence is in fact paused; its
        // dependency is unmet, so the applier would park it.
        var stale = new FrozenReceiveGate(new ReceiveFenceObservation { Paused = false, Epoch = await CurrentEpochAsync(tree) });
        var result = await NewApplier(stale).ApplyAsync(
            Entry(tree, "peer/park-paused", Hlc(17)) with { VectorClock = Vector("site-dependency", Hlc(100)) });

        Assert.Multiple(async () =>
        {
            Assert.That(result.Deferred, Is.True);
            Assert.That(await _fixture.GrainFactory.GetGrain<ICausalApplyBufferGrain>(tree).CountAsync(), Is.Zero);
        });
    }

    [Test]
    public async Task A_park_of_an_entry_admitted_before_the_restore_is_deferred_after_the_lift()
    {
        const string tree = "fence-park-stale";
        const string sagaId = "saga-park-stale";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        var admittedUnder = await _fixture.GrainFactory.GetGrain<ITreeReceiveFenceGrain>(tree).ObserveAsync();
        await participant.CommitAsync(request);
        _fixture.Completion.Complete = true;
        await _fixture.Fence(sagaId).PollResumeAsync();

        // Admitted before the pause, its dependency unmet, it reaches the park
        // only after the lift: the fence is unpaused again, but a pause has
        // superseded its admission.
        var result = await NewApplier(new FrozenReceiveGate(admittedUnder)).ApplyAsync(
            Entry(tree, "peer/park-stale", Hlc(18)) with { VectorClock = Vector("site-dependency", Hlc(100)) });

        Assert.Multiple(async () =>
        {
            Assert.That(result.Deferred, Is.True, "the sender re-ships it, and a fresh admission stamps it again");
            Assert.That(await _fixture.GrainFactory.GetGrain<ICausalApplyBufferGrain>(tree).CountAsync(), Is.Zero);
        });
    }

    [Test]
    public async Task A_batched_peer_write_after_the_swap_never_lands_on_the_restored_copy()
    {
        const string tree = "fence-batch";
        var (participant, request) = await PrepareRestoreAsync(tree, "saga-batch");
        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);

        await participant.CommitAsync(request);

        try
        {
            await seam.ApplyMergeManyAsync(
            [
                new ApplyMergeItem { Key = "peer/a", Value = Bytes("a"), SourceHlc = Hlc(3), OriginClusterId = Origin },
                new ApplyMergeItem { Key = "peer/b", Value = Bytes("b"), SourceHlc = Hlc(3), OriginClusterId = Origin },
            ]);
        }
        catch (CopyReceiveFencedException)
        {
        }

        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await lattice.GetAsync("peer/a"), Is.Null);
            Assert.That(await lattice.GetAsync("peer/b"), Is.Null);
            Assert.That(await lattice.CountAsync(), Is.EqualTo(CutKeys.Length));
        });
    }

    [Test]
    public async Task The_applier_defers_an_entry_its_stale_gate_admitted_after_the_swap()
    {
        const string tree = "fence-applier";
        var (participant, request) = await PrepareRestoreAsync(tree, "saga-applier");
        var applier = new ReplicationApplier(
            _fixture.SiloServices.GetRequiredService<IGrainFactory>(),
            _fixture.SiloServices.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            replicationContext: _fixture.SiloServices.GetService<ILatticeReplicationContext>(),
            receiveGate: new StaleOpenReceiveGate());

        await participant.CommitAsync(request);

        var result = await applier.ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "peer/applier",
            Value = Bytes("applier"),
            Timestamp = Hlc(4),
            OriginClusterId = Origin,
        });

        Assert.Multiple(async () =>
        {
            Assert.That(result.Deferred, Is.True, "the refused apply is deferred so the sender re-ships it");
            Assert.That(result.Applied, Is.False);
            Assert.That(await _fixture.GrainFactory.GetGrain<ILattice>(tree).GetAsync("peer/applier"), Is.Null);
        });
    }

    [Test]
    public async Task The_restored_copy_opens_when_the_saga_globally_completes()
    {
        const string tree = "fence-complete";
        const string sagaId = "saga-complete";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);
        await participant.CommitAsync(request);
        var copy = await PhysicalCopyAsync(tree);
        Assert.That((await CopyFence(copy).GetStatusAsync()).Closed, Is.True, "the restored copy is closed after the swap");

        _fixture.Completion.Complete = true;
        await _fixture.Fence(sagaId).PollResumeAsync();

        await StampedAsync(tree, await CurrentEpochAsync(tree),
            () => seam.ApplySetAsync("peer/after-lift", Bytes("after-lift"), Hlc(5), Origin, null, 0));
        Assert.Multiple(async () =>
        {
            Assert.That((await CopyFence(copy).GetStatusAsync()).Closed, Is.False);
            Assert.That(await _fixture.GrainFactory.GetGrain<ILattice>(tree).GetAsync("peer/after-lift"), Is.Not.Null,
                "after the lift the restored copy receives again");
        });
    }

    [Test]
    public async Task An_abort_after_the_commit_reverts_and_opens_the_restored_copy()
    {
        const string tree = "fence-abort-after-commit";
        var (participant, request) = await PrepareRestoreAsync(tree, "saga-abort-after-commit");
        await participant.CommitAsync(request);
        var shadow = await PhysicalCopyAsync(tree);

        await participant.AbortAsync(request);

        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);
        await seam.ApplySetAsync("peer/after-abort", Bytes("after-abort"), Hlc(6), Origin, null, 0);
        Assert.Multiple(async () =>
        {
            Assert.That((await CopyFence(shadow).GetStatusAsync()).Closed, Is.False, "the abort's lift opens the closed copy");
            Assert.That(await PhysicalCopyAsync(tree), Is.Not.EqualTo(shadow), "the alias reverted");
            Assert.That(await _fixture.GrainFactory.GetGrain<ILattice>(tree).GetAsync("peer/after-abort"), Is.Not.Null);
        });
    }

    [Test]
    public async Task An_abort_after_an_engage_whose_swap_never_ran_opens_the_closed_copy()
    {
        // Models a failed swap or a participant that crashed between the engage
        // and the swap: the copy is closed, the alias never moved, and the
        // coordinator then aborts.
        const string tree = "fence-abort-before-swap";
        const string sagaId = "saga-abort-before-swap";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        var built = await _fixture.Engine.BuildShadowAsync(
            new LatticeRestoreRequest(request.ManifestId, tree, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        await _fixture.Fence(sagaId).EngageAsync(new SagaWriteFenceRequest
        {
            SagaId = sagaId,
            Trees = [tree],
            CoordinatorClusterId = CoordinatedRestoreClusterFixture.ClusterId,
            ReceiveClosedCopies = new() { [built.ShadowPhysicalTreeId!] = tree },
        });
        Assert.That((await CopyFence(built.ShadowPhysicalTreeId!).GetStatusAsync()).Closed, Is.True);

        await participant.AbortAsync(request);

        Assert.That((await CopyFence(built.ShadowPhysicalTreeId!).GetStatusAsync()).Closed, Is.False);
    }

    [Test]
    public async Task A_re_driven_commit_keeps_the_restored_copy_closed_until_the_lift()
    {
        const string tree = "fence-redrive";
        const string sagaId = "saga-redrive";
        var (participant, request) = await PrepareRestoreAsync(tree, sagaId);
        await participant.CommitAsync(request);
        var copy = await PhysicalCopyAsync(tree);

        // A coordinator that lost the commit acknowledgement re-drives it, on a
        // fresh participant that lost its prepared-shadow cache.
        await NewParticipant().CommitAsync(request);

        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);
        Assert.ThrowsAsync<CopyReceiveFencedException>(
            () => seam.ApplySetAsync("peer/redrive", Bytes("redrive"), Hlc(7), Origin, null, 0));

        await _fixture.Fence(sagaId).LiftAsync();
        Assert.That((await CopyFence(copy).GetStatusAsync()).Closed, Is.False);
    }

    [Test]
    public async Task A_snapshot_bootstrap_entry_for_a_closed_copy_is_refused_at_the_seam()
    {
        // The bootstrap drain applies every snapshot entry through the canonical
        // applier under the bootstrap scope, so the seam's closed-copy check
        // refuses it exactly as it refuses a live entry.
        const string tree = "fence-bootstrap";
        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await lattice.SetAsync("seed", Bytes("seed"));
        var copy = await PhysicalCopyAsync(tree);
        await CopyFence(copy).CloseAsync("saga-bootstrap", 0);
        var applier = new ReplicationApplier(
            _fixture.SiloServices.GetRequiredService<IGrainFactory>(),
            _fixture.SiloServices.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            replicationContext: _fixture.SiloServices.GetService<ILatticeReplicationContext>());

        ApplyResult result;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            result = await applier.ApplyAsync(new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.Set,
                Key = "peer/bootstrap",
                Value = Bytes("bootstrap"),
                Timestamp = Hlc(10),
                OriginClusterId = Origin,
            });
        }

        Assert.Multiple(async () =>
        {
            Assert.That(result.Deferred, Is.True);
            Assert.That(result.Applied, Is.False);
            Assert.That(await lattice.GetAsync("peer/bootstrap"), Is.Null);
        });

        await CopyFence(copy).OpenAsync("saga-bootstrap");
    }

    /// <summary>
    /// Every write method on <see cref="IReplicationApplyGrain"/> must refuse an
    /// apply to a closed copy, so a newly added replicated write path cannot
    /// bypass the fence. The reflection walk fails when a method has no case here.
    /// </summary>
    [Test]
    public async Task Every_replicated_write_path_refuses_a_closed_copy()
    {
        const string tree = "fence-every-path";
        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await lattice.SetAsync("seed", Bytes("seed"));
        var copy = await PhysicalCopyAsync(tree);
        await CopyFence(copy).CloseAsync("saga-every-path", 0);
        var seam = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(tree);
        var tx = Guid.NewGuid();

        var cases = new Dictionary<string, Func<Task>>(StringComparer.Ordinal)
        {
            [nameof(IReplicationApplyGrain.ApplySetAsync)] = () =>
                seam.ApplySetAsync("k", Bytes("v"), Hlc(8), Origin, null, 0),
            [nameof(IReplicationApplyGrain.ApplyDeleteAsync)] = () =>
                seam.ApplyDeleteAsync("seed", Hlc(8), Origin, null),
            [nameof(IReplicationApplyGrain.ApplyDeleteRangeAsync)] = () =>
                seam.ApplyDeleteRangeAsync("a", "z", Hlc(8), Origin, null),
            [nameof(IReplicationApplyGrain.ApplyMergeManyAsync)] = () =>
                seam.ApplyMergeManyAsync(
                [
                    new ApplyMergeItem { Key = "k1", Value = Bytes("v"), SourceHlc = Hlc(8), OriginClusterId = Origin },
                    new ApplyMergeItem { Key = "k2", Value = Bytes("v"), SourceHlc = Hlc(8), OriginClusterId = Origin },
                ]),
            [nameof(IReplicationApplyGrain.ApplyCrdtDeltaManyAsync)] = () =>
                seam.ApplyCrdtDeltaManyAsync(
                [
                    new ApplyCrdtDeltaItem { Key = "c", Mode = LatticeMergeMode.GCounter, Delta = Bytes("d"), SourceHlc = Hlc(8), OriginClusterId = Origin },
                ]),
            [nameof(IReplicationApplyGrain.ApplyCrdtDeltaWithExpiryAsync)] = () =>
                seam.ApplyCrdtDeltaWithExpiryAsync("c", LatticeMergeMode.GCounter, Bytes("d"), 0),
            [nameof(IReplicationApplyGrain.ApplyPreparedSetAsync)] = () =>
                seam.ApplyPreparedSetAsync("p", Bytes("v"), Hlc(8), Origin, null, 0, tx, 1, 0),
            [nameof(IReplicationApplyGrain.ApplyPreparedDeleteAsync)] = () =>
                seam.ApplyPreparedDeleteAsync("p", Hlc(8), Origin, null, tx, 1, 0),
            [nameof(IReplicationApplyGrain.ApplyTxTerminalAsync)] = () =>
                seam.ApplyTxTerminalAsync(tx, committed: true, shardIndex: 0, Hlc(8), Origin),
            [nameof(IReplicationApplyGrain.FinalizeCrossTreeTerminalAsync)] = () =>
                seam.FinalizeCrossTreeTerminalAsync(tx, committed: true, [0], Hlc(8), Origin),
        };

        // A read is not a write path: the applier reads the stored value and
        // writes the fold back through one of the methods above.
        var exempt = new HashSet<string>(StringComparer.Ordinal) { nameof(IReplicationApplyGrain.ReadStoredWithVersionAsync) };

        var declared = typeof(IReplicationApplyGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Select(static m => m.Name)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        Assert.That(declared.Where(name => !cases.ContainsKey(name) && !exempt.Contains(name)), Is.Empty,
            "a replicated write path has no closed-copy case: add one so it cannot bypass the fence");
        Assert.That(cases.Count, Is.GreaterThanOrEqualTo(10));

        foreach (var (name, invoke) in cases)
        {
            Assert.ThrowsAsync<CopyReceiveFencedException>(() => invoke(), name);
        }

        Assert.That(await lattice.GetAsync("seed"), Is.Not.Null, "nothing was applied to the closed copy");
        Assert.That(await lattice.GetAsync("k"), Is.Null);

        await CopyFence(copy).OpenAsync("saga-every-path");
    }

    private async Task<(RestoreParticipant Participant, SagaControlRequest Request)> PrepareRestoreAsync(string tree, string sagaId)
    {
        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        foreach (var key in CutKeys)
        {
            await lattice.SetAsync(key, Bytes(key));
        }

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cut", BackupScopeSelector.WholeTree(tree)));
        await lattice.SetAsync("local/after-cut", Bytes("after-cut"));

        var participant = NewParticipant();
        var request = new SagaControlRequest
        {
            SagaId = sagaId,
            TargetTree = tree,
            ManifestId = backup.BackupId,
            CoordinatorClusterId = CoordinatedRestoreClusterFixture.ClusterId,
        };
        var vote = await participant.PrepareAsync(request);
        Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit), "the restore prepared");
        return (participant, request);
    }

    private RestoreParticipant NewParticipant() =>
        new(
            _fixture.SiloServices.GetRequiredService<ILatticeCoordinatedRestoreEngine>(),
            _fixture.SiloServices.GetRequiredService<ILatticeBackupRestoreService>(),
            _fixture.SiloServices.GetRequiredService<IRestoreCapacityProbe>(),
            _fixture.SiloServices.GetRequiredService<IGrainFactory>(),
            NullLogger<RestoreParticipant>.Instance);

    private async Task<string> PhysicalCopyAsync(string tree)
    {
        var entry = await _fixture.GrainFactory.GetLatticeRegistry().GetEntryAsync(tree);
        return entry?.PhysicalTreeId ?? tree;
    }

    private ICopyReceiveFenceGrain CopyFence(string physicalTreeId) =>
        _fixture.GrainFactory.GetGrain<ICopyReceiveFenceGrain>(physicalTreeId);

    private async Task<long> CurrentEpochAsync(string tree) =>
        (await _fixture.GrainFactory.GetGrain<ITreeReceiveFenceGrain>(tree).ObserveAsync()).Epoch;

    /// <summary>Runs <paramref name="apply"/> stamped as admitted by <paramref name="tree"/>'s fence under <paramref name="epoch"/>.</summary>
    private static async Task StampedAsync(string tree, long epoch, Func<Task> apply)
    {
        ReplicationAdmissionEpoch.Stamp(tree, epoch);
        await apply();
    }

    private ReplicationApplier NewApplier(IReplicationReceiveGate gate) =>
        new(
            _fixture.SiloServices.GetRequiredService<IGrainFactory>(),
            _fixture.SiloServices.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            replicationContext: _fixture.SiloServices.GetService<ILatticeReplicationContext>(),
            receiveGate: gate);

    private static WalRecord Entry(string tree, string key, HybridLogicalClock timestamp) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = Bytes(key),
        Timestamp = timestamp,
        OriginClusterId = Origin,
    };

    private static VersionVector Vector(string origin, HybridLogicalClock clock)
    {
        var vector = new VersionVector();
        vector.Entries[origin] = clock;
        return vector;
    }

    /// <summary>A receive gate that always answers with one fixed observation, as a stale cache does.</summary>
    private sealed class FrozenReceiveGate(ReceiveFenceObservation observation) : IReplicationReceiveGate
    {
        public ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default) =>
            new(observation.Paused);

        public ValueTask<ReceiveFenceObservation> ObserveAsync(string treeId, CancellationToken cancellationToken = default) =>
            new(observation);
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static HybridLogicalClock Hlc(int counter) =>
        new() { WallClockTicks = DateTime.UtcNow.Ticks, Counter = counter };

    /// <summary>
    /// A receive gate whose cached answer predates the pause: it reports the
    /// tree as receiving, as the real per-silo cache does for up to its refresh
    /// interval after the fence engaged.
    /// </summary>
    private sealed class StaleOpenReceiveGate : IReplicationReceiveGate
    {
        public ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default) =>
            new(false);
    }
}
