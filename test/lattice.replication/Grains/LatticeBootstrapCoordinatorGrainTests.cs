using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the receiver-side snapshot/bootstrap state
/// machine in <see cref="LatticeBootstrapCoordinatorGrain"/>. Tests
/// exercise the kickoff persistence path
/// (<see cref="LatticeBootstrapCoordinatorGrain.TryInitiateBootstrapAsync"/>)
/// and the phase pump
/// (<see cref="LatticeBootstrapCoordinatorGrain.ProcessNextPhaseAsync"/>)
/// directly, mirroring the
/// <c>TreeResizeGrainTests</c> pattern: bypass <c>StartCoordinatorAsync</c>
/// (which requires a real grain scheduler for the phase timer) by
/// pre-populating <see cref="BootstrapCoordinatorState"/> and calling
/// the phase hook explicitly. The public façade
/// <see cref="LatticeBootstrapCoordinator"/> is covered separately
/// in <see cref="LatticeBootstrapCoordinatorTests"/>.
/// </summary>
[TestFixture]
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private const string Tree = "boot-tree";
    private const string SourceCluster = "site-a";
    private const string OtherSource = "site-b";

    private static (
        LatticeBootstrapCoordinatorGrain Grain,
        FakePersistentState<BootstrapCoordinatorState> State,
        IGrainFactory Factory,
        IBootstrapSnapshotSource Provider,
        IReminderRegistry Reminders,
        IReplicationApplier Apply,
        IReplicationHighWaterMarkGrain Hwm,
        ILatticeMergeModeResolver MergeResolver) Create(
            FakePersistentState<BootstrapCoordinatorState>? existingState = null,
            string treeName = Tree,
            ILatticeMergeModeResolver? mergeResolver = null,
            LatticeReplicationOptions? replicationOptions = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("bootstrap-coordinator", treeName));
        var factory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<IBootstrapSnapshotSource>();
        var reminders = Substitute.For<IReminderRegistry>();
        var apply = Substitute.For<IReplicationApplier>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        // Default apply seam returns a successful ApplyResult so the
        // drain loop advances; individual tests override this where
        // they need to throw or observe the call.
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new ApplyResult
            {
                Applied = true,
                HighWaterMark = ((WalRecord)call[0]).Timestamp,
            }));
        // Default merge-mode resolver returns null so DrainSnapshotAsync
        // falls back to LwwRegister - preserves the historical default
        // behaviour for tests that pre-date the per-tree merge-mode
        // plumbing.
        var resolver = mergeResolver ?? Substitute.For<ILatticeMergeModeResolver>();
        if (mergeResolver is null)
        {
            resolver.Resolve(Arg.Any<string>()).Returns((LatticeMergeMode?)null);
        }
        // Default replication options disable bootstrap transient
        // retries (MaxAttempts = 1) so legacy tests that throw a
        // non-classified exception still observe the immediate
        // Failed pivot rather than the bounded retry loop. Tests
        // covering the retry path supply their own configured
        // options instance.
        var options = replicationOptions ?? new LatticeReplicationOptions
        {
            ClusterId = "test-cluster",
            BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
            {
                MaxAttempts = 1,
                InitialDelay = TimeSpan.Zero,
                MaxDelay = TimeSpan.Zero,
            },
        };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        optionsMonitor.CurrentValue.Returns(options);
        var fakeState = existingState ?? new FakePersistentState<BootstrapCoordinatorState>();
        var grain = new LatticeBootstrapCoordinatorGrain(
            context, factory, provider, apply, reminders, resolver, optionsMonitor,
            NullLogger<LatticeBootstrapCoordinatorGrain>.Instance, fakeState);
        return (grain, fakeState, factory, provider, reminders, apply, hwm, resolver);
    }

    /// <summary>
    /// Predicate matcher for the bootstrap-drain shape of
    /// <see cref="IReplicationApplier.ApplyAsync(WalRecord, CancellationToken)"/>:
    /// a Set mutation stamped with the supplied key + HLC, the configured
    /// <see cref="SourceCluster"/> origin, null vector clock, no expiry,
    /// and the supplied merge mode (default <see cref="LatticeMergeMode.LwwRegister"/>
    /// matches the resolver's null-fallback path that
    /// <c>DrainSnapshotAsync</c> stamps for unconfigured trees).
    /// Keeps assertion sites short and ensures every applier-seam contract
    /// field is checked at every call site.
    /// </summary>
    private static bool IsBootstrapSet(
        WalRecord r,
        string key,
        HybridLogicalClock ts,
        string origin = SourceCluster,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister) =>
        r.TreeId == Tree
        && r.Op == MutationKind.Set
        && r.Key == key
        && r.Timestamp == ts
        && r.OriginClusterId == origin
        && r.VectorClock == null
        && r.ExpiresAtTicks == 0
        && r.IsTombstone == false
        && r.Mode == mode;

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static async IAsyncEnumerable<SnapshotEntry> Stream(params SnapshotEntry[] entries)
    {
        await Task.CompletedTask;
        foreach (var e in entries) yield return e;
    }

    private static SnapshotStream MakeStream(
        HybridLogicalClock asOf,
        VersionVector frontier,
        IAsyncEnumerable<SnapshotEntry>? entries = null,
        string treeName = Tree) =>
        new(treeName, asOf, frontier, entries ?? Stream());

    /// <summary>
    /// Pre-populates the state shape that
    /// <see cref="LatticeBootstrapCoordinatorGrain.TryInitiateBootstrapAsync"/>
    /// would otherwise produce, so phase-pump tests can drive
    /// <c>ProcessNextPhaseAsync</c> directly without going through
    /// the kickoff path's coordinator-start logic.
    /// </summary>
    private static void Seed(
        FakePersistentState<BootstrapCoordinatorState> state,
        LatticeBootstrapState phase = LatticeBootstrapState.RequestingSnapshot,
        string sourceClusterId = SourceCluster,
        HybridLogicalClock? lastAppliedHlc = null)
    {
        state.State.InProgress = true;
        state.State.Phase = phase;
        state.State.SourceClusterId = sourceClusterId;
        state.State.OperationId = "test-op";
        state.State.LastAppliedHlc = lastAppliedHlc ?? HybridLogicalClock.Zero;
    }

    // --- Constructor null guards ---

    private static IOptionsMonitor<LatticeReplicationOptions> StubOptions()
    {
        var stub = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = "test-cluster" };
        stub.Get(Arg.Any<string>()).Returns(options);
        stub.CurrentValue.Returns(options);
        return stub;
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        var context = Substitute.For<IGrainContext>();
        var provider = Substitute.For<IBootstrapSnapshotSource>();
        var applier = Substitute.For<IReplicationApplier>();
        var reminders = Substitute.For<IReminderRegistry>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var fakeState = new FakePersistentState<BootstrapCoordinatorState>();
        Assert.That(
            () => new LatticeBootstrapCoordinatorGrain(
                context, null!, provider, applier, reminders, resolver, StubOptions(),
                NullLogger<LatticeBootstrapCoordinatorGrain>.Instance, fakeState),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_snapshot_provider_is_null()
    {
        var context = Substitute.For<IGrainContext>();
        var factory = Substitute.For<IGrainFactory>();
        var applier = Substitute.For<IReplicationApplier>();
        var reminders = Substitute.For<IReminderRegistry>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var fakeState = new FakePersistentState<BootstrapCoordinatorState>();
        Assert.That(
            () => new LatticeBootstrapCoordinatorGrain(
                context, factory, null!, applier, reminders, resolver, StubOptions(),
                NullLogger<LatticeBootstrapCoordinatorGrain>.Instance, fakeState),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_replication_applier_is_null()
    {
        var context = Substitute.For<IGrainContext>();
        var factory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<IBootstrapSnapshotSource>();
        var reminders = Substitute.For<IReminderRegistry>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var fakeState = new FakePersistentState<BootstrapCoordinatorState>();
        Assert.That(
            () => new LatticeBootstrapCoordinatorGrain(
                context, factory, provider, null!, reminders, resolver, StubOptions(),
                NullLogger<LatticeBootstrapCoordinatorGrain>.Instance, fakeState),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_merge_mode_resolver_is_null()
    {
        var context = Substitute.For<IGrainContext>();
        var factory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<IBootstrapSnapshotSource>();
        var applier = Substitute.For<IReplicationApplier>();
        var reminders = Substitute.For<IReminderRegistry>();
        var fakeState = new FakePersistentState<BootstrapCoordinatorState>();
        Assert.That(
            () => new LatticeBootstrapCoordinatorGrain(
                context, factory, provider, applier, reminders, null!, StubOptions(),
                NullLogger<LatticeBootstrapCoordinatorGrain>.Instance, fakeState),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        var context = Substitute.For<IGrainContext>();
        var factory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<IBootstrapSnapshotSource>();
        var applier = Substitute.For<IReplicationApplier>();
        var reminders = Substitute.For<IReminderRegistry>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var fakeState = new FakePersistentState<BootstrapCoordinatorState>();
        Assert.That(
            () => new LatticeBootstrapCoordinatorGrain(
                context, factory, provider, applier, reminders, resolver, null!,
                NullLogger<LatticeBootstrapCoordinatorGrain>.Instance, fakeState),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // --- GetStateAsync ---

    [Test]
    public async Task GetStateAsync_returns_idle_for_freshly_created_grain()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        Assert.That(await grain.GetStateAsync(CancellationToken.None),
            Is.EqualTo(LatticeBootstrapState.Idle));
    }

    [Test]
    public async Task GetStateAsync_reflects_persisted_phase()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.ApplyingSnapshot);
        var (grain, _, _, _, _, _, _, _) = Create(fake);
        Assert.That(await grain.GetStateAsync(CancellationToken.None),
            Is.EqualTo(LatticeBootstrapState.ApplyingSnapshot));
    }

    [Test]
    public void GetStateAsync_observes_cancellation()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await grain.GetStateAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // --- GetStatusAsync ---

    [Test]
    public async Task GetStatusAsync_returns_idle_with_null_source_for_freshly_created_grain()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        var status = await grain.GetStatusAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(LatticeBootstrapState.Idle));
            Assert.That(status.SourceClusterId, Is.Null);
        });
    }

    [Test]
    public async Task GetStatusAsync_reflects_persisted_phase_and_source_while_in_progress()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.ApplyingSnapshot);
        var (grain, _, _, _, _, _, _, _) = Create(fake);

        var status = await grain.GetStatusAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(LatticeBootstrapState.ApplyingSnapshot));
            Assert.That(status.SourceClusterId, Is.EqualTo(SourceCluster));
        });
    }

    [Test]
    public async Task GetStatusAsync_projects_source_to_null_when_in_progress_flag_is_false()
    {
        // Even if a stale SourceClusterId survived a prior cycle, the
        // façade must report null when InProgress is false so callers
        // do not mistake a terminal state for an active drain.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        fake.State.InProgress = false;
        fake.State.Phase = LatticeBootstrapState.LiveIncremental;
        fake.State.SourceClusterId = SourceCluster;
        var (grain, _, _, _, _, _, _, _) = Create(fake);

        var status = await grain.GetStatusAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(status.SourceClusterId, Is.Null);
        });
    }

    [Test]
    public void GetStatusAsync_observes_cancellation()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await grain.GetStatusAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // --- BootstrapAsync argument guards ---

    [Test]
    public void BootstrapAsync_throws_when_source_cluster_id_is_null()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        Assert.That(
            async () => await grain.BootstrapAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_source_cluster_id_is_empty()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        Assert.That(
            async () => await grain.BootstrapAsync(string.Empty, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_cancelled_up_front()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await grain.BootstrapAsync(SourceCluster, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // --- TryInitiateBootstrapAsync (kickoff persistence path) ---

    [Test]
    public async Task TryInitiateBootstrap_persists_intent_with_RequestingSnapshot_phase()
    {
        var (grain, fakeState, _, _, _, _, _, _) = Create();

        var started = await grain.TryInitiateBootstrapAsync(SourceCluster);

        Assert.That(started, Is.True);
        Assert.That(fakeState.State.InProgress, Is.True);
        Assert.That(fakeState.State.Phase, Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));
        Assert.That(fakeState.State.SourceClusterId, Is.EqualTo(SourceCluster));
        Assert.That(fakeState.State.OperationId, Is.Not.Null.And.Not.Empty);
        Assert.That(fakeState.State.LastAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        Assert.That(fakeState.State.CausalStableFrontier, Is.Not.Null);
        Assert.That(fakeState.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task TryInitiateBootstrap_is_idempotent_with_same_source_cluster()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        fake.State.OperationId = "first-op";
        var (grain, _, _, _, _, _, _, _) = Create(fake);

        var started = await grain.TryInitiateBootstrapAsync(SourceCluster);

        Assert.That(started, Is.False);
        Assert.That(fake.State.OperationId, Is.EqualTo("first-op"));
        Assert.That(fake.WriteCount, Is.EqualTo(0));
    }

    [Test]
    public void TryInitiateBootstrap_throws_when_in_progress_with_different_source_cluster()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, sourceClusterId: SourceCluster);
        var (grain, _, _, _, _, _, _, _) = Create(fake);

        Assert.That(
            async () => await grain.TryInitiateBootstrapAsync(OtherSource),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void TryInitiateBootstrap_throws_when_source_cluster_id_is_null()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        Assert.That(
            async () => await grain.TryInitiateBootstrapAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void TryInitiateBootstrap_throws_when_source_cluster_id_is_empty()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        Assert.That(
            async () => await grain.TryInitiateBootstrapAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    // --- ProcessNextPhaseAsync (phase-pump) ---

    [Test]
    public async Task ProcessNextPhase_in_idle_state_is_a_no_op()
    {
        var (grain, fake, _, provider, _, _, _, _) = Create();

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.InProgress, Is.False);
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Idle));
        await provider.DidNotReceiveWithAnyArgs().ExportAsync(default!, default!, default, default);
    }

    [Test]
    public async Task ProcessNextPhase_in_RequestingSnapshot_drains_stream_and_transitions_to_IncrementalHandoff()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake);
        var asOf = Hlc(123);
        var frontier = new VersionVector();
        var entry = new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(50) };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(asOf, frontier, Stream(entry))));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
        Assert.That(fake.State.InProgress, Is.True);
        Assert.That(fake.State.SnapshotAsOfHlc, Is.EqualTo(asOf));
        Assert.That(fake.State.CausalStableFrontier, Is.SameAs(frontier));
        Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(50)));
        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => IsBootstrapSet(r, "k", Hlc(50))),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhase_applies_every_emitted_entry_in_order()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake);
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "b", Value = new byte[] { 2 }, Timestamp = Hlc(2) },
            new SnapshotEntry { Key = "c", Value = new byte[] { 3 }, Timestamp = Hlc(3) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        Received.InOrder(() =>
        {
            apply.ApplyAsync(
                Arg.Is<WalRecord>(r => IsBootstrapSet(r, "a", Hlc(1))),
                Arg.Any<CancellationToken>());
            apply.ApplyAsync(
                Arg.Is<WalRecord>(r => IsBootstrapSet(r, "b", Hlc(2))),
                Arg.Any<CancellationToken>());
            apply.ApplyAsync(
                Arg.Is<WalRecord>(r => IsBootstrapSet(r, "c", Hlc(3))),
                Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task ProcessNextPhase_skips_entries_with_null_value()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake);
        var entries = new[]
        {
            new SnapshotEntry { Key = "live", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "ghost", Value = null!, Timestamp = Hlc(2) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => IsBootstrapSet(r, "live", Hlc(1))),
            Arg.Any<CancellationToken>());
        await apply.DidNotReceive().ApplyAsync(
            Arg.Is<WalRecord>(r => r.Key == "ghost"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhase_resumes_export_from_persisted_LastAppliedHlc_after_crash()
    {
        // Simulate post-crash state: phase=ApplyingSnapshot, cursor=Hlc(75).
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.ApplyingSnapshot, lastAppliedHlc: Hlc(75));
        var (grain, _, _, provider, _, apply, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, Hlc(75), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(150), new VersionVector(),
                Stream(new SnapshotEntry { Key = "post-crash", Value = new byte[] { 9 }, Timestamp = Hlc(120) }))));

        await grain.ProcessNextPhaseAsync();

        await provider.Received(1).ExportAsync(Tree, SourceCluster, Hlc(75), Arg.Any<CancellationToken>());
        await provider.DidNotReceive().ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
        Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(120)));
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => IsBootstrapSet(r, "post-crash", Hlc(120))),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhase_in_IncrementalHandoff_pins_frontier_and_completes()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        var asOf = Hlc(99);
        var frontier = new VersionVector();
        fake.State.SnapshotAsOfHlc = asOf;
        fake.State.CausalStableFrontier = frontier;
        var (grain, _, _, _, reminders, _, hwm, _) = Create(fake);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        await hwm.Received(1).PinSnapshotAsync(asOf, frontier, Arg.Any<CancellationToken>());
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        Assert.That(fake.State.InProgress, Is.False);
    }

    [Test]
    public async Task ProcessNextPhase_transitions_to_Failed_when_export_throws()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("export boom"));
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
        Assert.That(fake.State.InProgress, Is.False);
    }

    [Test]
    public async Task ProcessNextPhase_transitions_to_Failed_when_apply_throws()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, apply, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(2), new VersionVector(),
                Stream(new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1) }))));
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("apply boom"));
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
        Assert.That(fake.State.InProgress, Is.False);
    }

    [Test]
    public async Task ProcessNextPhase_transitions_to_Failed_when_pin_throws()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotAsOfHlc = Hlc(2);
        fake.State.CausalStableFrontier = new VersionVector();
        var (grain, _, _, _, reminders, _, hwm, _) = Create(fake);
        hwm.PinSnapshotAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("pin boom"));
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
        Assert.That(fake.State.InProgress, Is.False);
    }

    [Test]
    public async Task ProcessNextPhase_drives_full_RequestingSnapshot_to_LiveIncremental_in_two_ticks()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(7), new VersionVector())));
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync(); // drains (empty) → IncrementalHandoff
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));

        await grain.ProcessNextPhaseAsync(); // pins → LiveIncremental
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        Assert.That(fake.State.InProgress, Is.False);
    }

    [Test]
    public async Task TryInitiateBootstrap_after_LiveIncremental_starts_a_fresh_cycle()
    {
        // Operator-driven re-seed scenario: previous bootstrap finished
        // (InProgress=false, Phase=LiveIncremental). A new BootstrapAsync
        // call should reset cursor and transition back to RequestingSnapshot.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        fake.State.InProgress = false;
        fake.State.Phase = LatticeBootstrapState.LiveIncremental;
        fake.State.LastAppliedHlc = Hlc(500);
        var (grain, _, _, _, _, _, _, _) = Create(fake);

        var started = await grain.TryInitiateBootstrapAsync(SourceCluster);

        Assert.That(started, Is.True);
        Assert.That(fake.State.InProgress, Is.True);
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));
        Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task TryInitiateBootstrap_after_Failed_starts_a_fresh_cycle()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        fake.State.InProgress = false;
        fake.State.Phase = LatticeBootstrapState.Failed;
        var (grain, _, _, _, _, _, _, _) = Create(fake);

        var started = await grain.TryInitiateBootstrapAsync(SourceCluster);

        Assert.That(started, Is.True);
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));
    }

    // --- Durability / cursor invariants ---

    [Test]
    public async Task ProcessNextPhase_persists_cursor_every_hundred_entries()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, _, _, _) = Create(fake);
        var entries = Enumerable.Range(1, 250)
            .Select(i => new SnapshotEntry { Key = $"k{i}", Value = new byte[] { 1 }, Timestamp = Hlc(i) })
            .ToArray();
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(1000), new VersionVector(), Stream(entries))));

        // Capture LastAppliedHlc at every write so we can confirm
        // the cursor was persisted at batch boundaries (not only at
        // phase transitions).
        var cursorAtWrite = new List<HybridLogicalClock>();
        fake.OnAfterWrite = s => cursorAtWrite.Add(s.LastAppliedHlc);

        await grain.ProcessNextPhaseAsync();

        // Expected writes:
        //   1. drain-start (Phase=ApplyingSnapshot, cursor=Zero)
        //   2. after entry 100 (cursor=Hlc(100))
        //   3. after entry 200 (cursor=Hlc(200))
        //   4. drain-end → IncrementalHandoff (cursor=Hlc(250))
        Assert.That(fake.WriteCount, Is.EqualTo(4));
        Assert.That(cursorAtWrite, Has.Count.EqualTo(4));
        Assert.That(cursorAtWrite[0], Is.EqualTo(HybridLogicalClock.Zero));
        Assert.That(cursorAtWrite[1], Is.EqualTo(Hlc(100)));
        Assert.That(cursorAtWrite[2], Is.EqualTo(Hlc(200)));
        Assert.That(cursorAtWrite[3], Is.EqualTo(Hlc(250)));
    }

    [Test]
    public async Task ProcessNextPhase_skipped_null_value_does_not_advance_cursor()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, _, _, _) = Create(fake);
        var entries = new[]
        {
            new SnapshotEntry { Key = "live", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "ghost", Value = null!, Timestamp = Hlc(99) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(100), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        // The skipped tombstone's Hlc(99) must NOT advance the resume
        // cursor - on resume after crash we'd otherwise miss any live
        // entry stamped between Hlc(1) and Hlc(99).
        Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(1)));
    }

    [Test]
    public async Task ProcessNextPhase_out_of_order_entries_do_not_regress_cursor()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, _, _, _) = Create(fake);
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = Hlc(10) },
            new SnapshotEntry { Key = "b", Value = new byte[] { 2 }, Timestamp = Hlc(5) },
            new SnapshotEntry { Key = "c", Value = new byte[] { 3 }, Timestamp = Hlc(20) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(100), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        // Cursor must monotonically advance - a lower-HLC entry seen
        // mid-stream must not regress it (otherwise resume from a
        // regressed cursor would re-fetch entries that were already
        // applied).
        Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(20)));
    }

    [Test]
    public async Task ProcessNextPhase_drain_start_persists_ApplyingSnapshot_before_apply()
    {
        // Pin the invariant that on a crash mid-drain the persisted
        // phase is ApplyingSnapshot (not RequestingSnapshot), so the
        // resume path uses the persisted cursor rather than fetching
        // from scratch.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, reminders, apply, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(2), new VersionVector(),
                Stream(new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1) }))));

        var phaseAtWrite = new List<LatticeBootstrapState>();
        fake.OnAfterWrite = s => phaseAtWrite.Add(s.Phase);
        // Throw mid-drain so we can observe the pre-apply persisted
        // phase before the catch handler overwrites it with Failed.
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("boom"));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        // First write must be ApplyingSnapshot (drain start), then
        // the catch handler persists Failed.
        Assert.That(phaseAtWrite, Has.Count.GreaterThanOrEqualTo(2));
        Assert.That(phaseAtWrite[0], Is.EqualTo(LatticeBootstrapState.ApplyingSnapshot));
        Assert.That(phaseAtWrite[^1], Is.EqualTo(LatticeBootstrapState.Failed));
    }

    [Test]
    public async Task ProcessNextPhase_apply_receives_exact_value_payload()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake);
        var payload = new byte[] { 7, 8, 9, 10 };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(2), new VersionVector(),
                Stream(new SnapshotEntry { Key = "k", Value = payload, Timestamp = Hlc(1) }))));

        await grain.ProcessNextPhaseAsync();

        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r =>
                IsBootstrapSet(r, "k", Hlc(1))
                && r.Value!.SequenceEqual(payload)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhase_empty_resume_stream_still_pins_and_completes()
    {
        // Crash-recovery scenario: a prior drain applied entries up to
        // Hlc(50), crashed, then the producer reports no new entries
        // ≤ snapshot.AsOfHlc. The pump must still advance through
        // IncrementalHandoff and pin the frontier rather than spin.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.ApplyingSnapshot, lastAppliedHlc: Hlc(50));
        var (grain, _, _, provider, reminders, _, hwm, _) = Create(fake);
        var asOf = Hlc(60);
        var frontier = new VersionVector();
        provider.ExportAsync(Tree, SourceCluster, Hlc(50), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(asOf, frontier)));
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync(); // empty drain → IncrementalHandoff
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
        Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(50)));

        await grain.ProcessNextPhaseAsync(); // pins → LiveIncremental
        await hwm.Received(1).PinSnapshotAsync(asOf, frontier, Arg.Any<CancellationToken>());
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        Assert.That(fake.State.InProgress, Is.False);
    }

    [Test]
    public async Task ProcessNextPhase_zombie_LiveIncremental_with_in_progress_tears_down()
    {
        // Defensive scenario: a crash between PinSnapshotAsync and
        // the LiveIncremental persist could leave a re-activated grain
        // observing Phase=LiveIncremental && InProgress=true. The
        // pump's default branch must persist InProgress=false and tear
        // down rather than spin.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        fake.State.InProgress = true;
        fake.State.Phase = LatticeBootstrapState.LiveIncremental;
        fake.State.SourceClusterId = SourceCluster;
        var (grain, _, _, provider, reminders, _, hwm, _) = Create(fake);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.InProgress, Is.False);
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        await provider.DidNotReceiveWithAnyArgs().ExportAsync(default!, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().PinSnapshotAsync(default, default!, default);
    }

    [Test]
    public async Task ProcessNextPhase_IncrementalHandoff_writes_state_exactly_once_on_completion()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotAsOfHlc = Hlc(99);
        fake.State.CausalStableFrontier = new VersionVector();
        var (grain, _, _, _, reminders, _, _, _) = Create(fake);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        // The IncrementalHandoff phase persists exactly one write -
        // the LiveIncremental + InProgress=false transition.
        Assert.That(fake.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task ProcessNextPhase_persist_failure_in_catch_keeps_coordinator_armed()
    {
        // B2: if the catch handler's WriteStateAsync throws, the grain
        // must NOT call CompleteCoordinatorAsync. Otherwise persistent
        // state stays at (InProgress=true, Phase=ApplyingSnapshot) with
        // no driver attached - a "looks in-progress but nothing is
        // running" zombie.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("export boom"));
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));
        // Make the catch handler's persist of Failed throw too.
        fake.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        // Reminder must NOT have been unregistered - keepalive stays
        // armed so the next tick can retry.
        await reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    // --- Snapshot drain routes through IReplicationApplier ---
    //
    // The drain pump must hand every snapshot entry to the canonical
    // IReplicationApplier seam (not directly to IReplicationApplyGrain).
    // The tests below pin the observable side-effects every host
    // decorator stacked on top of the applier depends on:
    //
    //   * a host-supplied IReplicationApplier decorator sees every
    //     bootstrap-arrived entry exactly once with the correct shape;
    //   * a transient apply failure surfaces to the drain pump exactly
    //     as the underlying applier surfaces it - the dead-letter /
    //     retry decorators in the production DI graph then own the
    //     park-vs-throw decision (see DeadLetterTrackingReplicationApplier
    //     in src/lattice.replication/).

    [Test]
    public async Task ProcessNextPhase_routes_snapshot_drain_through_IReplicationApplier()
    {
        // Acceptance: a host-supplied decorator on
        // IReplicationApplier must observe every bootstrap-arrived
        // entry exactly once with the producer's HLC + origin id
        // preserved verbatim. The legacy drain bypassed the applier
        // and called IReplicationApplyGrain directly, so decorator
        // observers missed every bootstrap entry.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake);
        var observed = new List<WalRecord>();
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var record = (WalRecord)call[0];
                observed.Add(record);
                return Task.FromResult(new ApplyResult
                {
                    Applied = true,
                    HighWaterMark = record.Timestamp,
                });
            });
        var entries = new[]
        {
            new SnapshotEntry { Key = "k1", Value = new byte[] { 1, 1 }, Timestamp = Hlc(10) },
            new SnapshotEntry { Key = "k2", Value = new byte[] { 2, 2 }, Timestamp = Hlc(20) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(30), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observed, Has.Count.EqualTo(2),
                "decorator must observe every drained entry exactly once");
            Assert.That(observed[0].Key, Is.EqualTo("k1"));
            Assert.That(observed[0].Timestamp, Is.EqualTo(Hlc(10)));
            Assert.That(observed[0].OriginClusterId, Is.EqualTo(SourceCluster),
                "origin id is preserved end-to-end so receiver-side LWW resolution sees the producer's authoring cluster");
            Assert.That(observed[0].TreeId, Is.EqualTo(Tree));
            Assert.That(observed[0].Op, Is.EqualTo(MutationKind.Set));
            Assert.That(observed[0].Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(observed[1].Key, Is.EqualTo("k2"));
            Assert.That(observed[1].Timestamp, Is.EqualTo(Hlc(20)));
        });
    }

    [Test]
    public void ProcessNextPhase_surfaces_applier_failure_to_drain_pump()
    {
        // Acceptance: a failing apply call surfaces from the
        // applier seam exactly as it did from the legacy
        // IReplicationApplyGrain seam, so the surrounding catch
        // handler in ProcessNextPhaseAsync still pivots to Failed and
        // any host-supplied retry / dead-letter decorator stacked on
        // top of IReplicationApplier owns the recovery policy
        // (mirroring the live-incremental apply path).
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, apply, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(2), new VersionVector(),
                Stream(new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1) }))));
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("decorator boom"));
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("decorator boom"));
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
        Assert.That(fake.State.InProgress, Is.False);
    }
}
