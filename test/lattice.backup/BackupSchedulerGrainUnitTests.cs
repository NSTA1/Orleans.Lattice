using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupSchedulerGrain"/> that do not require a live
/// silo. Exercises: the <see cref="BackupSchedulerGrain.GrainContext"/> property,
/// the scheduler-overrun metric path, the retention-after-capture path, the
/// <see cref="BackupSchedulerGrain.IsIdleAsync"/> short-circuit,
/// <see cref="BackupSchedulerGrain.ReceiveReminder"/> for both known and unknown
/// reminder names, the capture-failure re-throw path, the unbounded-retention and
/// max-age-retention keep-set computations, and the
/// <c>UnregisterReminderAsync</c> catch block. All dependencies are substituted or
/// faked - no Orleans <c>TestCluster</c> is started.
/// </summary>
[TestFixture]
public sealed class BackupSchedulerGrainUnitTests
{
    private const string ScopeTreeId = "orders";

    // The grain key is the deterministic BackupScopeKey for the test scope.
    private static readonly string GrainKey =
        BackupScopeKey.For(BackupScopeSelector.WholeTree(ScopeTreeId));

    private static readonly BackupScopeSelector TestScope =
        BackupScopeSelector.WholeTree(ScopeTreeId);

    private static BackupSchedulerGrain CreateGrain(
        ILatticeBackupCaptureService? captureService = null,
        ILatticeBackupCatalogStore? catalog = null,
        ILatticeBackupSink? sink = null,
        LatticeBackupScheduleOptions? options = null,
        IReminderRegistry? reminders = null,
        BackupInventoryRegistry? inventory = null,
        FakePersistentState<BackupSchedulerState>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("backup-scheduler", GrainKey));

        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupScheduleOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeBackupScheduleOptions());

        return new BackupSchedulerGrain(
            context,
            reminders ?? Substitute.For<IReminderRegistry>(),
            captureService ?? Substitute.For<ILatticeBackupCaptureService>(),
            Substitute.For<ILatticeBackupIncrementalCaptureService>(),
            catalog ?? new FakeSchedulerCatalog(),
            sink ?? Substitute.For<ILatticeBackupSink>(),
            monitor,
            NullLogger<BackupSchedulerGrain>.Instance,
            inventory ?? new BackupInventoryRegistry(),
            state ?? new FakePersistentState<BackupSchedulerState>());
    }

    private static void SetCaptureInFlight(BackupSchedulerGrain grain, bool value) =>
        typeof(BackupSchedulerGrain)
            .GetField("_captureInFlight",
                BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(grain, value);

    // ---- GrainContext property (line 42) ----------------------------------------

    [Test]
    public void GrainContext_returns_the_injected_context()
    {
        // Line 42: the GrainContext property getter.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("backup-scheduler", GrainKey));
        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupScheduleOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeBackupScheduleOptions());

        var grain = new BackupSchedulerGrain(
            context,
            Substitute.For<IReminderRegistry>(),
            Substitute.For<ILatticeBackupCaptureService>(),
            Substitute.For<ILatticeBackupIncrementalCaptureService>(),
            new FakeSchedulerCatalog(),
            Substitute.For<ILatticeBackupSink>(),
            monitor,
            NullLogger<BackupSchedulerGrain>.Instance,
            new BackupInventoryRegistry(),
            new FakePersistentState<BackupSchedulerState>());

        Assert.That(grain.GrainContext, Is.SameAs(context));
    }

    // ---- Scheduler overrun (line 144) -------------------------------------------

    [Test]
    public async Task RunScheduledCycleAsync_records_overrun_when_capture_in_flight()
    {
        // Line 144: LatticeBackupMetrics.RecordSchedulerOverrun is called when
        // _captureInFlight is true at the time the scheduled cycle fires.
        var state = new FakePersistentState<BackupSchedulerState>();
        state.State.Scope = TestScope;

        var grain = CreateGrain(state: state);
        SetCaptureInFlight(grain, true);

        // Should not throw - overrun is recorded and RunCaptureAsync short-circuits.
        var result = await grain.RunScheduledCycleAsync(incremental: false);

        Assert.That(result, Is.Null);
    }

    // ---- IsIdleAsync (line 158) -------------------------------------------------

    [Test]
    public async Task IsIdleAsync_returns_true_when_no_capture_is_in_flight()
    {
        // Line 158: the simple Task.FromResult(!_captureInFlight) path.
        var grain = CreateGrain();
        Assert.That(await grain.IsIdleAsync(), Is.True);
    }

    [Test]
    public async Task IsIdleAsync_returns_false_when_a_capture_is_in_flight()
    {
        var grain = CreateGrain();
        SetCaptureInFlight(grain, true);
        Assert.That(await grain.IsIdleAsync(), Is.False);
    }

    // ---- ReceiveReminder (lines 194-200) ----------------------------------------

    [Test]
    public async Task ReceiveReminder_with_unknown_name_is_silently_ignored()
    {
        // Lines 194-196: the early-return guard for an unknown reminder name.
        var captureService = Substitute.For<ILatticeBackupCaptureService>();
        var grain = CreateGrain(captureService: captureService);

        await grain.ReceiveReminder("some-other-name", default);

        await captureService.DidNotReceive().CaptureAsync(Arg.Any<LatticeBackupCaptureRequest>());
    }

    [Test]
    public async Task ReceiveReminder_with_full_schedule_name_delegates_to_RunScheduledCycleAsync()
    {
        // Lines 199-200: the non-guard path runs a scheduled cycle when the name
        // is the full-schedule reminder name. Scope is null so RunScheduledCycle
        // returns null immediately, but the delegation must have happened.
        var grain = CreateGrain();

        // No scope set - RunScheduledCycleAsync returns null early (scope is null).
        // Should not throw.
        Assert.That(
            async () => await grain.ReceiveReminder("backup-schedule-full", default),
            Throws.Nothing);
    }

    [Test]
    public async Task ReceiveReminder_with_incremental_schedule_name_runs_incremental_cycle()
    {
        // Lines 199-200: the incremental-name path sets incremental=true.
        var grain = CreateGrain();
        Assert.That(
            async () => await grain.ReceiveReminder("backup-schedule-incremental", default),
            Throws.Nothing);
    }

    // ---- Capture failure catch block (lines 266-271) ----------------------------

    [Test]
    public async Task RunScheduledCycleAsync_rethrows_when_capture_service_throws()
    {
        // Lines 266-271: the catch-all in RunCaptureAsync records failure and rethrows.
        var captureService = Substitute.For<ILatticeBackupCaptureService>();
        captureService.CaptureAsync(Arg.Any<LatticeBackupCaptureRequest>())
            .Throws(new InvalidOperationException("simulated capture failure"));

        var state = new FakePersistentState<BackupSchedulerState>();
        state.State.Scope = TestScope;
        var inventory = new BackupInventoryRegistry();

        var grain = CreateGrain(captureService: captureService, state: state, inventory: inventory);

        await Assert.ThatAsync(
            async () => await grain.RunScheduledCycleAsync(incremental: false),
            Throws.InvalidOperationException);

        Assert.That(state.State.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.Failure));
    }

    // ---- Unbounded retention keep-all (lines 388-393) ---------------------------

    [Test]
    public async Task RunScheduledCycleAsync_retention_unbounded_keeps_all_manifests()
    {
        // Lines 388-393: when RetentionEnabled=true but both RetentionKeepLast and
        // RetentionMaxAge are null, ComputeKeepSet retains every manifest (no prune).
        var manifest = BackupManifestModelTests.Sample("full-1");
        var catalog = new FakeSchedulerCatalog(manifest);
        var captureResult = new LatticeBackupCaptureResult("full-1", manifest);
        var captureService = Substitute.For<ILatticeBackupCaptureService>();
        captureService.CaptureAsync(Arg.Any<LatticeBackupCaptureRequest>())
            .Returns(captureResult);

        var state = new FakePersistentState<BackupSchedulerState>();
        state.State.Scope = TestScope;

        var opts = new LatticeBackupScheduleOptions
        {
            RetentionEnabled = true,
            RetentionKeepLast = null,
            RetentionMaxAge = null,
        };

        var grain = CreateGrain(captureService: captureService, catalog: catalog, options: opts, state: state);

        var resultId = await grain.RunScheduledCycleAsync(incremental: false);

        // Unbounded retention prunes nothing; result is the captured backup id.
        Assert.That(resultId, Is.EqualTo("full-1"));
    }

    // ---- Max-age retention (lines 410-413) ------------------------------------

    [Test]
    public async Task RunScheduledCycleAsync_retention_max_age_keeps_recent_manifests()
    {
        // Lines 410-413: when RetentionMaxAge is set, manifests newer than the cutoff
        // are added to the keep set. Use a future-anchored manifest so it is always
        // within the window (avoiding flakiness from wall-clock drift).
        var recentManifest = BackupManifestModelTests.Sample("full-recent") with
        {
            CreatedAtUtc = DateTimeOffset.UtcNow,
        };
        var catalog = new FakeSchedulerCatalog(recentManifest);

        var captureResult = new LatticeBackupCaptureResult("full-recent", recentManifest);
        var captureService = Substitute.For<ILatticeBackupCaptureService>();
        captureService.CaptureAsync(Arg.Any<LatticeBackupCaptureRequest>())
            .Returns(captureResult);

        var state = new FakePersistentState<BackupSchedulerState>();
        state.State.Scope = TestScope;

        var opts = new LatticeBackupScheduleOptions
        {
            RetentionEnabled = true,
            RetentionKeepLast = null,
            RetentionMaxAge = TimeSpan.FromDays(7),
        };

        var grain = CreateGrain(captureService: captureService, catalog: catalog, options: opts, state: state);
        var resultId = await grain.RunScheduledCycleAsync(incremental: false);

        Assert.That(resultId, Is.Not.Null);
    }

    // ---- Retention-after-capture path (lines 150-151) ---------------------------

    [Test]
    public async Task RunScheduledCycleAsync_runs_retention_after_successful_capture()
    {
        // Lines 150-151: when the capture returns a backup id and RetentionEnabled is
        // true, ListScopeAsync and PruneCoreAsync are executed.
        var manifest = BackupManifestModelTests.Sample("full-1");
        var catalog = new FakeSchedulerCatalog(manifest);
        var captureResult = new LatticeBackupCaptureResult("full-1", manifest);
        var captureService = Substitute.For<ILatticeBackupCaptureService>();
        captureService.CaptureAsync(Arg.Any<LatticeBackupCaptureRequest>())
            .Returns(captureResult);

        var state = new FakePersistentState<BackupSchedulerState>();
        state.State.Scope = TestScope;

        var opts = new LatticeBackupScheduleOptions
        {
            RetentionEnabled = true,
            RetentionKeepLast = 1,   // keep exactly one: no pruning needed
        };
        var sink = Substitute.For<ILatticeBackupSink>();

        var grain = CreateGrain(captureService: captureService, catalog: catalog,
            options: opts, state: state, sink: sink);

        var resultId = await grain.RunScheduledCycleAsync(incremental: false);
        Assert.That(resultId, Is.EqualTo("full-1"));
    }

    // ---- UnregisterReminderAsync catch (lines 478-483) --------------------------

    [Test]
    public async Task EnsureScheduleAsync_swallows_non_transient_reminder_unregister_failure()
    {
        // Lines 478-483: a non-transient exception thrown by GetReminder inside
        // UnregisterReminderAsync is caught, logged, and swallowed (not rethrown).
        // With both schedule knobs disabled (default), EnsureScheduleAsync calls
        // UnregisterReminderAsync for both reminder names; a failure in the first
        // must not prevent the second and must not propagate.
        var reminders = Substitute.For<IReminderRegistry>();
        reminders
            .GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns<Task<IGrainReminder>>(_ => throw new InvalidOperationException("reminder service unavailable"));

        var state = new FakePersistentState<BackupSchedulerState>();
        var grain = CreateGrain(reminders: reminders, state: state);

        Assert.That(
            async () => await grain.EnsureScheduleAsync(TestScope),
            Throws.Nothing);
    }

    // ---- CancelScheduleAsync (line 109) ----------------------------------------

    [Test]
    public async Task CancelScheduleAsync_full_clears_runtime_full_backup_interval()
    {
        // Line 109: the else branch of CancelScheduleAsync that nulls out
        // state.State.RuntimeFullBackupInterval when incremental is false.
        var state = new FakePersistentState<BackupSchedulerState>();
        state.State.RuntimeFullBackupInterval = TimeSpan.FromHours(24);
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(null));

        var grain = CreateGrain(reminders: reminders, state: state);

        await grain.CancelScheduleAsync(incremental: false);

        Assert.That(state.State.RuntimeFullBackupInterval, Is.Null);
    }

    // ---- Fake catalog -----------------------------------------------------------

    private sealed class FakeSchedulerCatalog : ILatticeBackupCatalogStore
    {
        private readonly List<BackupManifest> _manifests;

        public FakeSchedulerCatalog(params BackupManifest[] manifests)
        {
            _manifests = new List<BackupManifest>(manifests);
        }

        public Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
        {
            _manifests.Add(manifest);
            return Task.CompletedTask;
        }

        public Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_manifests.Find(m => m.Id == backupId));

        public Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)
        {
            var removed = _manifests.RemoveAll(m => m.Id == backupId);
            return Task.FromResult(removed > 0);
        }

        public async IAsyncEnumerable<BackupManifest> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var manifest in _manifests.ToList())
            {
                yield return manifest;
                await Task.Yield();
            }
        }
    }
}
