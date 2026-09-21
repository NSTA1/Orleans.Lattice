using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression tests for issue #2608: the backup scheduler drives captures from a
/// reminder tick, which is authored by the Orleans runtime and carries no subject.
/// On a host whose access gate defaults to deny, every gated read the capture
/// performs was refused, so the scope simply stopped being backed up.
/// </summary>
/// <remarks>
/// <para>
/// The failure this fixture pins is the bucket's recurring family: a measurand
/// that is never exercised, whose silence reads as good news. A denied schedule
/// produced no denial anywhere an operator looks - no counter, no distinct
/// outcome, no warning naming the gate - so "this scope has no recent backups"
/// was indistinguishable from "this scope has nothing to back up".
/// </para>
/// <para>
/// Every gate used here is a real <see cref="ILatticeAccessGate"/>.
/// <c>LatticeAccessGateEnforcement</c> short-circuits only for a system-origin
/// turn or the inert <c>NullLatticeAccessGate</c>, so a
/// <see cref="DenyByDefaultAccessGate"/> is genuinely enforced and these tests
/// exercise the same path a production deny-by-default policy does.
/// </para>
/// </remarks>
[TestFixture]
public sealed class BackupSchedulerGrainAccessGateTests
{
    private const string ScopeTreeId = "orders";

    private static readonly BackupScopeSelector TestScope =
        BackupScopeSelector.WholeTree(ScopeTreeId);

    private static readonly string GrainKey = BackupScopeKey.For(TestScope);

    private static BackupSchedulerGrain CreateGrain(
        ILatticeAccessGate gate,
        FakePersistentState<BackupSchedulerState> state,
        out GatedCaptureService capture,
        IReminderRegistry? reminders = null,
        BackupInventoryRegistry? inventory = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("backup-scheduler", GrainKey));

        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupScheduleOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeBackupScheduleOptions { RetentionEnabled = false });

        // The capture service authorizes through a REAL authorizer over the supplied
        // gate, exactly as LatticeBackupCaptureService does before it touches data.
        capture = new GatedCaptureService(new BackupAccessAuthorizer(gate));

        return new BackupSchedulerGrain(
            context,
            reminders ?? Substitute.For<IReminderRegistry>(),
            capture,
            Substitute.For<ILatticeBackupIncrementalCaptureService>(),
            Substitute.For<ILatticeBackupCatalogStore>(),
            Substitute.For<ILatticeBackupSink>(),
            monitor,
            NullLogger<BackupSchedulerGrain>.Instance,
            inventory ?? new BackupInventoryRegistry(),
            new BackupAccessAuthorizer(gate),
            state);
    }

    private static FakePersistentState<BackupSchedulerState> ScopedState()
    {
        var state = new FakePersistentState<BackupSchedulerState>();
        state.State.Scope = TestScope;
        return state;
    }

    // ---- AC1 / AC2: the reminder-driven cycle survives a deny-by-default gate ----

    [Test]
    public async Task ReceiveReminder_captures_on_a_host_whose_gate_defaults_to_deny()
    {
        // Pre-fix this threw LatticeAuthorizationDeniedException out of the reminder
        // handler: the tick carried no subject, so the gate refused the capture and
        // the scope was never backed up.
        var gate = new DenyByDefaultAccessGate();
        var state = ScopedState();
        var grain = CreateGrain(gate, state, out var capture);

        await grain.ReceiveReminder("backup-schedule-full", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(capture.Captures, Is.EqualTo(1), "the scheduled capture must run");
            Assert.That(state.State.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.Success));
            Assert.That(state.State.LastFullSuccessUtc, Is.Not.Null);
        });
    }

    [Test]
    public async Task ReceiveReminder_restores_the_ambient_origin_when_the_cycle_completes()
    {
        // The system-origin marker must not leak past the tick: a later caller-authored
        // turn on the same activation has to be gated normally.
        var gate = new DenyByDefaultAccessGate();
        var state = ScopedState();
        var grain = CreateGrain(gate, state, out _);

        await grain.ReceiveReminder("backup-schedule-full", new TickStatus());

        Assert.That(
            LatticeAccessGateContext.IsGateBypassed,
            Is.False,
            "the system-origin scope must be disposed with the tick");
    }

    // ---- AC3: a denial is loud - a distinct outcome, a counter, and a reason ----

    [Test]
    public async Task A_denied_cycle_records_Denied_and_not_merely_an_absent_backup()
    {
        // A caller-authored cycle on a gated host is still denied - that is correct.
        // What #2608 adds is that the denial is now DISCRIMINATED from a generic
        // fault and from "nothing to back up".
        var gate = new DenyByDefaultAccessGate();
        var state = ScopedState();
        var inventory = new BackupInventoryRegistry();
        var grain = CreateGrain(gate, state, out _, inventory: inventory);

        var measurements = new List<(long Value, string? Scope, string? Reason)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeBackupMetrics.SchedulerFailures,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? scope = null;
                string? reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeBackupMetrics.TagScope)
                    {
                        scope = tag.Value as string;
                    }
                    else if (tag.Key == LatticeBackupMetrics.TagReason)
                    {
                        reason = tag.Value as string;
                    }
                }

                lock (measurements)
                {
                    measurements.Add((value, scope, reason));
                }
            }));

        Assert.That(
            async () => await grain.RunScheduledCycleAsync(incremental: false),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "a denial must surface as a throw, never as a null backup id");

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(
                state.State.LastRunOutcome,
                Is.EqualTo(BackupScopeRunOutcome.Denied),
                "a denial must be distinguishable from a generic Failure");
            Assert.That(measurements, Has.Count.EqualTo(1), "the denial must be counted");
            Assert.That(measurements[0].Value, Is.EqualTo(1));
            Assert.That(measurements[0].Scope, Is.EqualTo(GrainKey));
            Assert.That(
                measurements[0].Reason,
                Is.EqualTo(LatticeBackupMetrics.ReasonPermissionDenied),
                "the reason tag is what separates a denial from an I/O fault");
        });
    }

    [Test]
    public async Task A_generic_capture_fault_is_still_recorded_as_Failure_not_Denied()
    {
        // Guard narrowness: the new Denied outcome must not swallow ordinary faults.
        // This test is expected to pass before AND after the fix - it exists to stop
        // an over-broad change, not to demonstrate the defect.
        var gate = new DenyByDefaultAccessGate();
        gate.Allowed.Add(LatticeOperation.Backup);
        var state = ScopedState();
        var grain = CreateGrain(gate, state, out var capture);
        capture.Fault = new IOException("sink unreachable");

        Assert.That(
            async () => await grain.RunScheduledCycleAsync(incremental: false),
            Throws.InstanceOf<IOException>());

        Assert.That(state.State.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.Failure));
        await Task.CompletedTask;
    }

    // ---- AC4: registration is authorized, so a reminder implies an authorized caller ----

    [Test]
    public void EnsureScheduleAsync_is_denied_for_an_unauthorized_caller()
    {
        // ReceiveReminder runs system-origin, so an unauthorized registration would
        // otherwise buy a recurring gate-bypassed capture. The trust decision is
        // taken here, at registration.
        var gate = new DenyByDefaultAccessGate();
        var grain = CreateGrain(gate, new FakePersistentState<BackupSchedulerState>(), out _);

        Assert.That(
            async () => await grain.EnsureScheduleAsync(TestScope),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
        Assert.That(gate.Consultations, Is.GreaterThan(0), "the gate must actually be consulted");
    }

    [Test]
    public void ScheduleRecurringAsync_is_denied_for_an_unauthorized_caller()
    {
        var gate = new DenyByDefaultAccessGate();
        var grain = CreateGrain(gate, new FakePersistentState<BackupSchedulerState>(), out _);

        Assert.That(
            async () => await grain.ScheduleRecurringAsync(TestScope, incremental: false, TimeSpan.FromHours(1)),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task ScheduleRecurringAsync_succeeds_for_an_authorized_caller()
    {
        // Guard narrowness: an authorized caller must still be able to register.
        var gate = new DenyByDefaultAccessGate();
        gate.Allowed.Add(LatticeOperation.Backup);
        var reminders = Substitute.For<IReminderRegistry>();
        var state = new FakePersistentState<BackupSchedulerState>();
        var grain = CreateGrain(gate, state, out _, reminders: reminders);

        await grain.ScheduleRecurringAsync(TestScope, incremental: false, TimeSpan.FromHours(1));

        Assert.That(state.State.Scope, Is.EqualTo(TestScope));
    }

    /// <summary>
    /// An <see cref="ILatticeBackupCaptureService"/> that authorizes through a real
    /// <see cref="BackupAccessAuthorizer"/> before "capturing", mirroring the order
    /// <c>LatticeBackupCaptureService</c> uses.
    /// </summary>
    private sealed class GatedCaptureService(BackupAccessAuthorizer authorizer) : ILatticeBackupCaptureService
    {
        /// <summary>The number of captures that got past the gate.</summary>
        public int Captures { get; private set; }

        /// <summary>An optional fault raised after authorization succeeds.</summary>
        public Exception? Fault { get; set; }

        public async Task<LatticeBackupCaptureResult> CaptureAsync(
            LatticeBackupCaptureRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            await authorizer.AuthorizeBackupAsync(request.Scope, cancellationToken);

            if (Fault is not null)
            {
                throw Fault;
            }

            Captures++;
            return new LatticeBackupCaptureResult($"backup-{Captures}", Manifest(request));
        }

        public Task<LatticeBackupSetCaptureResult> CaptureSetAsync(
            LatticeBackupSetCaptureRequest request,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        private static BackupManifest Manifest(LatticeBackupCaptureRequest request) =>
            new(
                id: "backup-1",
                name: request.Name,
                createdAtUtc: DateTimeOffset.UnixEpoch,
                kind: BackupKind.Full,
                scope: request.Scope,
                consistencyCut: new BackupConsistencyCut(1, 1),
                topology: new BackupTopologySnapshot(1, 4096, new[] { "d0" }),
                structuralDigest: "digest-root",
                keyDescriptors: [],
                contentDescriptors: [],
                provenance: []);
    }
}
