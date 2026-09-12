using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Value coverage for the per-scope observable gauges
/// <c>orleans.lattice.backup.scope.last_run_status</c> and
/// <c>orleans.lattice.backup.scope.last_success_age</c>.
/// <para>
/// Before issue #2645 a scope entered <see cref="BackupInventoryRegistry"/> only
/// when a capture cycle <em>completed</em>, so
/// <see cref="BackupScopeRunOutcome.None"/> was structurally unreachable while
/// the gauge's published description promised <c>0=none</c>. "Never run" and
/// "not scheduled" were both absence, so an operator could not tell a scope
/// whose first cycle had not landed yet from one nobody had scheduled. These
/// tests pin the three-way reading the corrected description now guarantees:
/// an absent series means unscheduled, <c>0</c> means scheduled with nothing
/// completed yet, and <c>1</c> / <c>2</c> / <c>3</c> are terminal outcomes.
/// </para>
/// <para>
/// No test asserted on either gauge before this fixture existed, which is how
/// an undeliverable enum value survived to a live endpoint.
/// </para>
/// </summary>
[TestFixture]
public sealed class BackupScopeStatusGaugeTests
{
    private const string StatusInstrument = "orleans.lattice.backup.scope.last_run_status";
    private const string SuccessAgeInstrument = "orleans.lattice.backup.scope.last_success_age";

    private const string ScopeKey = "0|gauge-scope|";
    private const string OtherScopeKey = "0|other-gauge-scope|";

    /// <summary>
    /// The gauges read the process-wide singleton, so each test starts from a
    /// cleared registry and leaves one behind.
    /// </summary>
    [SetUp]
    public void SetUp() => BackupInventoryRegistry.Instance.Reset();

    [TearDown]
    public void TearDown() => BackupInventoryRegistry.Instance.Reset();

    // ---- 0 is reachable and means "scheduled, nothing completed yet" ----

    [Test]
    public void ScopeLastRunStatus_emits_zero_for_a_registered_but_never_run_scope()
    {
        // The assertion whose absence let issue #2645 ship. EnsureScopeRegistered
        // is what makes BackupScopeRunOutcome.None observable; without it this
        // scope emits no series at all.
        BackupInventoryRegistry.Instance.EnsureScopeRegistered(ScopeKey);

        var status = SampleStatus();

        Assert.That(status, Does.ContainKey(ScopeKey),
            "a registered scope must publish a series before any cycle has completed; "
            + "no series here is the defect this test exists to catch");
        Assert.That(status[ScopeKey], Is.EqualTo((long)BackupScopeRunOutcome.None));
    }

    [Test]
    public void ScopeLastSuccessAge_emits_minus_one_for_a_registered_but_never_run_scope()
    {
        BackupInventoryRegistry.Instance.EnsureScopeRegistered(ScopeKey);

        var age = SampleSuccessAge();

        Assert.That(age, Does.ContainKey(ScopeKey));
        Assert.That(age[ScopeKey], Is.EqualTo(-1d),
            "the companion gauge must report 'never' rather than an age of zero, which would read as "
            + "a success that just happened");
    }

    // ---- Absence still means "not scheduled" ----------------------------

    [Test]
    public void ScopeLastRunStatus_emits_no_series_for_a_scope_that_was_never_registered()
    {
        // The negative control for the test above. Registering one scope and not
        // another discriminates "the gauge publishes registered scopes" from
        // "the gauge publishes everything", which is what makes a 0 reading mean
        // something. Without this, a gauge that emitted 0 for every conceivable
        // scope would satisfy the first test.
        BackupInventoryRegistry.Instance.EnsureScopeRegistered(ScopeKey);

        var status = SampleStatus();

        Assert.That(status, Does.ContainKey(ScopeKey));
        Assert.That(status, Does.Not.ContainKey(OtherScopeKey),
            "an unscheduled, unrun scope must remain an absent series, which is what 0 is distinguishable from");
    }

    // ---- Terminal outcomes still discriminate ---------------------------

    [Test]
    public void ScopeLastRunStatus_emits_the_terminal_outcome_after_a_recorded_run()
    {
        // Discrimination: proves the zero above is a real reading of a real
        // state, not a gauge that reports 0 unconditionally.
        BackupInventoryRegistry.Instance.RecordScopeOutcome(
            ScopeKey, BackupScopeRunOutcome.Failure, DateTimeOffset.UtcNow);

        var status = SampleStatus();

        Assert.That(status[ScopeKey], Is.EqualTo((long)BackupScopeRunOutcome.Failure));
    }

    [Test]
    public void ScopeLastRunStatus_emits_three_for_a_denied_run()
    {
        // Denied = 3 is reachable (BackupSchedulerGrain classifies a gated
        // refusal as Denied), so the description must enumerate it. This test is
        // the evidence for that half of the description change.
        BackupInventoryRegistry.Instance.RecordScopeOutcome(
            ScopeKey, BackupScopeRunOutcome.Denied, DateTimeOffset.UtcNow);

        var status = SampleStatus();

        Assert.That(status[ScopeKey], Is.EqualTo(3L));
    }

    // ---- Registration is non-destructive --------------------------------

    [Test]
    public void EnsureScopeRegistered_does_not_reset_an_already_recorded_outcome()
    {
        // A schedule is re-registered on every EnsureScheduleAsync, so a
        // destructive registration would silently wipe a recorded failure back to
        // 0 on the next reminder refresh and report a failing scope as pending.
        BackupInventoryRegistry.Instance.RecordScopeOutcome(
            ScopeKey, BackupScopeRunOutcome.Failure, DateTimeOffset.UtcNow);

        BackupInventoryRegistry.Instance.EnsureScopeRegistered(ScopeKey);

        var status = SampleStatus();
        Assert.That(status[ScopeKey], Is.EqualTo((long)BackupScopeRunOutcome.Failure));
    }

    // ---- Sampling helpers -----------------------------------------------

    private static Dictionary<string, long> SampleStatus()
    {
        var observed = new Dictionary<string, long>(StringComparer.Ordinal);
        Sample(
            longCallback: (name, value, scope) =>
            {
                if (name == StatusInstrument && scope is not null)
                {
                    observed[scope] = value;
                }
            },
            doubleCallback: null);
        return observed;
    }

    private static Dictionary<string, double> SampleSuccessAge()
    {
        var observed = new Dictionary<string, double>(StringComparer.Ordinal);
        Sample(
            longCallback: null,
            doubleCallback: (name, value, scope) =>
            {
                if (name == SuccessAgeInstrument && scope is not null)
                {
                    observed[scope] = value;
                }
            });
        return observed;
    }

    private static void Sample(
        Action<string, long, string?>? longCallback,
        Action<string, double, string?>? doubleCallback)
    {
        // The gauges are DECLARED on LatticeBackupMetrics but PUBLISHED on
        // BackupMetrics.Meter, which is a different class. Passing the meter to
        // MeterListening therefore forces BackupMetrics's initialiser but NOT
        // LatticeBackupMetrics's, so the observable gauges may not exist yet when
        // the listener starts. MeterListener.Start replays only instruments that
        // already exist, so the gauges would never be enabled, every sample would
        // come back empty, and each assertion above would fail as "expected 1, got
        // 0" while pointing at production. Run the declaring class's initialiser
        // first, explicitly, so the instruments are published before Start.
        RuntimeHelpers.RunClassConstructor(typeof(LatticeBackupMetrics).TypeHandle);

        using var listener = MeterListening.StartForMeter(
            BackupMetrics.Meter,
            new[] { StatusInstrument, SuccessAgeInstrument },
            l =>
            {
                l.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
                    longCallback?.Invoke(instrument.Name, measurement, ScopeOf(tags)));
                l.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
                    doubleCallback?.Invoke(instrument.Name, measurement, ScopeOf(tags)));
            });

        listener.RecordObservableInstruments();
    }

    private static string? ScopeOf(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == LatticeBackupMetrics.TagScope)
            {
                return tag.Value as string;
            }
        }

        return null;
    }
}
