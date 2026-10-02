using NSubstitute;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #4123: <see cref="SchemaRemediationDriver"/> drives an accepted remediation
/// from outside its grain, one slice per call, reporting each slice's phase, values
/// processed and phase total, and turning a cancellation into a cancel of the
/// remediation it follows. Driven against substitutes, with no timing dependence.
/// </summary>
[TestFixture]
public sealed class SchemaRemediationDriverTests
{
    private const string OperationId = "op-drive";

    private static LatticeSchemaRemediationReport InFlight(LatticeSchemaRemediationPhase phase, int scanned) =>
        LatticeSchemaRemediationReport.InFlight(phase, scanned, "t/remediated/x", OperationId);

    private static SchemaRemediationSlice Slice(LatticeSchemaRemediationPhase phase, int scanned, int? total) =>
        new(InFlight(phase, scanned), total);

    private static readonly LatticeSchemaRemediationReport Done =
        LatticeSchemaRemediationReport.Completed(5, "t/remediated/x", OperationId);

    private sealed class RecordingProgress : ILatticeOperationProgress
    {
        public List<(string Phase, long Completed, long? Total, string? Unit)> Reports { get; } = [];

        public Func<bool> Refuse { get; set; } = () => false;

        public ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            if (Refuse())
            {
                throw new OperationCanceledException();
            }

            Reports.Add((phase, completedUnits, totalUnits, unitName));
            return ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task DriveAsync_reports_each_slice_phase_values_and_total_until_terminal()
    {
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        grain.RunSliceAsync().Returns(
            Slice(LatticeSchemaRemediationPhase.DryRun, 2, null),
            Slice(LatticeSchemaRemediationPhase.Build, 0, 5),
            Slice(LatticeSchemaRemediationPhase.Build, 4, 5),
            Slice(LatticeSchemaRemediationPhase.Cutover, 5, null),
            new SchemaRemediationSlice(Done, null));
        var progress = new RecordingProgress();

        var report = await SchemaRemediationDriver.DriveAsync(
            grain, InFlight(LatticeSchemaRemediationPhase.DryRun, 0), progress, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report, Is.EqualTo(Done));
            Assert.That(progress.Reports, Is.EqualTo(new (string, long, long?, string?)[]
            {
                (SchemaOperationPhases.DryRun, 0, null, SchemaOperationPhases.ValuesUnit),
                (SchemaOperationPhases.DryRun, 2, null, SchemaOperationPhases.ValuesUnit),
                (SchemaOperationPhases.Build, 0, 5, SchemaOperationPhases.ValuesUnit),
                (SchemaOperationPhases.Build, 4, 5, SchemaOperationPhases.ValuesUnit),
                (SchemaOperationPhases.Cutover, 0, null, null),
            }));
        });
    }

    [Test]
    public async Task DriveAsync_of_an_already_terminal_report_runs_no_slice()
    {
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        var progress = new RecordingProgress();

        var report = await SchemaRemediationDriver.DriveAsync(grain, Done, progress, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report, Is.EqualTo(Done));
            Assert.That(progress.Reports, Is.Empty);
        });
        await grain.DidNotReceive().RunSliceAsync();
    }

    [Test]
    public async Task DriveAsync_turns_a_cancellation_into_a_cancel_of_the_followed_remediation()
    {
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        var cancelled = LatticeSchemaRemediationReport.Cancelled(2, OperationId);
        grain.CancelAsync(OperationId).Returns(cancelled);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var report = await SchemaRemediationDriver.DriveAsync(
            grain, InFlight(LatticeSchemaRemediationPhase.Build, 2), progress: null, cts.Token);

        Assert.That(report, Is.EqualTo(cancelled));
        await grain.Received(1).CancelAsync(OperationId);
        await grain.DidNotReceive().RunSliceAsync();
    }

    [Test]
    public async Task DriveAsync_drives_on_to_completion_when_cutover_declines_the_cancel()
    {
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        grain.CancelAsync(OperationId).Returns(InFlight(LatticeSchemaRemediationPhase.Cutover, 5));
        grain.RunSliceAsync().Returns(new SchemaRemediationSlice(Done, null));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var report = await SchemaRemediationDriver.DriveAsync(
            grain, InFlight(LatticeSchemaRemediationPhase.Cutover, 5), progress: null, cts.Token);

        Assert.That(report.Succeeded, Is.True);
        await grain.Received(1).CancelAsync(OperationId);
        await grain.Received(1).RunSliceAsync();
    }

    [Test]
    public async Task DriveAsync_still_cancels_the_remediation_when_the_sink_refuses_reports_after_cancellation()
    {
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        using var cts = new CancellationTokenSource();
        var slices = 0;
        grain.RunSliceAsync().Returns(_ =>
        {
            // The first slice is where cancellation lands; any further slice ends
            // the run, so a driver that ignored the request fails on the report
            // rather than looping.
            cts.Cancel();
            return ++slices == 1 ? Slice(LatticeSchemaRemediationPhase.Build, 3, 5) : new SchemaRemediationSlice(Done, null);
        });
        var cancelled = LatticeSchemaRemediationReport.Cancelled(3, OperationId);
        grain.CancelAsync(OperationId).Returns(cancelled);
        var progress = new RecordingProgress { Refuse = () => cts.IsCancellationRequested };

        var report = await SchemaRemediationDriver.DriveAsync(
            grain, InFlight(LatticeSchemaRemediationPhase.Build, 0), progress, cts.Token);

        Assert.That(report, Is.EqualTo(cancelled), "the sink's refusal must not abandon the remediation mid-phase");
        await grain.Received(1).RunSliceAsync();
        await grain.Received(1).CancelAsync(OperationId);
    }

    [Test]
    public void DriveAsync_rejects_a_null_grain() =>
        Assert.That(
            async () => await SchemaRemediationDriver.DriveAsync(null!, Done, null, CancellationToken.None),
            Throws.ArgumentNullException);

    [TestCase(LatticeSchemaRemediationPhase.DryRun, SchemaOperationPhases.DryRun)]
    [TestCase(LatticeSchemaRemediationPhase.Build, SchemaOperationPhases.Build)]
    [TestCase(LatticeSchemaRemediationPhase.Cutover, SchemaOperationPhases.Cutover)]
    [TestCase(LatticeSchemaRemediationPhase.Completed, "Completed")]
    public void PhaseName_maps_each_phase_to_its_operation_phase(LatticeSchemaRemediationPhase phase, string expected) =>
        Assert.That(SchemaRemediationDriver.PhaseName(phase), Is.EqualTo(expected));
}
