namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaRemediationReport"/>: the factory
/// helpers that describe in-flight, completed, and aborted remediation states.
/// </summary>
public sealed class LatticeSchemaRemediationReportTests
{
    [Test]
    public void InFlight_records_phase_progress_destination_and_operation()
    {
        var report = LatticeSchemaRemediationReport.InFlight(
            LatticeSchemaRemediationPhase.Build,
            scannedCount: 7,
            destinationTreeId: "orders/remediated/op",
            operationId: "op");

        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build));
        Assert.That(report.InProgress, Is.True);
        Assert.That(report.ScannedCount, Is.EqualTo(7));
        Assert.That(report.DestinationTreeId, Is.EqualTo("orders/remediated/op"));
        Assert.That(report.OperationId, Is.EqualTo("op"));
        Assert.That(report.Succeeded, Is.False);
        Assert.That(report.DidAbort, Is.False);
    }

    [Test]
    public void Completed_records_terminal_success()
    {
        var report = LatticeSchemaRemediationReport.Completed(3, "orders/remediated/op", "op");

        Assert.That(report.Succeeded, Is.True);
        Assert.That(report.DidAbort, Is.False);
        Assert.That(report.InProgress, Is.False);
        Assert.That(report.ScannedCount, Is.EqualTo(3));
        Assert.That(report.DestinationTreeId, Is.EqualTo("orders/remediated/op"));
        Assert.That(report.OperationId, Is.EqualTo("op"));
    }

    [Test]
    public void Aborted_records_first_offending_entry()
    {
        var preview = new byte[] { 1, 2, 3 };
        var report = LatticeSchemaRemediationReport.Aborted(2, "k2", "too long", preview, "op");

        Assert.That(report.DidAbort, Is.True);
        Assert.That(report.Succeeded, Is.False);
        Assert.That(report.InProgress, Is.False);
        Assert.That(report.ScannedCount, Is.EqualTo(2));
        Assert.That(report.OffendingKey, Is.EqualTo("k2"));
        Assert.That(report.Reason, Is.EqualTo("too long"));
        Assert.That(report.OffendingValuePreview, Is.EqualTo(preview));
        Assert.That(report.OperationId, Is.EqualTo("op"));
    }
}
