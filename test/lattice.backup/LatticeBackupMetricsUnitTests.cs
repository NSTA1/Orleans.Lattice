using System.IO;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for the static helpers on <see cref="LatticeBackupMetrics"/> that
/// do not require a running meter infrastructure: <see cref="LatticeBackupMetrics.RecordSchedulerOverrun"/>
/// (argument validation) and <see cref="LatticeBackupMetrics.MapReason"/> (exception
/// classification). These exercice every switch arm in MapReason and both guard
/// branches in RecordSchedulerOverrun.
/// </summary>
[TestFixture]
public sealed class LatticeBackupMetricsUnitTests
{
    // ---- RecordSchedulerOverrun ----------------------------------------

    [Test]
    public void RecordSchedulerOverrun_with_valid_scope_key_does_not_throw()
    {
        // Lines 343-345: the normal path through RecordSchedulerOverrun.
        Assert.That(() => LatticeBackupMetrics.RecordSchedulerOverrun("my-scope"), Throws.Nothing);
    }

    [Test]
    public void RecordSchedulerOverrun_with_null_or_empty_scope_key_throws()
    {
        // Line 343: the null/empty guard on RecordSchedulerOverrun.
        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeBackupMetrics.RecordSchedulerOverrun(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => LatticeBackupMetrics.RecordSchedulerOverrun(""), Throws.InstanceOf<ArgumentException>());
        });
    }

    // ---- MapReason ------------------------------------------------------

    [Test]
    public void MapReason_classifies_authorization_denied_as_permission_denied()
    {
        // Line 405: LatticeAuthorizationDeniedException -> ReasonPermissionDenied.
        var reason = LatticeBackupMetrics.MapReason(new LatticeAuthorizationDeniedException("denied"));
        Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonPermissionDenied));
    }

    [Test]
    public void MapReason_classifies_saturation_as_saturation()
    {
        // Line 406: LatticeSaturatedException -> ReasonSaturation.
        var reason = LatticeBackupMetrics.MapReason(new LatticeSaturatedException("saturated"));
        Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonSaturation));
    }

    [Test]
    public void MapReason_classifies_cursor_snapshot_expired_as_saturation()
    {
        // Line 407: LatticeCursorSnapshotExpiredException -> ReasonSaturation.
        var reason = LatticeBackupMetrics.MapReason(new LatticeCursorSnapshotExpiredException("expired"));
        Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonSaturation));
    }

    [Test]
    public void MapReason_classifies_restore_validation_as_integrity_mismatch()
    {
        // Line 409: LatticeRestoreValidationException -> ReasonIntegrityMismatch.
        var reason = LatticeBackupMetrics.MapReason(new LatticeRestoreValidationException("mismatch"));
        Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonIntegrityMismatch));
    }

    [Test]
    public void MapReason_classifies_operation_cancelled_as_cancellation()
    {
        // Line 410: OperationCanceledException -> ReasonCancellation.
        var reason = LatticeBackupMetrics.MapReason(new OperationCanceledException());
        Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonCancellation));
    }

    [Test]
    public void MapReason_classifies_io_exception_as_sink_io_error()
    {
        // Line 411: IOException -> ReasonSinkIoError.
        var reason = LatticeBackupMetrics.MapReason(new IOException("disk full"));
        Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonSinkIoError));
    }

    [Test]
    public void MapReason_classifies_unknown_exception_as_unknown()
    {
        // Line 412: default -> ReasonUnknown.
        var reason = LatticeBackupMetrics.MapReason(new InvalidOperationException("unexpected"));
        Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonUnknown));
    }
}
