using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupRestoreRecordDisposition"/>: the per-record
/// decision the restore stream applies. Its members are wire-stable ordinals
/// (<c>Admit</c> is the default zero), so a mis-numbering would silently change
/// the per-record verdict.
/// </summary>
[TestFixture]
public sealed class BackupRestoreRecordDispositionTests
{
    [Test]
    public void Admit_is_the_default_zero_value()
    {
        Assert.That((int)default(BackupRestoreRecordDisposition), Is.Zero);
        Assert.That(default(BackupRestoreRecordDisposition), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
    }

    [Test]
    public void Members_have_their_expected_ordinals()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)BackupRestoreRecordDisposition.Admit, Is.EqualTo(0));
            Assert.That((int)BackupRestoreRecordDisposition.CrossTenant, Is.EqualTo(1));
            Assert.That((int)BackupRestoreRecordDisposition.OverQuota, Is.EqualTo(2));
        });
    }

    [Test]
    public void The_enum_has_exactly_three_members()
    {
        Assert.That(Enum.GetValues<BackupRestoreRecordDisposition>(), Has.Length.EqualTo(3));
    }
}
