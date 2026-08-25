using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantBackupRestoreAdmission"/>: the active per-record
/// admission controller a restore stream consults once per record. Its verdict is a
/// pure function of the pre-resolved cross-tenant flag and key quota, so every case
/// is exact and allocation-free - no clock, no timing, no ordering dependence.
/// </summary>
[TestFixture]
public sealed class TenantBackupRestoreAdmissionTests
{
    [Test]
    public void Cross_tenant_admission_refuses_every_record()
    {
        var admission = new TenantBackupRestoreAdmission(crossTenant: true, maxKeys: null);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.CrossTenant));
            Assert.That(admission.Admit("b"), Is.EqualTo(BackupRestoreRecordDisposition.CrossTenant));
            Assert.That(admission.DeadLetteredCrossTenant, Is.EqualTo(2));
            Assert.That(admission.AdmittedCount, Is.Zero);
            Assert.That(admission.DeadLetteredOverQuota, Is.Zero);
        });
    }

    [Test]
    public void Within_quota_records_are_admitted_then_the_rest_are_over_quota()
    {
        var admission = new TenantBackupRestoreAdmission(crossTenant: false, maxKeys: 2);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.Admit("b"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.Admit("c"), Is.EqualTo(BackupRestoreRecordDisposition.OverQuota));
            Assert.That(admission.Admit("d"), Is.EqualTo(BackupRestoreRecordDisposition.OverQuota));
            Assert.That(admission.AdmittedCount, Is.EqualTo(2));
            Assert.That(admission.DeadLetteredOverQuota, Is.EqualTo(2));
            Assert.That(admission.DeadLetteredCrossTenant, Is.Zero);
        });
    }

    [Test]
    public void A_null_quota_admits_every_record()
    {
        var admission = new TenantBackupRestoreAdmission(crossTenant: false, maxKeys: null);

        Assert.Multiple(() =>
        {
            for (var i = 0; i < 1000; i++)
            {
                Assert.That(admission.Admit("k" + i), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            }

            Assert.That(admission.AdmittedCount, Is.EqualTo(1000));
            Assert.That(admission.DeadLetteredOverQuota, Is.Zero);
        });
    }

    [Test]
    public void A_zero_quota_admits_nothing()
    {
        var admission = new TenantBackupRestoreAdmission(crossTenant: false, maxKeys: 0);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.OverQuota));
            Assert.That(admission.AdmittedCount, Is.Zero);
            Assert.That(admission.DeadLetteredOverQuota, Is.EqualTo(1));
        });
    }

    [Test]
    public void Admit_null_key_throws()
    {
        var admission = new TenantBackupRestoreAdmission(crossTenant: false, maxKeys: null);

        Assert.That(() => admission.Admit(null!), Throws.ArgumentNullException);
    }
}
