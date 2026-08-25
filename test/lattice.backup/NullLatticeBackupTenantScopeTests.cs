using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="NullLatticeBackupTenantScope"/> and its
/// <see cref="PermissiveBackupRestoreAdmission"/>: the inert default the core
/// backup package registers when no tenancy add-on is present. It reports
/// <see cref="ILatticeBackupTenantScope.IsActive"/> as <c>false</c>, both
/// authorization methods are no-ops, and its admission admits every record with no
/// dead-lettering, so the tenancy-off capture / restore path is byte-for-byte
/// unchanged.
/// </summary>
[TestFixture]
public sealed class NullLatticeBackupTenantScopeTests
{
    private static NullLatticeBackupTenantScope Scope => NullLatticeBackupTenantScope.Instance;

    [Test]
    public void IsActive_is_false()
    {
        Assert.That(Scope.IsActive, Is.False);
    }

    [Test]
    public void AuthorizeCapture_never_throws_for_any_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => Scope.AuthorizeCapture("t/acme/orders"), Throws.Nothing);
            Assert.That(() => Scope.AuthorizeCapture("sys-foo"), Throws.Nothing);
            Assert.That(() => Scope.AuthorizeCapture("legacy"), Throws.Nothing);
        });
    }

    [Test]
    public void AuthorizeRestoreTarget_never_throws_for_any_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => Scope.AuthorizeRestoreTarget("t/acme/orders"), Throws.Nothing);
            Assert.That(() => Scope.AuthorizeRestoreTarget("sys-foo"), Throws.Nothing);
        });
    }

    [Test]
    public async Task BeginRestoreAsync_returns_a_permissive_admission_that_admits_every_record()
    {
        var admission = await Scope.BeginRestoreAsync("t/acme/orders");

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.Admit("b"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.DeadLetteredCrossTenant, Is.Zero);
            Assert.That(admission.DeadLetteredOverQuota, Is.Zero);
        });
    }

    [Test]
    public void Instance_is_a_shared_singleton()
    {
        Assert.That(NullLatticeBackupTenantScope.Instance, Is.SameAs(NullLatticeBackupTenantScope.Instance));
    }

    // ---- PermissiveBackupRestoreAdmission -------------------------------

    [Test]
    public void Permissive_admission_admits_and_never_counts()
    {
        var admission = PermissiveBackupRestoreAdmission.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("k"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.AdmittedCount, Is.Zero, "the permissive admission tracks nothing");
            Assert.That(admission.DeadLetteredCrossTenant, Is.Zero);
            Assert.That(admission.DeadLetteredOverQuota, Is.Zero);
        });
    }

    [Test]
    public void Permissive_admission_Admit_null_key_throws()
    {
        Assert.That(() => PermissiveBackupRestoreAdmission.Instance.Admit(null!), Throws.ArgumentNullException);
    }
}
