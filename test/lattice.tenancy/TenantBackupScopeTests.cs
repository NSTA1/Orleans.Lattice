using NSubstitute;
using Orleans.Lattice.Backup;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantBackupScope"/>: the active
/// <see cref="ILatticeBackupTenantScope"/> that confines a capture / restore to the
/// active tenant's <c>t/{tenantId}/{name}</c> namespace and its key quota. Tree
/// ownership is derived from the tree id by <see cref="LatticeTenantTrees.GetOwner"/>,
/// the ambient active tenant is set directly, and the registry is substituted, so
/// every decision is exact and timing-independent.
/// </summary>
[TestFixture]
public sealed class TenantBackupScopeTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private const string AcmeTree = "t/acme/orders";
    private const string BetaTree = "t/beta/orders";
    private const string PlatformTree = "sys-foo";

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static ITenantRegistry Registry(long? maxKeys = null)
    {
        var registry = Substitute.For<ITenantRegistry>();
        var record = TenantRecord.Create(
            Acme, TenantStatus.Active, new TenantQuotas { MaxKeys = maxKeys }, TenantPlacement.Shared, Clock(10), "w1");
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).Returns(record);
        return registry;
    }

    private static TenantBackupScope Create(ITenantRegistry? registry = null) =>
        new(registry ?? Registry());

    // ---- IsActive -------------------------------------------------------

    [Test]
    public void IsActive_is_true()
    {
        Assert.That(Create().IsActive, Is.True);
    }

    // ---- AuthorizeCapture / AuthorizeRestoreTarget ----------------------

    [Test]
    public void AuthorizeCapture_own_tree_is_allowed()
    {
        LatticeActiveTenantContext.Current = Acme;
        var scope = Create();

        Assert.That(() => scope.AuthorizeCapture(AcmeTree), Throws.Nothing);
    }

    [Test]
    public void AuthorizeCapture_cross_tenant_tree_is_refused()
    {
        LatticeActiveTenantContext.Current = Acme;
        var scope = Create();

        Assert.That(
            () => scope.AuthorizeCapture(BetaTree),
            Throws.InstanceOf<LatticeBackupTenantIsolationException>());
    }

    [Test]
    public void AuthorizeCapture_platform_tree_is_allowed_and_deferred_to_the_gate()
    {
        LatticeActiveTenantContext.Current = Acme;
        var scope = Create();

        Assert.That(() => scope.AuthorizeCapture(PlatformTree), Throws.Nothing);
    }

    [Test]
    public void AuthorizeCapture_with_no_active_tenant_is_deferred_to_the_gate()
    {
        var scope = Create();

        Assert.That(() => scope.AuthorizeCapture(BetaTree), Throws.Nothing,
            "with no active tenant the tenant scope defers to the auth gate rather than refusing");
    }

    [Test]
    public void AuthorizeRestoreTarget_own_tree_is_allowed()
    {
        LatticeActiveTenantContext.Current = Acme;
        var scope = Create();

        Assert.That(() => scope.AuthorizeRestoreTarget(AcmeTree), Throws.Nothing);
    }

    [Test]
    public void AuthorizeRestoreTarget_cross_tenant_tree_is_refused()
    {
        LatticeActiveTenantContext.Current = Acme;
        var scope = Create();

        Assert.That(
            () => scope.AuthorizeRestoreTarget(BetaTree),
            Throws.InstanceOf<LatticeBackupTenantIsolationException>());
    }

    [Test]
    public void Authorize_null_or_empty_tree_throws()
    {
        var scope = Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => scope.AuthorizeCapture(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => scope.AuthorizeCapture(""), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => scope.AuthorizeRestoreTarget(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => scope.AuthorizeRestoreTarget(""), Throws.InstanceOf<ArgumentException>());
        });
    }

    // ---- BeginRestoreAsync ----------------------------------------------

    [Test]
    public async Task BeginRestoreAsync_own_tree_admits_up_to_the_quota()
    {
        LatticeActiveTenantContext.Current = Acme;
        var scope = Create(Registry(maxKeys: 2));

        var admission = await scope.BeginRestoreAsync(AcmeTree);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.Admit("b"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.Admit("c"), Is.EqualTo(BackupRestoreRecordDisposition.OverQuota));
            Assert.That(admission.DeadLetteredOverQuota, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task BeginRestoreAsync_own_tree_with_no_quota_admits_every_record()
    {
        LatticeActiveTenantContext.Current = Acme;
        var scope = Create(Registry(maxKeys: null));

        var admission = await scope.BeginRestoreAsync(AcmeTree);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.Admit("b"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.DeadLetteredOverQuota, Is.Zero);
        });
    }

    [Test]
    public async Task BeginRestoreAsync_with_no_active_tenant_admits_every_record_without_consulting_the_registry()
    {
        var registry = Registry(maxKeys: 1);
        var scope = Create(registry);

        var admission = await scope.BeginRestoreAsync(AcmeTree);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.Admit("b"), Is.EqualTo(BackupRestoreRecordDisposition.Admit));
            Assert.That(admission.DeadLetteredOverQuota, Is.Zero);
        });
        await registry.DidNotReceive().GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BeginRestoreAsync_cross_tenant_target_refuses_every_record_without_consulting_the_registry()
    {
        LatticeActiveTenantContext.Current = Acme;
        var registry = Registry(maxKeys: 10);
        var scope = Create(registry);

        var admission = await scope.BeginRestoreAsync(BetaTree);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admit("a"), Is.EqualTo(BackupRestoreRecordDisposition.CrossTenant));
            Assert.That(admission.DeadLetteredCrossTenant, Is.EqualTo(1));
        });
        await registry.DidNotReceive().GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void BeginRestoreAsync_null_or_empty_target_throws()
    {
        var scope = Create();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await scope.BeginRestoreAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await scope.BeginRestoreAsync(""), Throws.InstanceOf<ArgumentException>());
        });
    }
}
