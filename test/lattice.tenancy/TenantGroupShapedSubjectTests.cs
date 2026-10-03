using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for the reserved tenant-group namespace on the subject side
/// of an authorization probe. Tenant group entries are stored as literal
/// <c>t/{tenant}/{name}</c> strings in the <em>same</em> slot maps as subject ids,
/// so a principal whose asserted <c>sub</c> is literally a group id would
/// exact-match a stored group admin entry and administer the tenant without ever
/// belonging to the group. Every write seam already refuses a non-group id in that
/// namespace; these tests pin the read-side half of the rule on
/// <see cref="TenantRecord"/> and on the compiled snapshot, and pin that the
/// genuine group path still works.
/// </summary>
[TestFixture]
public sealed class TenantGroupShapedSubjectTests
{
    private const string TenantGroup = "t/acme/editors";

    private static TenantRecord GroupAdminRecord(string tenant = "acme")
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Clock(1),
            "test");
        record.AddAdminSubject(TenantGroup, Clock(2), "test");
        return record;
    }

    private static CompiledTenant Compiled(TenantRecord record)
    {
        var policy = CompiledTenantPolicy.Compile([record], true);
        Assert.That(policy.TryGetTenant(record.Id.Value!, out var tenant), Is.True);
        return tenant!;
    }

    [Test]
    public void IsAdmin_subjectIdShapedLikeAGroupEntry_doesNotAdministerTheTenant()
    {
        var record = GroupAdminRecord();

        Assert.That(
            record.IsAdmin(TenantGroup, Array.Empty<string>()),
            Is.False,
            "a subject id in the reserved t/ namespace must not match a stored group entry");
    }

    [Test]
    public void IsMember_subjectIdShapedLikeAGroupEntry_doesNotActAsTheTenant()
    {
        var record = GroupAdminRecord();
        record.AddMemberSubject(TenantGroup, Clock(3), "test");

        Assert.That(record.IsMember(TenantGroup, Array.Empty<string>()), Is.False);
    }

    [Test]
    public void IsAdmin_subjectThatActuallyHoldsTheGroup_stillAdministersTheTenant()
    {
        var record = GroupAdminRecord();

        Assert.That(
            record.IsAdmin("alice", new[] { TenantGroup }),
            Is.True,
            "the genuine group path must be unaffected");
    }

    [Test]
    public void HasAdminSubject_stillReportsTheStoredGroupEntry()
    {
        var record = GroupAdminRecord();

        Assert.Multiple(() =>
        {
            Assert.That(
                record.HasAdminSubject(TenantGroup),
                Is.True,
                "set inspection is not authorization and must keep listing group entries");
            Assert.That(record.AdminSubjects, Does.Contain(TenantGroup));
        });
    }

    [Test]
    public void CompiledTenant_IsAdmin_subjectIdShapedLikeAGroupEntry_doesNotAdministerTheTenant()
    {
        var tenant = Compiled(GroupAdminRecord());

        Assert.Multiple(() =>
        {
            Assert.That(tenant.IsAdmin(TenantGroup), Is.False);
            Assert.That(tenant.IsAdmin(TenantGroup, Array.Empty<string>()), Is.False);
            Assert.That(tenant.IsMember(TenantGroup, Array.Empty<string>()), Is.False);
        });
    }

    [Test]
    public void CompiledTenant_IsMember_subjectIdShapedLikeAMemberGroupEntry_doesNotActAsTheTenant()
    {
        var record = GroupAdminRecord();
        record.AddMemberSubject("t/acme/readers", Clock(3), "test");
        var tenant = Compiled(record);

        Assert.That(tenant.IsMember("t/acme/readers", Array.Empty<string>()), Is.False);
    }

    [Test]
    public void CompiledTenant_subjectThatActuallyHoldsTheGroup_stillAdministersTheTenant()
    {
        var tenant = Compiled(GroupAdminRecord());

        Assert.Multiple(() =>
        {
            Assert.That(tenant.IsAdmin("alice", new[] { TenantGroup }), Is.True);
            Assert.That(tenant.IsMember("alice", new[] { TenantGroup }), Is.True);
        });
    }
}
