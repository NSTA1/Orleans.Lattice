using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// An admin-set change that read the tenant's record before the tenant was
/// deleted must not re-create the tenant when it writes back (issue #6230).
/// </summary>
public sealed partial class LatticeTenantAccessAdminTests
{
    [Test]
    public void Adding_an_admin_while_the_tenant_is_deleted_does_not_recreate_it()
    {
        var registry = new DeletingTenantRegistry(Tenant);
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.AddAdminSubjectAsync(Tenant, "bob@example.com"),
            Throws.TypeOf<TenantRecordNotFoundException>());
        Assert.That(registry.Peek(Tenant), Is.Null, "the deleted tenant stays deleted");
    }
}
