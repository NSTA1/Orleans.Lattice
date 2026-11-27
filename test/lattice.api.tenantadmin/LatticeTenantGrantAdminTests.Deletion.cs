using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// A grant call that read the granting tenant's record before the tenant was
/// deleted must not re-create the tenant when it writes back: the registry's
/// update refuses an absent record, and the facade reports the outcome as it
/// reports an unoffered grant (issue #6230).
/// </summary>
public sealed partial class LatticeTenantGrantAdminTests
{
    [Test]
    public void Approving_while_the_granting_tenant_is_deleted_does_not_recreate_it()
    {
        var registry = new DeletingTenantRegistry(Granter);
        var granter = Tenant(Granter, "alice@acme.example");
        granter.OfferGrant(
            CrossTenantGrant.Create(Grantee, TenantGranteeKind.Tenant, Scope, TenantGrantOperations.Read),
            Stamp(10),
            "seed");
        registry.Seed(granter);
        registry.Seed(Tenant(Grantee, "bob@beta.example"));
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.ApproveGrantAsync(Granter, Grantee, Scope),
            Throws.TypeOf<TenantGrantNotFoundException>());
        Assert.That(registry.Peek(Granter), Is.Null, "the deleted granting tenant stays deleted");
    }
}
