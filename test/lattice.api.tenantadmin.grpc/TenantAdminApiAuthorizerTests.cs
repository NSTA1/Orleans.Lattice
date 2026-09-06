using Grpc.Core;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for the two shipped <see cref="ILatticeTenantAdminApiAuthorizer"/>
/// implementations: the default-deny <see cref="DenyTenantAdminApiAuthorizer"/>
/// (registered automatically so an un-configured host fails closed) and the
/// opt-in <see cref="AllowAllTenantAdminApiAuthorizer"/>.
/// </summary>
[TestFixture]
public sealed class TenantAdminApiAuthorizerTests
{
    private static LatticeTenantAdminApiAuthorizationContext Context(LatticeTenantAdminApiOperation operation)
    {
        var call = new FakeServerCallContext("/orleans.lattice.api.tenantadmin/CreateTenant");
        return new LatticeTenantAdminApiAuthorizationContext(call, operation, "acme");
    }

    [Test]
    public async Task DenyTenantAdminApiAuthorizer_denies_every_operation()
    {
        var authorizer = new DenyTenantAdminApiAuthorizer();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeTenantAdminApiOperation.CreateTenant), default), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeTenantAdminApiOperation.DeleteTenant), default), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeTenantAdminApiOperation.Unknown), default), Is.False);
        });
    }

    [Test]
    public async Task AllowAllTenantAdminApiAuthorizer_allows_every_operation()
    {
        var authorizer = new AllowAllTenantAdminApiAuthorizer();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeTenantAdminApiOperation.CreateTenant), default), Is.True);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeTenantAdminApiOperation.DeleteTenant), default), Is.True);
        });
    }

    [Test]
    public void AuthorizationContext_rejects_a_null_call()
    {
        Assert.That(
            () => new LatticeTenantAdminApiAuthorizationContext(null!, LatticeTenantAdminApiOperation.CreateTenant, "acme"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AuthorizationContext_carries_the_operation_and_target()
    {
        var call = new FakeServerCallContext("/orleans.lattice.api.tenantadmin/DeleteTenant");
        var context = new LatticeTenantAdminApiAuthorizationContext(call, LatticeTenantAdminApiOperation.DeleteTenant, "acme");

        Assert.Multiple(() =>
        {
            Assert.That(context.Operation, Is.EqualTo(LatticeTenantAdminApiOperation.DeleteTenant));
            Assert.That(context.TargetId, Is.EqualTo("acme"));
            Assert.That(context.Call, Is.SameAs(call));
        });
    }
}
