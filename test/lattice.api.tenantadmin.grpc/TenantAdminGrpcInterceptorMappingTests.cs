namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for the tenant-administration interceptor's pure decode helpers -
/// <see cref="LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall{TRequest}"/>
/// and <see cref="LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod"/> -
/// asserted directly, without standing up a gRPC server. Proves each lifecycle RPC
/// maps to its <see cref="LatticeTenantAdminApiOperation"/>, the target tenant id
/// is decoded from the request shape, an unrecognised method degrades to
/// <see cref="LatticeTenantAdminApiOperation.Unknown"/> (never a permissive
/// default), and only <c>GetAuthScheme</c> is exempt from authorization.
/// </summary>
[TestFixture]
public sealed class TenantAdminGrpcInterceptorMappingTests
{
    private const string Svc = "/orleans.lattice.api.tenantadmin/";

    private static string Method(string name) => Svc + name;

    [Test]
    public void Operation_values_are_stable_and_unknown_is_last()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeTenantAdminApiOperation.CreateTenant, Is.EqualTo(0));
            Assert.That((int)LatticeTenantAdminApiOperation.SuspendTenant, Is.EqualTo(1));
            Assert.That((int)LatticeTenantAdminApiOperation.ResumeTenant, Is.EqualTo(2));
            Assert.That((int)LatticeTenantAdminApiOperation.DeleteTenant, Is.EqualTo(3));
            Assert.That((int)LatticeTenantAdminApiOperation.SetTenantQuotas, Is.EqualTo(4));
            Assert.That((int)LatticeTenantAdminApiOperation.Unknown, Is.EqualTo(5));
        });
    }

    [Test]
    public void DescribeCall_maps_each_lifecycle_rpc_and_decodes_the_target_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.CreateTenantMethodName),
                new TenantAdminTenantRequest { TenantId = "acme" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.CreateTenant, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.SuspendTenantMethodName),
                new TenantAdminTenantRequest { TenantId = "acme" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.SuspendTenant, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.ResumeTenantMethodName),
                new TenantAdminTenantRequest { TenantId = "acme" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.ResumeTenant, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName),
                new TenantAdminTenantRequest { TenantId = "acme" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.DeleteTenant, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.SetTenantQuotasMethodName),
                new TenantAdminSetQuotasRequest { TenantId = "acme", Quotas = TenantQuotasDescriptor.Unbounded }),
                Is.EqualTo((LatticeTenantAdminApiOperation.SetTenantQuotas, "acme")));
        });
    }

    [Test]
    public void DescribeCall_maps_an_unrecognised_method_to_unknown_with_no_target()
    {
        var (operation, targetId) = LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
            Method("SomethingElse"),
            new AuthSchemeAdvertisementRequest());

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTenantAdminApiOperation.Unknown));
            Assert.That(targetId, Is.Null);
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_exempts_only_get_auth_scheme()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTenantAdminGrpcMethods.GetAuthSchemeMethodName)), Is.True);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTenantAdminGrpcMethods.CreateTenantMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)), Is.False);
        });
    }

    [Test]
    public void IsSelfServiceMethod_exempts_only_the_three_read_only_self_service_rpcs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.GetCurrentTenantMethodName)), Is.True);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.ListAccessibleTenantsMethodName)), Is.True);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.GetTenantMethodName)), Is.True);

            // The mutating lifecycle RPCs are never self-service-exempt: they stay
            // behind the default-deny admin authorizer.
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.CreateTenantMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.DeleteTenantMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.SetTenantQuotasMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.GetAuthSchemeMethodName)), Is.False);
        });
    }
}
