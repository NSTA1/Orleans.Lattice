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
    public void Operation_values_are_stable_and_unknown_keeps_its_original_value()
    {
        // Appended after Unknown rather than before it: the values are a stable
        // wire-adjacent contract a host policy is written against, so an existing
        // operation must never shift when a new one is added.
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeTenantAdminApiOperation.CreateTenant, Is.EqualTo(0));
            Assert.That((int)LatticeTenantAdminApiOperation.SuspendTenant, Is.EqualTo(1));
            Assert.That((int)LatticeTenantAdminApiOperation.ResumeTenant, Is.EqualTo(2));
            Assert.That((int)LatticeTenantAdminApiOperation.DeleteTenant, Is.EqualTo(3));
            Assert.That((int)LatticeTenantAdminApiOperation.SetTenantQuotas, Is.EqualTo(4));
            Assert.That((int)LatticeTenantAdminApiOperation.Unknown, Is.EqualTo(5));
            Assert.That((int)LatticeTenantAdminApiOperation.AuthorizeAllowedRegions, Is.EqualTo(6));
            Assert.That((int)LatticeTenantAdminApiOperation.SetTenantResidency, Is.EqualTo(7));
            Assert.That((int)LatticeTenantAdminApiOperation.GetTenantRegionStatus, Is.EqualTo(8));
            Assert.That((int)LatticeTenantAdminApiOperation.ListTenantAdminSubjects, Is.EqualTo(9));
            Assert.That((int)LatticeTenantAdminApiOperation.AddTenantAdminSubject, Is.EqualTo(10));
            Assert.That((int)LatticeTenantAdminApiOperation.RemoveTenantAdminSubject, Is.EqualTo(11));
        });
    }

    [Test]
    public void DescribeCall_maps_each_admin_subject_rpc_and_decodes_the_target_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.ListTenantAdminSubjectsMethodName),
                new TenantAdminTenantRequest { TenantId = "acme" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.ListTenantAdminSubjects, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.AddTenantAdminSubjectMethodName),
                new TenantAdminSubjectRequest { TenantId = "acme", SubjectId = "carol@example.com" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.AddTenantAdminSubject, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.RemoveTenantAdminSubjectMethodName),
                new TenantAdminSubjectRequest { TenantId = "acme", SubjectId = "carol@example.com" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.RemoveTenantAdminSubject, "acme")));
        });
    }

    [Test]
    public void No_admin_subject_rpc_is_exempt_from_authorization()
    {
        // Admin-subject management is tenant-tier at the facade, but it stays behind
        // the transport interceptor exactly as the region-residency RPCs do: the
        // exemption lists cover only auth-scheme discovery and read-only
        // self-service.
        Assert.Multiple(() =>
        {
            foreach (var method in new[]
            {
                LatticeTenantAdminGrpcMethods.ListTenantAdminSubjectsMethodName,
                LatticeTenantAdminGrpcMethods.AddTenantAdminSubjectMethodName,
                LatticeTenantAdminGrpcMethods.RemoveTenantAdminSubjectMethodName,
            })
            {
                Assert.That(
                    LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(Method(method)),
                    Is.False,
                    $"{method} must not be unauthenticated.");
                Assert.That(
                    LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(Method(method)),
                    Is.False,
                    $"{method} must not be self-service-exempt.");
            }
        });
    }

    [Test]
    public void DescribeCall_maps_each_lifecycle_rpc_and_decodes_the_target_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.CreateTenantMethodName),
                new TenantAdminCreateRequest { TenantId = "acme", AdminSubjects = ["ops@example.com"] }),
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
    public void DescribeCall_maps_each_region_residency_rpc_and_decodes_the_target_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName),
                new TenantAdminRegionSetRequest { TenantId = "acme", Regions = ["eu", "ap"] }),
                Is.EqualTo((LatticeTenantAdminApiOperation.AuthorizeAllowedRegions, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName),
                new TenantAdminRegionSetRequest { TenantId = "acme", Regions = ["eu"] }),
                Is.EqualTo((LatticeTenantAdminApiOperation.SetTenantResidency, "acme")));

            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName),
                new TenantAdminTenantRequest { TenantId = "acme" }),
                Is.EqualTo((LatticeTenantAdminApiOperation.GetTenantRegionStatus, "acme")));
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

            // Nor are the region-residency RPCs - including the read-only status
            // read, which is operator-or-tenant-admin at the facade and stays
            // interceptor-enforced here rather than joining the self-service
            // exemption list.
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(
                Method(LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName)), Is.False);
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_never_exempts_a_region_residency_rpc()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTenantAdminGrpcMethods.AuthorizeAllowedRegionsMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTenantAdminGrpcMethods.SetTenantResidencyMethodName)), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTenantAdminGrpcMethods.GetTenantRegionStatusMethodName)), Is.False);
        });
    }
}
