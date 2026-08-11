namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Unit tests for the tree-administration control-API interceptor's pure decode
/// helpers - <see cref="LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall{TRequest}"/>
/// and <see cref="LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod"/> -
/// asserted directly, without standing up a gRPC server. Proves the capability-probe
/// RPC maps to its <see cref="LatticeTreeAdminApiOperation"/>, the target tree id is
/// decoded from the request shape, an unrecognised method degrades to
/// <see cref="LatticeTreeAdminApiOperation.Unknown"/> (never a permissive default),
/// and only <c>GetAuthScheme</c> is exempt from authorization.
/// </summary>
[TestFixture]
public sealed class TreeAdminGrpcInterceptorMappingTests
{
    private const string Svc = "/orleans.lattice.api.treeadmin/";

    private static string Method(string name) => Svc + name;

    [Test]
    public void DescribeCall_maps_probe_capabilities_to_its_operation()
    {
        var (operation, targetId) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            Method(LatticeTreeAdminGrpcMethods.ProbeCapabilitiesMethodName),
            new TreeAdminTreeRequest { TreeId = "orders" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTreeAdminApiOperation.ProbeCapabilities));
            Assert.That(targetId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void DescribeCall_unrecognised_method_maps_to_unknown()
    {
        var (operation, _) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            Method("SomeFutureRpc"), new TreeAdminTreeRequest { TreeId = "orders" });

        Assert.That(operation, Is.EqualTo(LatticeTreeAdminApiOperation.Unknown));
    }

    [Test]
    public void DescribeCall_unknown_request_shape_has_no_target()
    {
        var (_, targetId) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            Method(LatticeTreeAdminGrpcMethods.GetAuthSchemeMethodName), new AuthSchemeAdvertisementRequest());

        Assert.That(targetId, Is.Null);
    }

    [Test]
    public void IsUnauthenticatedMethod_exempts_only_get_auth_scheme()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    Method(LatticeTreeAdminGrpcMethods.GetAuthSchemeMethodName)),
                Is.True);
            Assert.That(
                LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    Method(LatticeTreeAdminGrpcMethods.ProbeCapabilitiesMethodName)),
                Is.False);
        });
    }
}
