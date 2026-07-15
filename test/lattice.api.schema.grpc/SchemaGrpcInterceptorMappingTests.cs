using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Unit tests for the schema control-API interceptor's pure decode helpers -
/// <see cref="LatticeSchemaApiGrpcAuthInterceptor.DescribeCall{TRequest}"/> and
/// <see cref="LatticeSchemaApiGrpcAuthInterceptor.IsUnauthenticatedMethod"/> -
/// asserted directly, without standing up a gRPC server. Proves every RPC maps to
/// its <see cref="LatticeSchemaApiOperation"/>, the target tree id is decoded from
/// each request shape, an unrecognised method degrades to
/// <see cref="LatticeSchemaApiOperation.Unknown"/> (never a permissive default),
/// and only <c>GetAuthScheme</c> is exempt from authorization.
/// </summary>
[TestFixture]
public sealed class SchemaGrpcInterceptorMappingTests
{
    private const string Svc = "/orleans.lattice.api.schema/";

    private static string Method(string name) => Svc + name;

    [Test]
    public void DescribeCall_maps_each_method_name_to_its_operation()
    {
        Assert.Multiple(() =>
        {
            AssertOp(LatticeSchemaGrpcMethods.SetPolicyMethodName, LatticeSchemaApiOperation.SetPolicy);
            AssertOp(LatticeSchemaGrpcMethods.ClearPolicyMethodName, LatticeSchemaApiOperation.ClearPolicy);
            AssertOp(LatticeSchemaGrpcMethods.GetPolicyMethodName, LatticeSchemaApiOperation.GetPolicy);
            AssertOp(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName, LatticeSchemaApiOperation.StreamDeadLetters);
            AssertOp(LatticeSchemaGrpcMethods.CountDeadLettersMethodName, LatticeSchemaApiOperation.CountDeadLetters);
            AssertOp(LatticeSchemaGrpcMethods.SetVersionConfigMethodName, LatticeSchemaApiOperation.SetVersionConfig);
            AssertOp(LatticeSchemaGrpcMethods.GetVersionConfigMethodName, LatticeSchemaApiOperation.GetVersionConfig);
            AssertOp(LatticeSchemaGrpcMethods.AdvanceTargetVersionMethodName, LatticeSchemaApiOperation.AdvanceTargetVersion);
            AssertOp(LatticeSchemaGrpcMethods.AdvanceAndMigrateMethodName, LatticeSchemaApiOperation.AdvanceAndMigrate);
            AssertOp(LatticeSchemaGrpcMethods.MigrateToTargetVersionMethodName, LatticeSchemaApiOperation.MigrateToTargetVersion);
            AssertOp(LatticeSchemaGrpcMethods.ClearVersionConfigMethodName, LatticeSchemaApiOperation.ClearVersionConfig);
            AssertOp(LatticeSchemaGrpcMethods.RemediateMethodName, LatticeSchemaApiOperation.Remediate);
            AssertOp(LatticeSchemaGrpcMethods.GetRemediationStatusMethodName, LatticeSchemaApiOperation.GetRemediationStatus);
            AssertOp(LatticeSchemaGrpcMethods.ScanComplianceMethodName, LatticeSchemaApiOperation.ScanCompliance);
            AssertOp(LatticeSchemaGrpcMethods.ProbeCapabilitiesMethodName, LatticeSchemaApiOperation.ProbeCapabilities);
        });

        static void AssertOp(string methodName, LatticeSchemaApiOperation expected)
        {
            var (operation, _) = LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
                Svc + methodName, new SchemaTreeRequest { TreeId = "orders" });
            Assert.That(operation, Is.EqualTo(expected), methodName);
        }
    }

    [Test]
    public void DescribeCall_unrecognised_method_maps_to_unknown()
    {
        var (operation, _) = LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
            Method("SomeFutureRpc"), new SchemaTreeRequest { TreeId = "orders" });

        Assert.That(operation, Is.EqualTo(LatticeSchemaApiOperation.Unknown));
    }

    [Test]
    public void DescribeCall_decodes_the_target_tree_from_each_request_shape()
    {
        Assert.Multiple(() =>
        {
            AssertTarget(new SchemaTreeRequest { TreeId = "t1" }, "t1");
            AssertTarget(new SetPolicyRequest { TreeId = "t2", Policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>()) }, "t2");
            AssertTarget(new SetVersionConfigRequest { TreeId = "t3", Config = new LatticeSchemaVersionConfig(1, 2) }, "t3");
            AssertTarget(new AdvanceVersionRequest { TreeId = "t4", NewTargetVersion = 5 }, "t4");
            AssertTarget(
                new RemediateRequest
                {
                    TreeId = "t5",
                    Transform = LatticeValueTransform.Passthrough(),
                    TargetPolicy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>()),
                },
                "t5");
        });

        static void AssertTarget<TRequest>(TRequest request, string expected)
        {
            var (_, targetId) = LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
                Svc + LatticeSchemaGrpcMethods.SetPolicyMethodName, request);
            Assert.That(targetId, Is.EqualTo(expected));
        }
    }

    [Test]
    public void DescribeCall_unknown_request_shape_has_no_target()
    {
        var (_, targetId) = LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
            Method(LatticeSchemaGrpcMethods.GetAuthSchemeMethodName), new AuthSchemeAdvertisementRequest());

        Assert.That(targetId, Is.Null);
    }

    [Test]
    public void IsUnauthenticatedMethod_exempts_only_get_auth_scheme()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeSchemaApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    Method(LatticeSchemaGrpcMethods.GetAuthSchemeMethodName)),
                Is.True);
            Assert.That(
                LatticeSchemaApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    Method(LatticeSchemaGrpcMethods.SetPolicyMethodName)),
                Is.False);
            Assert.That(
                LatticeSchemaApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    Method(LatticeSchemaGrpcMethods.ScanComplianceMethodName)),
                Is.False);
        });
    }
}
