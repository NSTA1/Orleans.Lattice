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
    public void DescribeCall_maps_the_read_only_diagnostics_rpcs_to_their_operations()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.GetShardHotnessMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.GetShardHotness, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.GetDiagnosticsMethodName),
                new TreeAdminDiagnosticsRequest { TreeId = "orders", Deep = true }),
                Is.EqualTo((LatticeTreeAdminApiOperation.GetDiagnostics, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.InspectShardMapMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.InspectShardMap, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.GetProjectionDigestMethodName),
                new TreeAdminShardRequest { TreeId = "orders", ShardIndex = 2 }),
                Is.EqualTo((LatticeTreeAdminApiOperation.GetProjectionDigest, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.GetTreeStatsMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.GetTreeStats, "orders")));
        });
    }

    [Test]
    public void DescribeCall_cluster_storage_usage_has_no_target_tree()
    {
        var (operation, targetId) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            Method(LatticeTreeAdminGrpcMethods.GetStorageUsageMethodName),
            new TreeAdminStorageUsageRequest { Deep = true });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTreeAdminApiOperation.GetStorageUsage));
            Assert.That(targetId, Is.Null);
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_does_not_exempt_the_diagnostics_rpcs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.GetShardHotnessMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.GetStorageUsageMethodName)), Is.False);
        });
    }

    [Test]
    public void DescribeCall_maps_the_lifecycle_rpcs_to_their_operations()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.CreateTreeMethodName),
                new TreeAdminCreateRequest { TreeId = "orders", ShardCount = 8 }),
                Is.EqualTo((LatticeTreeAdminApiOperation.CreateTree, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.CheckTreeExistsMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.CheckTreeExists, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.SetTreeAliasMethodName),
                new TreeAdminSetAliasRequest { TreeId = "orders", PhysicalTreeId = "phys" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.SetTreeAlias, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.ResolveTreeAliasMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.ResolveTreeAlias, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.GetTreeConfigMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.GetTreeConfig, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.SetTreeConfigMethodName),
                new TreeAdminSetConfigRequest { TreeId = "orders", Update = new TreeConfigurationUpdate() }),
                Is.EqualTo((LatticeTreeAdminApiOperation.SetTreeConfig, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.GetShardMapMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.GetShardMap, "orders")));
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_does_not_exempt_the_mutating_lifecycle_rpcs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.CreateTreeMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.SetTreeAliasMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.SetTreeConfigMethodName)), Is.False);
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
    public void DescribeCall_decodes_the_target_tree_from_the_bulk_load_request_shapes()
    {
        // The bulk-load RPCs share the unmapped-operation posture of the other
        // whole-tree lifecycle verbs (Unknown, so a deny-by-default policy refuses
        // them), but their target tree is still decoded from the request so a
        // per-tree authorizer sees the tree the call targets.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.BeginBulkLoadMethodName),
                new TreeAdminBulkLoadSessionRequest { TreeId = "orders", OperationId = "op" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.AppendBulkLoadMethodName),
                new TreeAdminBulkLoadAppendRequest { TreeId = "orders", OperationId = "op", ChunkIndex = 0 }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.CommitBulkLoadMethodName),
                new TreeAdminBulkLoadSessionRequest { TreeId = "orders", OperationId = "op" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, "orders")));
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_does_not_exempt_the_bulk_load_rpcs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.BeginBulkLoadMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.AppendBulkLoadMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.CommitBulkLoadMethodName)), Is.False);
        });
    }

    [Test]
    public void DescribeCall_decodes_the_target_tree_from_the_restore_request_shapes()
    {
        // The single-tree restore verbs share the Unknown operation posture of the
        // other whole-tree lifecycle verbs (real enforcement is in the facade), but
        // their target tree is still decoded so a per-tree authorizer sees it. The
        // set-level restore targets no single tree, so it decodes to null.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.RestoreTreeMethodName),
                new TreeAdminRestoreRequest { TreeId = "orders", BackupId = "bk", OperationId = "op" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.RevertTreeRestoreMethodName),
                new TreeRestoreResult { TargetTreeId = "orders", BackupId = "bk", OperationId = "op", Mode = TreeRestoreMode.ShadowCutover, ManifestChain = [], EntriesApplied = 0 }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.RestoreTreeSetMethodName),
                new TreeAdminRestoreSetRequest { SetId = "nightly" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, (string?)null)));
        });
    }

    [Test]
    public void DescribeCall_decodes_the_target_tree_from_the_reshard_request_shapes()
    {
        // The reshard trigger and status read share the Unknown operation posture of
        // the other whole-tree lifecycle verbs (real enforcement is in the facade),
        // but their target tree is still decoded so a per-tree authorizer sees it.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.ReshardTreeMethodName),
                new TreeAdminReshardRequest { TreeId = "orders", TargetShardCount = 8 }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, "orders")));

            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
                Method(LatticeTreeAdminGrpcMethods.GetReshardStatusMethodName),
                new TreeAdminTreeRequest { TreeId = "orders" }),
                Is.EqualTo((LatticeTreeAdminApiOperation.Unknown, "orders")));
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_does_not_exempt_the_reshard_rpcs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.ReshardTreeMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.GetReshardStatusMethodName)), Is.False);
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_does_not_exempt_the_restore_rpcs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.RestoreTreeMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.RestoreTreeSetMethodName)), Is.False);
            Assert.That(LatticeTreeAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                Method(LatticeTreeAdminGrpcMethods.RevertTreeRestoreMethodName)), Is.False);
        });
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
