using System.Reflection;
using System.Runtime.CompilerServices;
using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Asserts that the authorization seam describes every state-API RPC faithfully:
/// each gRPC method maps to its own <see cref="LatticeStateApiOperation"/> and
/// surfaces the tree the call targets, and an unrecognised method falls through
/// to <see cref="LatticeStateApiOperation.Unknown"/> with no target rather than
/// masquerading as a benign catalog operation. This is the seam a host's
/// <c>ILatticeStateApiAuthorizer</c> reasons over, so per-operation /
/// per-tree fidelity here is a security property.
/// </summary>
[TestFixture]
public sealed class LatticeStateApiAuthInterceptorDescribeCallTests
{
    private const string TreeId = "tree-a";

    private static string Method(string methodName) =>
        $"/{LatticeStateGrpcMethods.ServiceName}/{methodName}";

    private static (LatticeStateApiOperation Operation, string? TargetTreeId) Describe<TRequest>(
        string methodName,
        TRequest request) =>
        LatticeStateApiGrpcAuthInterceptor.DescribeCall(Method(methodName), request);

    [Test]
    public void ListTrees_is_cluster_scoped_with_no_target()
    {
        var result = Describe(LatticeStateGrpcMethods.ListTreesMethodName, new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ListTrees));
            Assert.That(result.TargetTreeId, Is.Null);
        });
    }

    [Test]
    public void ListViews_is_cluster_scoped_with_no_target()
    {
        var result = Describe(LatticeStateGrpcMethods.ListViewsMethodName, new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ListViews));
            Assert.That(result.TargetTreeId, Is.Null);
        });
    }

    [Test]
    public void ListTagIndexes_targets_the_subject_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ListTagIndexesMethodName,
            new CatalogRequest { SourceTreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ListTagIndexes));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void ListTagValues_targets_the_subject_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ListTagValuesMethodName,
            new CatalogRequest { SourceTreeId = TreeId, IndexName = "idx" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ListTagValues));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void ListCoveredTrees_is_index_scoped_with_no_single_target()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ListCoveredTreesMethodName,
            new CatalogRequest { IndexName = "idx" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ListCoveredTrees));
            Assert.That(result.TargetTreeId, Is.Null,
                "an index-wide covered-trees listing spans many trees, so it presents no single target");
        });
    }

    [Test]
    public void ListIndexTags_is_index_scoped_with_no_single_target()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ListIndexTagsMethodName,
            new CatalogRequest { IndexName = "idx" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ListIndexTags));
            Assert.That(result.TargetTreeId, Is.Null,
                "an index-wide tag listing spans many trees, so it presents no single target");
        });
    }

    [Test]
    public void ScanTagMembers_is_index_scoped_with_no_single_target()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ScanTagMembersMethodName,
            new TagMemberScanRequest { IndexName = "idx", Tag = "open" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ScanTagMembers));
            Assert.That(result.TargetTreeId, Is.Null,
                "a tag-member scan spans every covered tree, so it presents no single target");
        });
    }

    [Test]
    public void GetTreeStructure_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.GetTreeStructureMethodName,
            new StructureRequest { TreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.GetTreeStructure));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void ScanEntries_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ScanEntriesMethodName,
            new EntryScanRequest { TreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ScanEntries));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void GetEntry_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.GetEntryMethodName,
            new EntryGetRequest { TreeId = TreeId, Key = "k1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.GetEntry));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void GetEntryHistory_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.GetEntryHistoryMethodName,
            new EntryHistoryRequest { TreeId = TreeId, Key = "k1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.GetEntryHistory));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void CancelScan_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.CancelScanMethodName,
            new EntryScanCancelRequest { TreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.CancelScan));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void ObserveChanges_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ObserveChangesMethodName,
            new StateObserveRequest { TreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ObserveChanges));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void ObserveMetrics_for_a_single_tree_surfaces_that_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ObserveMetricsMethodName,
            new TreeMetricsRequest { TreeIds = [TreeId] });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ObserveMetrics));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void ObserveMetrics_for_many_trees_surfaces_no_single_target()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ObserveMetricsMethodName,
            new TreeMetricsRequest { TreeIds = ["a", "b"] });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ObserveMetrics));
            Assert.That(result.TargetTreeId, Is.Null);
        });
    }

    [Test]
    public void GetMetricsSnapshot_for_a_single_tree_surfaces_that_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.GetMetricsSnapshotMethodName,
            new TreeMetricsRequest { TreeIds = [TreeId] });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.GetMetricsSnapshot));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void GetClusterInfo_is_cluster_scoped_with_no_target()
    {
        var result = Describe(LatticeStateGrpcMethods.GetClusterInfoMethodName, new ClusterInfoRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.GetClusterInfo));
            Assert.That(result.TargetTreeId, Is.Null);
        });
    }

    [Test]
    public void GetDeadLetterCount_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.GetDeadLetterCountMethodName,
            new DeadLetterCountRequest { TreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.GetDeadLetterCount));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId),
                "The dead-letter count is read from a caller-named tree, so the authorizer must see that tree.");
        });
    }

    [Test]
    public void ListDeadLetters_targets_its_tree()
    {
        var result = Describe(
            LatticeStateGrpcMethods.ListDeadLettersMethodName,
            new DeadLetterQueueRequest { TreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeStateApiOperation.ListDeadLetters));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId),
                "Dead-letter entries carry the key and a preview of the value bytes, so this "
                + "read must be authorized against the tree it names.");
        });
    }

    [Test]
    public void An_unrecognised_method_maps_to_Unknown_with_no_target()
    {
        var result = Describe("SomeFutureRpc", new CatalogRequest { SourceTreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(
                result.Operation,
                Is.EqualTo(LatticeStateApiOperation.Unknown),
                "An unmapped method must never default to a benign operation.");
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    /// <summary>
    /// Every enforced gRPC method name the service binds, discovered by reflection
    /// rather than restated by hand, so adding an RPC without extending the
    /// operation map fails here instead of silently reaching the authorizer as
    /// <see cref="LatticeStateApiOperation.Unknown"/>. The auth-scheme
    /// advertisement is excluded through the same predicate the interceptor uses,
    /// so the exemption cannot drift apart from the guard.
    /// </summary>
    private static IEnumerable<string> EnforcedBoundMethodNames() =>
        typeof(LatticeStateGrpcMethods)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string)
                && f.Name.EndsWith("MethodName", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(n => !LatticeStateApiGrpcAuthInterceptor.IsUnauthenticatedMethod(Method(n)))
            .OrderBy(n => n, StringComparer.Ordinal);

    /// <summary>
    /// Every bound request type that names a single tree, paired with the method
    /// that carries it. Discovered from the strongly typed
    /// <see cref="Method{TRequest, TResponse}"/> properties, so a new tree-scoped
    /// RPC is enrolled in the guard automatically.
    /// </summary>
    private static IEnumerable<TestCaseData> BoundTreeScopedMethods() =>
        typeof(LatticeStateGrpcMethods)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType.IsGenericType
                && p.PropertyType.GetGenericTypeDefinition() == typeof(Method<,>))
            .Select(p => (MethodName: p.Name, RequestType: p.PropertyType.GetGenericArguments()[0]))
            .Where(x => x.RequestType.GetProperty("TreeId")?.PropertyType == typeof(string))
            .OrderBy(x => x.MethodName, StringComparer.Ordinal)
            .Select(x => new TestCaseData(x.MethodName, x.RequestType).SetArgDisplayNames(x.MethodName));

    [TestCaseSource(nameof(EnforcedBoundMethodNames))]
    public void Every_enforced_bound_method_maps_to_a_known_operation(string methodName)
    {
        var result = Describe(methodName, new object());

        Assert.That(
            result.Operation,
            Is.Not.EqualTo(LatticeStateApiOperation.Unknown),
            $"'{methodName}' is a bound, enforced RPC but is missing from the operation map, so an "
            + "authorizer cannot make a per-operation decision about it. Add an arm to "
            + "LatticeStateApiGrpcAuthInterceptor.DescribeCall.");
    }

    [TestCaseSource(nameof(BoundTreeScopedMethods))]
    public void Every_bound_tree_scoped_method_surfaces_its_target_tree(string methodName, Type requestType)
    {
        var request = RuntimeHelpers.GetUninitializedObject(requestType);
        requestType.GetProperty("TreeId")!.SetValue(request, TreeId);

        var result = Describe(methodName, request);

        Assert.That(
            result.TargetTreeId,
            Is.EqualTo(TreeId),
            $"'{methodName}' carries a caller-supplied TreeId but DescribeCall reports no target. "
            + "A null target means 'not scoped to a single tree', so an authorizer that restricts a "
            + "caller to a set of trees would skip its tree check entirely. Add an arm to the "
            + "target switch in LatticeStateApiGrpcAuthInterceptor.DescribeCall.");
    }
}
