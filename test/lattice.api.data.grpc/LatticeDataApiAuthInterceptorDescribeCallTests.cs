using System.Reflection;
using System.Runtime.CompilerServices;
using Grpc.Core;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Asserts that the authorization seam describes every data-API RPC faithfully:
/// each gRPC method maps to its own <see cref="LatticeDataApiOperation"/> and
/// surfaces the tree the call targets, the cross-tree batch presents no single
/// target (it spans several trees), and an unrecognised method falls through to
/// <see cref="LatticeDataApiOperation.Unknown"/> rather than masquerading as a
/// benign operation. This is the seam a host's
/// <see cref="ILatticeDataApiAuthorizer"/> reasons over, so per-operation /
/// per-tree fidelity here is a security property.
/// </summary>
[TestFixture]
public sealed class LatticeDataApiAuthInterceptorDescribeCallTests
{
    private const string TreeId = "tree-a";

    private static string Method(string methodName) =>
        $"/{LatticeDataApiGrpcMethods.ServiceName}/{methodName}";

    private static (LatticeDataApiOperation Operation, string? TargetTreeId) Describe<TRequest>(
        string methodName,
        TRequest request) =>
        LatticeDataApiGrpcAuthInterceptor.DescribeCall(Method(methodName), request);

    [Test]
    public void Set_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.SetMethodName,
            new DataSetRequest { TreeId = TreeId, Key = "k1", Value = [1] });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.SetPoint));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void Delete_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.DeleteMethodName,
            new DataDeleteRequest { TreeId = TreeId, Key = "k1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.DeletePoint));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void SetManyAtomic_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.SetManyAtomicMethodName,
            new DataAtomicRequest { TreeId = TreeId, OperationId = "op-1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.SetManyAtomic));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void SetManyAtomicCrossTree_spans_many_trees_with_no_single_target()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.SetManyAtomicCrossTreeMethodName,
            new DataCrossTreeRequest
            {
                OperationId = "xt-1",
                Batches = [new DataTreeBatch { TreeId = "a" }, new DataTreeBatch { TreeId = "b" }],
            });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.SetManyAtomicCrossTree));
            Assert.That(result.TargetTreeId, Is.Null,
                "a cross-tree atomic batch spans several trees, so it presents no single target");
        });
    }

    [Test]
    public void Get_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.GetMethodName,
            new DataGetRequest { TreeId = TreeId, Key = "k1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.GetPoint));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void ReadRange_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.ReadRangeMethodName,
            new DataRangeRequest { TreeId = TreeId, PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.ReadRange));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void DeleteRange_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.DeleteRangeMethodName,
            new DataRangeDeleteRequest { TreeId = TreeId, StartInclusive = "a", EndExclusive = "z" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.DeleteRange));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId));
        });
    }

    [Test]
    public void SetMany_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.SetManyMethodName,
            new DataSetManyRequest { TreeId = TreeId });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.SetMany));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId),
                "SetMany writes to a caller-named tree, so the authorizer must see that tree.");
        });
    }

    [Test]
    public void CrdtWrite_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.CrdtWriteMethodName,
            new CrdtWriteRequest { TreeId = TreeId, Key = "k1", Op = CrdtWriteOp.CounterIncrement });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.CrdtWrite));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId),
                "CrdtWrite mutates a caller-named tree, so the authorizer must see that tree.");
        });
    }

    [Test]
    public void CrdtRead_targets_its_tree()
    {
        var result = Describe(
            LatticeDataApiGrpcMethods.CrdtReadMethodName,
            new CrdtReadRequest { TreeId = TreeId, Key = "k1", Kind = CrdtKind.PnCounter });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeDataApiOperation.CrdtRead));
            Assert.That(result.TargetTreeId, Is.EqualTo(TreeId),
                "CrdtRead reads a caller-named tree, so the authorizer must see that tree.");
        });
    }

    [Test]
    public void An_unrecognised_method_maps_to_Unknown()
    {
        var result = Describe("SomeFutureRpc", new DataSetRequest { TreeId = TreeId, Key = "k1" });

        Assert.That(
            result.Operation,
            Is.EqualTo(LatticeDataApiOperation.Unknown),
            "An unmapped method must never default to a benign operation.");
    }

    /// <summary>
    /// Every gRPC method name the service binds, discovered by reflection rather
    /// than restated by hand, so adding an RPC without extending the operation map
    /// fails here instead of silently reaching the authorizer as
    /// <see cref="LatticeDataApiOperation.Unknown"/>.
    /// </summary>
    private static IEnumerable<string> BoundMethodNames() =>
        typeof(LatticeDataApiGrpcMethods)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string)
                && f.Name.EndsWith("MethodName", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .OrderBy(n => n, StringComparer.Ordinal);

    /// <summary>
    /// Every bound request type that names a single tree, paired with the method
    /// that carries it. Discovered from the strongly typed
    /// <see cref="Method{TRequest, TResponse}"/> properties, so a new tree-scoped
    /// RPC is enrolled in the guard automatically.
    /// </summary>
    private static IEnumerable<TestCaseData> BoundTreeScopedMethods() =>
        typeof(LatticeDataApiGrpcMethods)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType.IsGenericType
                && p.PropertyType.GetGenericTypeDefinition() == typeof(Method<,>))
            .Select(p => (MethodName: p.Name, RequestType: p.PropertyType.GetGenericArguments()[0]))
            .Where(x => x.RequestType.GetProperty("TreeId")?.PropertyType == typeof(string))
            .OrderBy(x => x.MethodName, StringComparer.Ordinal)
            .Select(x => new TestCaseData(x.MethodName, x.RequestType).SetArgDisplayNames(x.MethodName));

    [TestCaseSource(nameof(BoundMethodNames))]
    public void Every_bound_method_maps_to_a_known_operation(string methodName)
    {
        var result = Describe(methodName, new object());

        Assert.That(
            result.Operation,
            Is.Not.EqualTo(LatticeDataApiOperation.Unknown),
            $"'{methodName}' is a bound RPC but is missing from the operation map, so an "
            + "authorizer cannot make a per-operation decision about it. Add an arm to "
            + "LatticeDataApiGrpcAuthInterceptor.DescribeCall.");
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
            + "target switch in LatticeDataApiGrpcAuthInterceptor.DescribeCall.");
    }
}
