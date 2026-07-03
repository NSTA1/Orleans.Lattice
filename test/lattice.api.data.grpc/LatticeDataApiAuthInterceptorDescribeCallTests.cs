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
    public void An_unrecognised_method_maps_to_Unknown()
    {
        var result = Describe("SomeFutureRpc", new DataSetRequest { TreeId = TreeId, Key = "k1" });

        Assert.That(
            result.Operation,
            Is.EqualTo(LatticeDataApiOperation.Unknown),
            "An unmapped method must never default to a benign operation.");
    }
}
