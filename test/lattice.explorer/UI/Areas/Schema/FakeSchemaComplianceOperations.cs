using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.Tests.UI.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>A scripted <see cref="ILatticeSchemaComplianceOperations"/>: a start registers a queued scan a test then drives by hand.</summary>
internal sealed class FakeSchemaComplianceOperations() : FakeOperations(SchemaComplianceScanOperation.Kind), ILatticeSchemaComplianceOperations
{
    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartComplianceScanAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default) =>
        StartAsync(nameof(StartComplianceScanAsync), operationId, treeId);

    /// <summary>Finishes scan <paramref name="operationId"/> as succeeded with <paramref name="report"/>.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="report">The report.</param>
    public void Succeed(string operationId, LatticeSchemaComplianceReport report) =>
        Succeed(operationId, SchemaComplianceScanResults.ToResultMap(report));
}
