using Grpc.Core;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the read-write data facade
/// (<see cref="ILatticeDataApi"/>) by delegating to the data-API gRPC client
/// (<see cref="LatticeDataApiGrpcClient"/>), so the topology-agnostic data tool
/// module works unchanged against a cluster reached over gRPC.
/// </summary>
/// <remarks>
/// The gRPC binding signals a denied write as a <c>PermissionDenied</c>
/// <see cref="RpcException"/>; the facade contract is a
/// <see cref="LatticeAuthorizationDeniedException"/>. The four mutating members
/// translate the former to the latter so a denied write surfaces the same typed
/// failure - with nothing persisted - as in-silo. The two read members never
/// throw on denial: an unreadable key reports absent and a range read prunes to
/// the authorized subset, exactly as the wire contract already returns.
/// </remarks>
internal sealed class GrpcLatticeDataApi : ILatticeDataApi
{
    private readonly LatticeDataApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied data-API gRPC client.</summary>
    public GrpcLatticeDataApi(LatticeDataApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public async Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.SetAsync(
                new DataSetRequest { TreeId = treeId, Key = key, Value = value },
                cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
        {
            throw Denied(ex);
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _client.DeleteAsync(
                new DataDeleteRequest { TreeId = treeId, Key = key },
                cancellationToken).ConfigureAwait(false);
            return response.Removed;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
        {
            throw Denied(ex);
        }
    }

    /// <inheritdoc />
    public async Task SetManyAtomicAsync(
        string treeId,
        DataAtomicBatch batch,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.SetManyAtomicAsync(
                new DataAtomicRequest { TreeId = treeId, Batch = batch, OperationId = operationId },
                cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
        {
            throw Denied(ex);
        }
    }

    /// <inheritdoc />
    public async Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(
        IReadOnlyList<DataTreeBatch> batches,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batches);

        var wire = new List<DataTreeBatch>(batches.Count);
        for (var i = 0; i < batches.Count; i++)
        {
            wire.Add(batches[i]);
        }

        try
        {
            var response = await _client.SetManyAtomicCrossTreeAsync(
                new DataCrossTreeRequest { Batches = wire, OperationId = operationId },
                cancellationToken).ConfigureAwait(false);
            return response.Outcome;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
        {
            throw Denied(ex);
        }
    }

    /// <inheritdoc />
    public Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => _client.GetAsync(new DataGetRequest { TreeId = treeId, Key = key }, cancellationToken);

    /// <inheritdoc />
    public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)
        => _client.ReadRangeAsync(request, cancellationToken);

    private static LatticeAuthorizationDeniedException Denied(RpcException ex)
        => new(ex.Status.Detail, ex);
}
