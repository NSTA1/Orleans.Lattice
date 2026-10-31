using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeDataApi"/> over gRPC: a per-circuit adapter over
/// <see cref="LatticeDataApiGrpcClient"/>, so the Data area writes, deletes and
/// probes its write rights as the circuit's signed-in user. Faults map through
/// <see cref="ShellTransportFaults"/>: a denied write arrives as the facade's
/// <see cref="LatticeAuthorizationDeniedException"/> and a value the tree's schema
/// policy refuses as a <see cref="ShellValueValidationException"/>.
/// </summary>
/// <remarks>
/// The typed-CRDT members, which collapse onto the binding's two unified CRDT
/// RPCs, are in <c>ShellDataTransport.Crdt.cs</c>.
/// </remarks>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed partial class ShellDataTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeDataApiGrpcClient>(channel, LatticeDataApiGrpcClient.Create), ILatticeDataApi
{
    /// <inheritdoc />
    public Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        return CallAsync(
            new DataSetRequest { TreeId = treeId, Key = key, Value = value },
            static (client, request, ct) => client.SetAsync(request, ct),
            treeId,
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        var response = await CallAsync(
            new DataDeleteRequest { TreeId = treeId, Key = key },
            static (client, request, ct) => client.DeleteAsync(request, ct),
            treeId,
            cancellationToken).ConfigureAwait(false);
        return response.Removed;
    }

    /// <inheritdoc />
    public Task<DataRangeDeleteResult> DeleteRangeAsync(DataRangeDeleteRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.DeleteRangeAsync(state, ct), request.TreeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetManyAtomicAsync(string treeId, DataAtomicBatch batch, string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(batch);
        return CallAsync(
            new DataAtomicRequest { TreeId = treeId, Batch = batch, OperationId = operationId },
            static (client, request, ct) => client.SetManyAtomicAsync(request, ct),
            treeId,
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(
        IReadOnlyList<DataTreeBatch> batches,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batches);
        var response = await CallAsync(
            new DataCrossTreeRequest { Batches = batches as List<DataTreeBatch> ?? [.. batches], OperationId = operationId },
            static (client, request, ct) => client.SetManyAtomicCrossTreeAsync(request, ct),
            null,
            cancellationToken).ConfigureAwait(false);
        return response.Outcome;
    }

    /// <inheritdoc />
    public Task SetManyAsync(string treeId, IReadOnlyList<DataEntry> upserts, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(upserts);
        return CallAsync(
            new DataSetManyRequest { TreeId = treeId, Upserts = upserts as List<DataEntry> ?? [.. upserts] },
            static (client, request, ct) => client.SetManyAsync(request, ct),
            treeId,
            cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Streams over the binding's single duplex <c>StreamSetMany</c> call on the circuit's
    /// channel, through the same client every other verb uses, rather than one unary call
    /// per batch.
    /// </remarks>
    public Task<DataSetManyStreamResult> SetManyStreamAsync(
        string treeId,
        IAsyncEnumerable<DataEntry> upserts,
        IProgress<DataSetManyStreamBatch>? progress = null,
        DataSetManyStreamOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(upserts);
        return CallAsync(
            (TreeId: treeId, Upserts: upserts, Progress: progress, Options: options),
            static (client, state, ct) => client.SetManyStreamAsync(state.TreeId, state.Upserts, state.Progress, state.Options, ct),
            treeId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        return CallAsync(
            new DataGetRequest { TreeId = treeId, Key = key },
            static (client, request, ct) => client.GetAsync(request, ct),
            treeId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.ReadRangeAsync(state, ct), request.TreeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeDataCapabilities> ProbeCapabilitiesAsync(string treeId, string? key = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            new DataProbeRequest { TreeId = treeId, Key = key },
            static (client, request, ct) => client.ProbeCapabilitiesAsync(request, ct),
            treeId,
            cancellationToken);
    }
}
