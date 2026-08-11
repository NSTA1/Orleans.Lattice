using Grpc.Core;
using Orleans.Lattice.Api.Data.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Typed-CRDT half of the remote data-facade adapter. Every strongly-typed CRDT
/// facade member collapses onto one of the two unified wire RPCs -
/// <see cref="LatticeDataApiGrpcClient.CrdtWriteAsync"/> (discriminated by
/// <see cref="CrdtWriteOp"/>) and <see cref="LatticeDataApiGrpcClient.CrdtReadAsync"/>
/// (discriminated by <see cref="CrdtKind"/>) - and the read members re-project the
/// single carry-all response back to the facade's explicit CLR shape.
/// </summary>
internal sealed partial class GrpcLatticeDataApi
{
    /// <inheritdoc />
    public Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.CounterIncrement, ReplicaId = replicaId, Amount = amount }, cancellationToken);

    /// <inheritdoc />
    public Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.CounterDecrement, ReplicaId = replicaId, Amount = amount }, cancellationToken);

    /// <inheritdoc />
    public async Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.PnCounter, cancellationToken).ConfigureAwait(false)).CounterValue;

    /// <inheritdoc />
    public Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.SetAdd, ReplicaId = replicaId, Element = element }, cancellationToken);

    /// <inheritdoc />
    public Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.SetRemove, Element = element }, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.OrSet, cancellationToken).ConfigureAwait(false)).Elements;

    /// <inheritdoc />
    public Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.OrFlagEnable, ReplicaId = replicaId }, cancellationToken);

    /// <inheritdoc />
    public Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.OrFlagDisable }, cancellationToken);

    /// <inheritdoc />
    public async Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.OrFlag, cancellationToken).ConfigureAwait(false)).FlagValue;

    /// <inheritdoc />
    public Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.RwFlagEnable, ReplicaId = replicaId }, cancellationToken);

    /// <inheritdoc />
    public Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.RwFlagDisable, ReplicaId = replicaId }, cancellationToken);

    /// <inheritdoc />
    public async Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.RwFlag, cancellationToken).ConfigureAwait(false)).FlagValue;

    /// <inheritdoc />
    public Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.GCounterIncrement, ReplicaId = replicaId, Amount = amount }, cancellationToken);

    /// <inheritdoc />
    public async Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.GCounter, cancellationToken).ConfigureAwait(false)).CounterValue;

    /// <inheritdoc />
    public Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.RwSetAdd, ReplicaId = replicaId, Element = element }, cancellationToken);

    /// <inheritdoc />
    public Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.RwSetRemove, ReplicaId = replicaId, Element = element }, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.RwSet, cancellationToken).ConfigureAwait(false)).Elements;

    /// <inheritdoc />
    public Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.VersionVectorTick, ReplicaId = replicaId }, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        var response = await ReadAsync(treeId, key, CrdtKind.VersionVector, cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, string>(response.Vector.Count);
        foreach (var entry in response.Vector)
        {
            result[entry.ReplicaId] = entry.Clock;
        }

        return result;
    }

    /// <inheritdoc />
    public Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.RegisterSet, ReplicaId = replicaId, Element = value }, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.MvRegister, cancellationToken).ConfigureAwait(false)).Elements;

    /// <inheritdoc />
    public Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.MaxRegisterSet, Element = value }, cancellationToken);

    /// <inheritdoc />
    public async Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        var elements = (await ReadAsync(treeId, key, CrdtKind.MaxRegister, cancellationToken).ConfigureAwait(false)).Elements;
        return elements.Count > 0 ? elements[0] : null;
    }

    /// <inheritdoc />
    public Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.MinRegisterSet, Element = value }, cancellationToken);

    /// <inheritdoc />
    public async Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        var elements = (await ReadAsync(treeId, key, CrdtKind.MinRegister, cancellationToken).ConfigureAwait(false)).Elements;
        return elements.Count > 0 ? elements[0] : null;
    }

    /// <inheritdoc />
    public Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.SequenceInsertAt, ReplicaId = replicaId, Element = value, Index = index }, cancellationToken);

    /// <inheritdoc />
    public Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.SequenceRemoveAt, Index = index }, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.Sequence, cancellationToken).ConfigureAwait(false)).Elements;

    /// <inheritdoc />
    public Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.MapSet, ReplicaId = replicaId, Element = value, Field = field }, cancellationToken);

    /// <inheritdoc />
    public Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.MapRemove, Field = field }, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        var response = await ReadAsync(treeId, key, CrdtKind.OrMap, cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, IReadOnlyList<byte[]>>(response.Map.Count);
        foreach (var field in response.Map)
        {
            result[field.Field] = field.Values;
        }

        return result;
    }

    /// <inheritdoc />
    public Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
        => WriteAsync(new CrdtWriteRequest { TreeId = treeId, Key = key, Op = CrdtWriteOp.GSetAdd, Element = element }, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => (await ReadAsync(treeId, key, CrdtKind.GSet, cancellationToken).ConfigureAwait(false)).Elements;

    private async Task WriteAsync(CrdtWriteRequest request, CancellationToken cancellationToken)
    {
        try
        {
            await _client.CrdtWriteAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
        {
            throw Denied(ex);
        }
    }

    private Task<CrdtReadResponse> ReadAsync(string treeId, string key, CrdtKind kind, CancellationToken cancellationToken)
        => _client.CrdtReadAsync(new CrdtReadRequest { TreeId = treeId, Key = key, Kind = kind }, cancellationToken);
}
