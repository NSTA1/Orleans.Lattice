using ModelContextProtocol;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Typed-CRDT half of the data tool adapter mapping: each method translates a
/// CRDT tool's inputs into the matching strongly-typed <see cref="ILatticeDataApi"/>
/// verb and shapes the facade result into the tool's structured-content DTO. Kept
/// separate from the point / batch mappings in <c>DataToolCore.cs</c> so the CRDT
/// surface stays unit-testable against a fake facade. As elsewhere, no
/// authorization path of its own: a denied write throws; a denied read reads as
/// the empty value for its kind.
/// </summary>
internal static partial class DataToolCore
{
    /// <summary>Maps a PN-counter write onto the increment / decrement facade verb.</summary>
    public static async Task<CrdtWriteToolResult> CounterWriteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CrdtCounterOp operation,
        string replicaId,
        long amount,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var task = operation switch
        {
            CrdtCounterOp.Increment => api.CounterIncrementAsync(treeId, key, replicaId, amount, cancellationToken),
            CrdtCounterOp.Decrement => api.CounterDecrementAsync(treeId, key, replicaId, amount, cancellationToken),
            _ => throw UnknownOperation(nameof(operation)),
        };

        await task.ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.CounterGetAsync"/> onto the counter read result.</summary>
    public static async Task<CrdtCounterToolResult> CounterGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var value = await api.CounterGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtCounterToolResult { TreeId = treeId, Key = key, Value = value };
    }

    /// <summary>Maps an OR-Set write onto the add / remove facade verb.</summary>
    public static async Task<CrdtWriteToolResult> SetWriteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CrdtSetOp operation,
        byte[] element,
        string replicaId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var task = operation switch
        {
            CrdtSetOp.Add => api.SetAddAsync(treeId, key, element, replicaId, cancellationToken),
            CrdtSetOp.Remove => api.SetRemoveAsync(treeId, key, element, cancellationToken),
            _ => throw UnknownOperation(nameof(operation)),
        };

        await task.ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.SetGetAsync"/> onto the elements read result.</summary>
    public static async Task<CrdtElementsToolResult> SetGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var elements = await api.SetGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtElementsToolResult { TreeId = treeId, Key = key, Elements = elements };
    }

    /// <summary>Maps an OR-Flag write onto the enable / disable facade verb.</summary>
    public static async Task<CrdtWriteToolResult> OrFlagWriteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CrdtFlagOp operation,
        string replicaId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var task = operation switch
        {
            CrdtFlagOp.Enable => api.OrFlagEnableAsync(treeId, key, replicaId, cancellationToken),
            CrdtFlagOp.Disable => api.OrFlagDisableAsync(treeId, key, cancellationToken),
            _ => throw UnknownOperation(nameof(operation)),
        };

        await task.ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.OrFlagGetAsync"/> onto the flag read result.</summary>
    public static async Task<CrdtFlagToolResult> OrFlagGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var enabled = await api.OrFlagGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtFlagToolResult { TreeId = treeId, Key = key, Enabled = enabled };
    }

    /// <summary>Maps an RW-Flag write onto the enable / disable facade verb.</summary>
    public static async Task<CrdtWriteToolResult> RwFlagWriteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CrdtFlagOp operation,
        string replicaId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var task = operation switch
        {
            CrdtFlagOp.Enable => api.RwFlagEnableAsync(treeId, key, replicaId, cancellationToken),
            CrdtFlagOp.Disable => api.RwFlagDisableAsync(treeId, key, replicaId, cancellationToken),
            _ => throw UnknownOperation(nameof(operation)),
        };

        await task.ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.RwFlagGetAsync"/> onto the flag read result.</summary>
    public static async Task<CrdtFlagToolResult> RwFlagGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var enabled = await api.RwFlagGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtFlagToolResult { TreeId = treeId, Key = key, Enabled = enabled };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.GCounterIncrementAsync"/> onto the increment write result.</summary>
    public static async Task<CrdtWriteToolResult> GCounterIncrementAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        string replicaId,
        long amount,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        await api.GCounterIncrementAsync(treeId, key, replicaId, amount, cancellationToken).ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.GCounterGetAsync"/> onto the counter read result.</summary>
    public static async Task<CrdtCounterToolResult> GCounterGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var value = await api.GCounterGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtCounterToolResult { TreeId = treeId, Key = key, Value = value };
    }

    /// <summary>Maps a remove-wins (RW) set write onto the add / remove facade verb.</summary>
    public static async Task<CrdtWriteToolResult> RwSetWriteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CrdtRwSetOp operation,
        byte[] element,
        string replicaId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var task = operation switch
        {
            CrdtRwSetOp.Add => api.RwSetAddAsync(treeId, key, element, replicaId, cancellationToken),
            CrdtRwSetOp.Remove => api.RwSetRemoveAsync(treeId, key, element, replicaId, cancellationToken),
            _ => throw UnknownOperation(nameof(operation)),
        };

        await task.ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.RwSetGetAsync"/> onto the elements read result.</summary>
    public static async Task<CrdtElementsToolResult> RwSetGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var elements = await api.RwSetGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtElementsToolResult { TreeId = treeId, Key = key, Elements = elements };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.VersionVectorTickAsync"/> onto the tick write result.</summary>
    public static async Task<CrdtWriteToolResult> VersionVectorTickAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        string replicaId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        await api.VersionVectorTickAsync(treeId, key, replicaId, cancellationToken).ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.VersionVectorGetAsync"/> onto the version-vector read result.</summary>
    public static async Task<CrdtVersionVectorToolResult> VersionVectorGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var entries = await api.VersionVectorGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtVersionVectorToolResult { TreeId = treeId, Key = key, Entries = entries };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.RegisterSetAsync"/> onto the register write result.</summary>
    public static async Task<CrdtWriteToolResult> RegisterSetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        string replicaId,
        byte[] value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        await api.RegisterSetAsync(treeId, key, replicaId, value, cancellationToken).ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.RegisterGetAsync"/> onto the elements read result.</summary>
    public static async Task<CrdtElementsToolResult> RegisterGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var values = await api.RegisterGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtElementsToolResult { TreeId = treeId, Key = key, Elements = values };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.MaxRegisterSetAsync"/> onto the register write result.</summary>
    public static async Task<CrdtWriteToolResult> MaxRegisterSetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        byte[] value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        await api.MaxRegisterSetAsync(treeId, key, value, cancellationToken).ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.MaxRegisterGetAsync"/> onto the elements read result (zero or one element).</summary>
    public static async Task<CrdtElementsToolResult> MaxRegisterGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var value = await api.MaxRegisterGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtElementsToolResult { TreeId = treeId, Key = key, Elements = value is null ? Array.Empty<byte[]>() : new[] { value } };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.MinRegisterSetAsync"/> onto the register write result.</summary>
    public static async Task<CrdtWriteToolResult> MinRegisterSetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        byte[] value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        await api.MinRegisterSetAsync(treeId, key, value, cancellationToken).ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.MinRegisterGetAsync"/> onto the elements read result (zero or one element).</summary>
    public static async Task<CrdtElementsToolResult> MinRegisterGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var value = await api.MinRegisterGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtElementsToolResult { TreeId = treeId, Key = key, Elements = value is null ? Array.Empty<byte[]>() : new[] { value } };
    }

    /// <summary>Maps a Sequence write onto the insert-at / remove-at facade verb.</summary>
    public static async Task<CrdtWriteToolResult> SequenceWriteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CrdtSequenceOp operation,
        int index,
        string replicaId,
        byte[]? value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var task = operation switch
        {
            CrdtSequenceOp.InsertAt => api.SequenceInsertAtAsync(
                treeId, key, index, replicaId, value ?? throw MissingValue(), cancellationToken),
            CrdtSequenceOp.RemoveAt => api.SequenceRemoveAtAsync(treeId, key, index, cancellationToken),
            _ => throw UnknownOperation(nameof(operation)),
        };

        await task.ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.SequenceGetAsync"/> onto the elements read result.</summary>
    public static async Task<CrdtElementsToolResult> SequenceGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var values = await api.SequenceGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtElementsToolResult { TreeId = treeId, Key = key, Elements = values };
    }

    /// <summary>Maps an OR-Map write onto the set / remove facade verb.</summary>
    public static async Task<CrdtWriteToolResult> MapWriteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CrdtMapOp operation,
        string field,
        string replicaId,
        byte[]? value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var task = operation switch
        {
            CrdtMapOp.Set => api.MapSetAsync(
                treeId, key, field, replicaId, value ?? throw MissingValue(), cancellationToken),
            CrdtMapOp.Remove => api.MapRemoveAsync(treeId, key, field, cancellationToken),
            _ => throw UnknownOperation(nameof(operation)),
        };

        await task.ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.MapGetAsync"/> onto the map read result.</summary>
    public static async Task<CrdtMapToolResult> MapGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var fields = await api.MapGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtMapToolResult { TreeId = treeId, Key = key, Fields = fields };
    }

    /// <summary>Maps a G-Set add onto <see cref="ILatticeDataApi.GSetAddAsync"/>. The set is grow-only, so add is the only write.</summary>
    public static async Task<CrdtWriteToolResult> GSetAddAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        byte[] element,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        await api.GSetAddAsync(treeId, key, element, cancellationToken).ConfigureAwait(false);
        return new CrdtWriteToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.GSetGetAsync"/> onto the elements read result.</summary>
    public static async Task<CrdtElementsToolResult> GSetGetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var elements = await api.GSetGetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new CrdtElementsToolResult { TreeId = treeId, Key = key, Elements = elements };
    }

    private static McpException UnknownOperation(string parameterName)
        => new($"The '{parameterName}' parameter names an operation this tool does not support.");

    private static McpException MissingValue()
        => new("The 'value' parameter is required for this operation and must be a base64-encoded byte string.");
}
