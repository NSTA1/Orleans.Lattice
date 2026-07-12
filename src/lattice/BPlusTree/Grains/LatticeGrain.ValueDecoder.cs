using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Read-path value-decoder wiring for the <see cref="LatticeGrain"/>
/// client-facing boundary. Resolves the registered
/// <see cref="ILatticeValueDecoder"/> once per activation, gates it per tree via
/// <see cref="ILatticeValueDecoder.IsActive"/>, and strips the per-value
/// envelope from values on their way out to the caller.
/// </summary>
/// <remarks>
/// <para>
/// <b>Store-verbatim invariant.</b> Decoding happens <i>only</i> at the
/// client-facing return boundary of the public read surfaces (point read,
/// range scan, cursor page). The stored bytes that flow to snapshots,
/// replication framing, WAL records, and history rows keep their envelope, and
/// the <c>ISystemLattice</c> read paths (used by replication-apply, saga legs,
/// and maintenance) never route through this wiring - so downstream clusters
/// and restore targets stay coherent and WAL byte-pressure accounts the stored
/// (envelope) form. The envelope is stripped only here, per read, for the tree
/// the client asked for.
/// </para>
/// <para>
/// <b>Decode order.</b> The schema envelope sits outside any per-value
/// compression body, so on read the surrounding layer inflates the compressed
/// body first and this decoder strips the envelope afterwards. In the core read
/// path values are stored uncompressed (compression lives at the
/// replication-framing and WAL-segment layers, not per value), so the bytes
/// handed to the decoder here are already the plain stored (envelope) form.
/// </para>
/// <para>
/// <b>Zero-cost default.</b> With only <c>AddLattice</c> registered the decoder
/// is <see cref="NullLatticeValueDecoder"/>, whose <see cref="ILatticeValueDecoder.IsActive"/>
/// is always <c>false</c>. <see cref="ValueDecoderActive"/> resolves the decoder
/// and evaluates the per-tree gate exactly once per activation and caches the
/// result, so every read short-circuits on a cached <c>bool</c> and the default
/// path is byte-for-byte identical to the pre-seam behaviour with no per-read
/// decode call and no allocation.
/// </para>
/// </remarks>
internal sealed partial class LatticeGrain
{
    private ILatticeValueDecoder? _valueDecoder;
    private bool _valueDecoderResolved;
    private bool _valueDecoderActive;

    /// <summary>
    /// <c>true</c> when a registered <see cref="ILatticeValueDecoder"/> opted
    /// into this activation's tree. Resolved and gated once per activation
    /// (the tree id is fixed for the activation and <c>IsActive</c> is stable
    /// per tree), then cached, so the read boundaries pay only a cached
    /// <c>bool</c> check on the hot path.
    /// </summary>
    private bool ValueDecoderActive
    {
        get
        {
            if (!_valueDecoderResolved)
            {
                _valueDecoder = services.GetService<ILatticeValueDecoder>();
                // Null default: NullLatticeValueDecoder.IsActive is always
                // false, so an unregistered / null-default decoder resolves
                // inactive and no read ever calls DecodeAsync.
                _valueDecoderActive = _valueDecoder is not null && _valueDecoder.IsActive(TreeId);
                _valueDecoderResolved = true;
            }
            return _valueDecoderActive;
        }
    }

    /// <summary>
    /// Strips the per-value envelope from a single stored value when the tree's
    /// decoder is active. Pass-through (returns <paramref name="storedValue"/>
    /// verbatim) for a <c>null</c> value. Callers gate on
    /// <see cref="ValueDecoderActive"/> before awaiting this so the inactive
    /// path never enters the method.
    /// </summary>
    private async Task<byte[]?> DecodeValueAsync(byte[]? storedValue, CancellationToken cancellationToken)
    {
        if (storedValue is null)
        {
            return null;
        }

        return await _valueDecoder!.DecodeAsync(TreeId, storedValue, cancellationToken);
    }

    /// <summary>
    /// Awaits <paramref name="stored"/> (a point-read core call) and strips the
    /// per-value envelope from its result. Used by the public point-read
    /// surfaces on the active-decoder branch so the inactive branch can return
    /// the core task directly with no added state machine.
    /// </summary>
    private async Task<byte[]?> DecodePointReadAsync(Task<byte[]?> stored, CancellationToken cancellationToken)
    {
        var value = await stored;
        return await DecodeValueAsync(value, cancellationToken);
    }

    /// <summary>
    /// Strips the per-value envelope from every value of a multi-key point-read
    /// result in place. Only invoked on the active-decoder branch, so the key
    /// snapshot it allocates is never paid on the default null-decoder path.
    /// </summary>
    private async Task DecodeManyInPlaceAsync(Dictionary<string, byte[]> entries, CancellationToken cancellationToken)
    {
        // Snapshot the keys so the dictionary can be rewritten in place while
        // iterating (mutating values during a live key enumeration is illegal).
        var keys = new string[entries.Count];
        entries.Keys.CopyTo(keys, 0);
        foreach (var key in keys)
        {
            entries[key] = await _valueDecoder!.DecodeAsync(TreeId, entries[key], cancellationToken);
        }
    }
}
