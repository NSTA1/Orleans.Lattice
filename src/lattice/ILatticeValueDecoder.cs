namespace Orleans.Lattice;

/// <summary>
/// Pluggable read-path seam that strips or upcasts a per-value envelope
/// from a stored value immediately before the bytes are returned to a
/// client at the public read boundary. Modelled on the
/// <see cref="ILatticeCompressor"/> dispatch and the null-default seam
/// pattern: the core library registers <see cref="NullLatticeValueDecoder"/>
/// (a no-op whose <see cref="IsActive"/> is always <c>false</c>), so with only
/// <c>AddLattice</c> registered the read path is byte-for-byte identical to the
/// pre-seam behaviour - no decode call, no allocation. A companion package
/// (schema enforcement / versioning) replaces it with a real decoder.
/// <para>
/// <b>Store-verbatim invariant.</b> The decoder runs <i>only</i> at the
/// client-facing return boundary of point reads
/// (<c>GetAsync</c> / <c>GetWithVersionAsync</c> / <c>GetManyAsync</c>), range
/// scans (<c>EntriesAsync</c>), and cursor pages (<c>NextEntriesAsync</c>). The
/// stored bytes that flow to snapshots, replication framing, WAL records, and
/// history rows keep their envelope: the envelope stays on the
/// stored/replicated/snapshot/history form and is stripped only on the way out
/// to the caller. This keeps downstream clusters and restore targets coherent
/// (they receive the same enveloped bytes the producer stored) and keeps size
/// accounting / WAL byte-pressure counting the stored (envelope) form, not the
/// decoded form. Internal (system-origin) read paths must not decode.
/// </para>
/// <para>
/// <b>Decode order.</b> The envelope sits <i>outside</i> any per-value
/// compression body, so on read the surrounding layer inflates the compressed
/// body first and this decoder strips the schema envelope afterwards
/// (decompress then schema-decode). In the core read path values are stored
/// uncompressed (core stores opaque <c>byte[]</c>; compression lives at the
/// replication-framing and WAL-segment layers), so the value handed to
/// <see cref="DecodeAsync"/> is already the plain stored (envelope) form.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from multiple
/// threads. <see cref="IsActive"/> is expected to be cheap and stable for a
/// given tree id (it is consulted once per activation and cached), so only
/// opted-in trees pay the per-read decode call.
/// </para>
/// </summary>
public interface ILatticeValueDecoder
{
    /// <summary>
    /// Returns <c>true</c> when this decoder wants to inspect values returned
    /// from the tree identified by <paramref name="treeId"/>. Gates the seam
    /// per tree so a tree with no schema envelope never pays the decode call.
    /// The result must be stable for a given <paramref name="treeId"/> for the
    /// lifetime of the process; consumers cache it per grain activation.
    /// </summary>
    /// <param name="treeId">The tree whose read boundary is being gated.</param>
    /// <returns><c>true</c> to decode values for the tree; <c>false</c> to pass them through verbatim.</returns>
    bool IsActive(string treeId);

    /// <summary>
    /// Strips or upcasts the per-value envelope from a single
    /// <paramref name="storedValue"/> and returns the client-facing bytes.
    /// Invoked only when <see cref="IsActive"/> returned <c>true</c> for
    /// <paramref name="treeId"/> and only for a non-<c>null</c> stored value.
    /// The returned array is handed straight to the caller; implementations may
    /// return <paramref name="storedValue"/> itself when no transformation is
    /// required.
    /// </summary>
    /// <param name="treeId">The tree the value was read from.</param>
    /// <param name="storedValue">The stored (envelope) bytes read from the tree.</param>
    /// <param name="ct">Cancels the decode.</param>
    /// <returns>The decoded client-facing bytes.</returns>
    ValueTask<byte[]> DecodeAsync(string treeId, byte[] storedValue, CancellationToken ct);
}
