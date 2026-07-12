namespace Orleans.Lattice;

/// <summary>
/// Minimal core seam that reads the per-value schema-version tag from a stored
/// value and strips the version envelope from a CRDT delta immediately before it
/// is folded. It is the merge / apply-path complement to
/// <see cref="ILatticeValueDecoder"/> (the read-boundary seam): the decoder
/// upcasts a value on its way out to a client, whereas this codec never upcasts -
/// it only reports the stamped version (so the post-merge observer can dispatch a
/// per-record upcaster) and removes the envelope header so the raw typed-CRDT body
/// can be deserialized and folded.
/// </summary>
/// <remarks>
/// <para>
/// <b>Determinism (crown-jewel invariant).</b> <see cref="StripForFold"/> is a
/// pure, <i>version-agnostic</i> header removal: it recovers the exact body bytes
/// a producer stamped, independent of any tree target version, and it never
/// upcasts. The one-and-only version lift of a CRDT delta happens once, at the
/// ingest / apply boundary (the write interceptor), which persists the upcast
/// delta in the WAL. Every later fold - a fresh apply, a cold WAL replay, or a
/// snapshot-restore projection fold - strips the same durable enveloped bytes to
/// the same body and folds them identically, so CRDT convergence and WAL-replay
/// determinism are preserved exactly. Upcasting at fold time (where apply-time and
/// replay-time targets could differ) is forbidden and is precisely what this
/// strip-only contract prevents.
/// </para>
/// <para>
/// <b>Zero overhead when off.</b> The core library registers
/// <see cref="NullLatticeEnvelopeCodec"/> (<see cref="IsActive"/> always
/// <c>false</c>, <see cref="ReadVersion"/> always <c>0</c>,
/// <see cref="StripForFold"/> the identity). With only <c>AddLattice</c>
/// registered every merge / apply path caches an inactive flag per activation and
/// never calls into the codec, so the path is byte-for-byte identical to the
/// pre-seam behaviour with no per-fold allocation. A schema / versioning add-on
/// replaces it with a real, envelope-aware codec.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from multiple threads.
/// <see cref="IsActive"/> is expected to be cheap and stable for a given tree id;
/// it is consulted once per activation and cached.
/// </para>
/// </remarks>
public interface ILatticeEnvelopeCodec
{
    /// <summary>
    /// Returns <c>true</c> when this codec wants to inspect merge inputs and CRDT
    /// deltas for the tree identified by <paramref name="treeId"/>. Gates the seam
    /// per tree so a tree that never carries a version envelope pays nothing. The
    /// result must be stable for a given <paramref name="treeId"/> for the lifetime
    /// of the process; consumers cache it per grain activation.
    /// </summary>
    /// <param name="treeId">The tree whose merge / apply path is being gated.</param>
    /// <returns><c>true</c> to inspect values for the tree; otherwise <c>false</c>.</returns>
    bool IsActive(string treeId);

    /// <summary>
    /// Returns the stamped schema version carried by <paramref name="value"/>, or
    /// <c>0</c> when the value is <c>null</c> or carries no envelope. <c>0</c> is the
    /// reserved "unversioned" sentinel. Invoked only when <see cref="IsActive"/>
    /// returned <c>true</c>. Reads the header only; it never allocates or copies the
    /// body.
    /// </summary>
    /// <param name="value">The stored (possibly enveloped) value bytes, or <c>null</c>.</param>
    /// <returns>The stamped schema version, or <c>0</c> when unversioned.</returns>
    uint ReadVersion(byte[]? value);

    /// <summary>
    /// Strips the per-value version envelope from a CRDT <paramref name="delta"/>,
    /// returning the raw typed-CRDT body to be deserialized and folded. Returns
    /// <paramref name="delta"/> itself (same reference) when it carries no envelope,
    /// so an unversioned tree's deltas are handed through untouched. This operation
    /// is version-agnostic and never upcasts - see the determinism remarks on
    /// <see cref="ILatticeEnvelopeCodec"/>.
    /// </summary>
    /// <param name="delta">The stored (possibly enveloped) CRDT delta bytes.</param>
    /// <returns>The raw typed-CRDT delta body.</returns>
    byte[] StripForFold(byte[] delta);
}
