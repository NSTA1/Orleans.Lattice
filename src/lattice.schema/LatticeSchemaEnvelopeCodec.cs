namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-versioning <see cref="ILatticeEnvelopeCodec"/>. It is the merge /
/// apply-path complement to <see cref="LatticeSchemaVersionDecoder"/> (the
/// read-boundary decoder): it reports the schema version stamped on a merge input
/// (so the post-merge observer can dispatch a per-record upcaster on the true
/// stored version) and strips the <see cref="LatticeSchemaEnvelope"/> header from a
/// durable CRDT delta so the raw typed-CRDT body can be folded.
/// </summary>
/// <remarks>
/// <para>
/// <b>Strip-only, never upcast.</b> Unlike the read decoder,
/// <see cref="StripForFold"/> performs a purely mechanical, version-agnostic header
/// removal via <see cref="LatticeSchemaEnvelope.StripToBody"/>: it recovers the
/// exact body a producer stamped and never lifts it to a target version. The single
/// version lift of a CRDT delta happens once, at the ingest / apply boundary (the
/// write interceptor), which persists the upcast delta. Every fold thereafter -
/// fresh apply, cold WAL replay, or snapshot-restore projection fold - strips the
/// same durable bytes to the same body, so CRDT convergence and WAL-replay
/// determinism are preserved exactly.
/// </para>
/// <para>
/// <b>Self-dispatch.</b> Like the decoder, the codec is active for every tree once
/// versioning is registered but dispatches on each value's own envelope, so an
/// un-stamped (legacy / unversioned) value reads version <c>0</c> and strips to
/// itself after a single leading-byte check.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaEnvelopeCodec : ILatticeEnvelopeCodec
{
    /// <inheritdoc />
    public bool IsActive(string treeId) => true;

    /// <inheritdoc />
    public uint ReadVersion(byte[]? value)
    {
        if (value is null)
        {
            return 0;
        }

        return LatticeSchemaEnvelope.TryReadHeader(value, out _, out var version) ? version : 0;
    }

    /// <inheritdoc />
    public byte[] StripForFold(byte[] delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        return LatticeSchemaEnvelope.IsEnveloped(delta) ? LatticeSchemaEnvelope.StripToBody(delta) : delta;
    }
}
