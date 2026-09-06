namespace Orleans.Lattice;

/// <summary>
/// A client-side staging token describing a single typed CRDT mutation prepared
/// by a CRDT accessor's <c>Stage*</c> method (for example
/// <see cref="OrFlagAccessor.StageEnableAsync(string, CancellationToken)"/> or
/// <see cref="PnCounterAccessor.StageIncrementAsync(string, long, CancellationToken)"/>)
/// so the mutation can ride a cross-tree atomic write instead of being applied on
/// its own. Add the token to a builder slice via
/// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> on a tree
/// configured with the matching CRDT merge mode; the whole cross-tree batch then
/// commits all-or-nothing.
/// <para>
/// The token carries two byte payloads minted once at stage time from a single
/// read of the current state: <see cref="Value"/> is the <em>merged</em> CRDT
/// state (so local last-writer-wins reads decode the post-mutation value without
/// replaying the delta), and <see cref="Delta"/> is the opaque typed CRDT delta
/// that ships alongside the value so remote clusters fold it through the
/// primitive's <c>MergeDelta</c> and converge. The token is purely client-side and
/// consumed synchronously by the builder; it never crosses the wire, so it carries
/// no Orleans serialization metadata.
/// </para>
/// <para>
/// <b>Caller contract.</b> The <see cref="LatticeAtomicWriteBuilder.ForTree(string)"/>
/// tree a token is added under must be configured with the CRDT merge mode that
/// matches the accessor the token came from (an OR-Set add must be added under an
/// OR-Set-mode tree, a PN-counter increment under a PN-counter-mode tree, and so
/// on): the replication receiver dispatches the delta on the tree's configured
/// mode. The accessor was obtained from that same tree's <see cref="ILattice"/>,
/// so the caller is responsible for keeping the tree ids matching, mirroring the
/// single-key accessor contract.
/// </para>
/// <para>
/// <b>Single-cluster concurrent-writer caveat.</b> <see cref="Value"/> is computed
/// from a snapshot taken at stage time and stored last-writer-wins by HLC at
/// commit; the local cluster never folds the staged delta (only remote clusters
/// do). So two concurrent atomic CRDT writes to the same key in the same cluster
/// last-writer-wins-clobber the locally stored value - one contribution can be
/// missing from a local read - while the typed deltas still converge across
/// clusters. This is the same reconcile / eventual-heals fallback the single-tree
/// saga gives for CRDT keys.
/// </para>
/// <para>
/// <b>Compensation.</b> An aborting saga drops the staged value write and its delta
/// on every cluster (the prepare-phase write never became visible), so no
/// byte-inverse compensation is authored per CRDT family: an abort means the
/// mutation never happened, never an inverse decrement or remove-dot.
/// </para>
/// </summary>
public readonly record struct LatticeStagedCrdtWrite
{
    internal LatticeStagedCrdtWrite(string key, byte[] value, byte[] delta)
    {
        Key = key;
        Value = value;
        Delta = delta;
    }

    /// <summary>The key the staged CRDT mutation targets within its tree.</summary>
    public string Key { get; }

    /// <summary>
    /// The merged CRDT state serialized for the local last-writer-wins value
    /// write, so local reads decode the post-mutation value without replaying
    /// <see cref="Delta"/>.
    /// </summary>
    public byte[] Value { get; }

    /// <summary>
    /// The opaque, already-serialized typed CRDT delta that rides the atomic write
    /// alongside <see cref="Value"/> and is folded by remote clusters for
    /// cross-cluster convergence.
    /// </summary>
    public byte[] Delta { get; }

    /// <summary>
    /// Compares two staged writes by value, with <see cref="Value"/> and
    /// <see cref="Delta"/> compared by content. The compiler-generated
    /// record-struct equality compares each byte array with
    /// <see cref="EqualityComparer{T}.Default"/> - reference equality for a
    /// <see cref="byte"/> array - so two tokens built from independently
    /// allocated but byte-identical payloads would otherwise never compare
    /// equal.
    /// </summary>
    /// <param name="other">The staged write to compare against.</param>
    public bool Equals(LatticeStagedCrdtWrite other) =>
        string.Equals(Key, other.Key, StringComparison.Ordinal)
        && BytesEqual(Value, other.Value)
        && BytesEqual(Delta, other.Delta);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        if (Delta is { } delta)
        {
            hash.AddBytes(delta);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
