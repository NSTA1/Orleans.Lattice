namespace Orleans.Lattice;

/// <summary>
/// The immutable inputs handed to an <see cref="ILatticeMergeObserver"/> after
/// a per-key merge completes: the key, the declared
/// <see cref="LatticeMergeMode"/> for the record (taken from the durable
/// <c>WalRecord.Mode</c>), the decoded merge inputs and result, the
/// <see cref="TreeId"/> the merge occurred on, and the per-record durable schema
/// versions (<see cref="LocalVersion"/> / <see cref="IncomingVersion"/>) a schema
/// observer dispatches a per-record upcaster on.
/// </summary>
/// <remarks>
/// <para>
/// This is an in-process request value passed by <c>in</c> reference; it never
/// crosses a grain boundary and therefore carries no Orleans serialization
/// attributes (mirroring <see cref="LatticeAccessDecision"/> /
/// <see cref="LatticeAccessRequest"/>). It is a plain <c>readonly struct</c> so
/// constructing it on the merge path allocates nothing on the heap.
/// </para>
/// <para>
/// The <see cref="LocalValue"/> / <see cref="IncomingValue"/> / <see cref="MergedValue"/>
/// byte arrays are the <b>decoded</b> (envelope-stripped) forms - the observer
/// reasons about logical values, not stored envelopes. When no
/// <see cref="ILatticeValueDecoder"/> is active for the tree, the decoded form
/// equals the stored form. For a typed CRDT merge (delta-based apply) the
/// <see cref="IncomingValue"/> may be <c>null</c> because the incoming change is
/// a delta rather than a full value; <see cref="LocalValue"/> is <c>null</c>
/// when the key had no prior value.
/// </para>
/// </remarks>
public readonly struct LatticeMergeContext
{
    /// <summary>
    /// Constructs a merge context for the observer.
    /// </summary>
    /// <param name="key">The key whose value was merged.</param>
    /// <param name="mode">The declared merge mode for the record (from <c>WalRecord.Mode</c>).</param>
    /// <param name="localValue">The decoded prior (local) value, or <c>null</c> when the key had no prior value.</param>
    /// <param name="incomingValue">The decoded incoming value, or <c>null</c> when the change was a typed delta.</param>
    /// <param name="mergedValue">The decoded canonical merged result.</param>
    /// <param name="treeId">The id of the tree the merge occurred on, or <c>null</c> when the leaf has no tree id.</param>
    /// <param name="localVersion">The durable schema version stamped on the local input, or <c>0</c> when unversioned.</param>
    /// <param name="incomingVersion">The durable schema version stamped on the incoming input / delta, or <c>0</c> when unversioned.</param>
    public LatticeMergeContext(
        string key,
        LatticeMergeMode mode,
        byte[]? localValue,
        byte[]? incomingValue,
        byte[] mergedValue,
        string? treeId = null,
        uint localVersion = 0,
        uint incomingVersion = 0)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(mergedValue);
        Key = key;
        Mode = mode;
        LocalValue = localValue;
        IncomingValue = incomingValue;
        MergedValue = mergedValue;
        TreeId = treeId;
        LocalVersion = localVersion;
        IncomingVersion = incomingVersion;
    }

    /// <summary>The key whose value was merged.</summary>
    public string Key { get; }

    /// <summary>
    /// The id of the tree the merge occurred on, stamped from the leaf's durable
    /// state so the post-merge observer can resolve the tree's schema / version
    /// config directly from the context rather than an ambient scope. <c>null</c>
    /// when the leaf has no tree id (unit tests that construct a context directly).
    /// </summary>
    public string? TreeId { get; }

    /// <summary>
    /// The durable schema version stamped on <see cref="LocalValue"/>'s stored
    /// (enveloped) form, or <c>0</c> when the local input was unversioned or absent.
    /// Mirrors the durable <c>WalRecord.Mode</c> discipline (a per-record datum the
    /// merge path threads through) so a schema observer can dispatch a per-record
    /// upcaster on the true stored version rather than guessing.
    /// </summary>
    public uint LocalVersion { get; }

    /// <summary>
    /// The durable schema version stamped on the incoming input - for an LWW merge
    /// the stored version of <see cref="IncomingValue"/>, for a CRDT merge the
    /// version of the incoming delta - or <c>0</c> when the incoming change was
    /// unversioned.
    /// </summary>
    public uint IncomingVersion { get; }

    /// <summary>
    /// The declared convergence rule for the record, taken from the durable
    /// <c>WalRecord.Mode</c>. Drives the transform-permission check: only
    /// <see cref="LatticeMergeMode.LwwRegister"/> may be rewritten via
    /// <see cref="MergeOutcomeKind.AcceptTransformed"/>.
    /// </summary>
    public LatticeMergeMode Mode { get; }

    /// <summary>
    /// The decoded prior (local) value the merge folded the incoming change
    /// into, or <c>null</c> when the key had no prior value.
    /// </summary>
    public byte[]? LocalValue { get; }

    /// <summary>
    /// The decoded incoming value, or <c>null</c> when the incoming change was
    /// a typed CRDT delta rather than a full value.
    /// </summary>
    public byte[]? IncomingValue { get; }

    /// <summary>The decoded canonical merged result the grain has stored.</summary>
    public byte[] MergedValue { get; }
}
