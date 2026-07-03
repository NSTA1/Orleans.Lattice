namespace Orleans.Lattice;

/// <summary>
/// The immutable description of a single data-plane call presented to an
/// <see cref="ILatticeAccessGate"/> for an authorization decision: which tree
/// and key(s) are touched, what <see cref="LatticeOperation"/> is being
/// attempted, and the resolved <see cref="LatticeSubject"/> making the call.
/// </summary>
/// <remarks>
/// <para>
/// This is in-process request vocabulary. It is passed by <c>in</c> reference
/// on the hot path and is deliberately a <c>struct</c> so a request can be
/// constructed and handed to the (default no-op) gate without a heap
/// allocation. It is never persisted or sent on the wire by the core library,
/// so it carries no Orleans serialization attributes.
/// </para>
/// <para>
/// <see cref="Key"/> is set for single-key shapes (read / write / delete /
/// CRDT apply); <see cref="RangeStart"/> and <see cref="RangeEnd"/> are set for
/// range shapes; a lifecycle / bulk shape may leave all three <c>null</c>.
/// </para>
/// </remarks>
public readonly record struct LatticeAccessRequest
{
    /// <summary>
    /// Initializes a new <see cref="LatticeAccessRequest"/>.
    /// </summary>
    /// <param name="treeId">The logical id of the tree the call targets. Must not be <c>null</c> or empty.</param>
    /// <param name="operation">The operation being attempted.</param>
    /// <param name="subject">The resolved caller identity, or <see cref="LatticeSubject.Anonymous"/>.</param>
    /// <param name="key">The single key touched, or <c>null</c> for range / lifecycle shapes.</param>
    /// <param name="rangeStart">The inclusive start of the key range, or <c>null</c> when not a range shape.</param>
    /// <param name="rangeEnd">The exclusive / inclusive end of the key range, or <c>null</c> when not a range shape.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public LatticeAccessRequest(
        string treeId,
        LatticeOperation operation,
        LatticeSubject subject,
        string? key = null,
        string? rangeStart = null,
        string? rangeEnd = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        TreeId = treeId;
        Operation = operation;
        Subject = subject;
        Key = key;
        RangeStart = rangeStart;
        RangeEnd = rangeEnd;
    }

    /// <summary>The logical id of the tree the call targets.</summary>
    public string TreeId { get; init; }

    /// <summary>The operation being attempted.</summary>
    public LatticeOperation Operation { get; init; }

    /// <summary>The single key touched, or <c>null</c> for range / lifecycle shapes.</summary>
    public string? Key { get; init; }

    /// <summary>The inclusive start of the key range, or <c>null</c> when not a range shape.</summary>
    public string? RangeStart { get; init; }

    /// <summary>The end of the key range, or <c>null</c> when not a range shape.</summary>
    public string? RangeEnd { get; init; }

    /// <summary>The resolved caller identity, or <see cref="LatticeSubject.Anonymous"/>.</summary>
    public LatticeSubject Subject { get; init; }
}
