namespace Orleans.Lattice;

/// <summary>
/// The immutable description of a single incoming write presented to an
/// <see cref="ILatticeWriteInterceptor"/> for a pre-commit decision: which tree
/// and key are touched, the incoming value <see cref="Value"/> bytes, the
/// <see cref="LatticeOperation"/> being performed, and the optional
/// <see cref="Ttl"/> the write carries.
/// </summary>
/// <remarks>
/// <para>
/// This is in-process request vocabulary, modelled on
/// <see cref="LatticeAccessRequest"/>. It is passed by <c>in</c> reference on the
/// hot path and is deliberately a <c>struct</c> so a request can be constructed
/// and handed to the (default no-op) interceptor without a heap allocation. It
/// is never persisted or sent on the wire by the core library, so it carries no
/// Orleans serialization attributes.
/// </para>
/// <para>
/// <see cref="Ttl"/> is informational for v1: an interceptor sees the write's
/// time-to-live but a <see cref="LatticeWriteDecision.AcceptTransformed"/>
/// decision may only replace the value bytes, never alter the TTL.
/// </para>
/// </remarks>
public readonly record struct LatticeWriteRequest
{
    /// <summary>
    /// Initializes a new <see cref="LatticeWriteRequest"/>.
    /// </summary>
    /// <param name="treeId">The logical id of the tree the write targets. Must not be <c>null</c> or empty.</param>
    /// <param name="key">The single key being written. Must not be <c>null</c>.</param>
    /// <param name="value">The incoming value bytes. Must not be <c>null</c>.</param>
    /// <param name="operation">The operation being performed (for example <see cref="LatticeOperation.Write"/>).</param>
    /// <param name="ttl">The write's time-to-live, or <c>null</c> when the write does not expire.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> or <paramref name="value"/> is <c>null</c>.</exception>
    public LatticeWriteRequest(
        string treeId,
        string key,
        byte[] value,
        LatticeOperation operation,
        TimeSpan? ttl = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        TreeId = treeId;
        Key = key;
        Value = value;
        Operation = operation;
        Ttl = ttl;
    }

    /// <summary>The logical id of the tree the write targets.</summary>
    public string TreeId { get; init; }

    /// <summary>The single key being written.</summary>
    public string Key { get; init; }

    /// <summary>The incoming value bytes about to be appended to the WAL.</summary>
    public byte[] Value { get; init; }

    /// <summary>The operation being performed.</summary>
    public LatticeOperation Operation { get; init; }

    /// <summary>The write's time-to-live, or <c>null</c> when the write does not expire.</summary>
    public TimeSpan? Ttl { get; init; }
}
