namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>ShardRootGrain</c> when its one-time activation-readiness
/// seed exceeds the per-tree
/// <see cref="Orleans.Lattice.BPlusTree.LatticeOptions.ActivationReadyTimeout"/>
/// deadline (default 15 seconds). The seed runs the chain of cross-grain
/// awaits a brand-new or freshly-reactivated shard performs the first time
/// it prepares for an operation: the defensive state re-read, the
/// tree-registry registration, the deterministic root-leaf init pair, and
/// the initial shard-state write. During a startup reshard or membership
/// change Orleans can park one of those messages and the seed is abandoned
/// by the deadline; this exception surfaces that abandonment.
/// <para>
/// <b>The exception is retriable.</b> Every cross-grain step in the seed is
/// idempotent on retry: the registry registration by contract, the leaf-init
/// pair by its cycle-1 guard, the shard-state write by the
/// re-read-and-recheck at the top of the slow path. A caller that observes
/// this exception should re-invoke the same operation after a short backoff;
/// the next attempt runs against refreshed routing once the cold-start race
/// has settled. The public <see cref="ILattice"/> operators that drive the
/// seed transparently absorb a small bounded number of these and re-issue
/// against the same grain, so external callers normally do not see the
/// exception. It is exposed publicly as a typed surface for code that wants
/// to detect it explicitly (e.g. host-startup orchestrators that want to
/// stamp a custom log line, or test harnesses that want to assert on the
/// retry path).
/// </para>
/// <para>
/// Derives from <see cref="System.TimeoutException"/> so existing catch
/// handlers that match on <see cref="System.TimeoutException"/> continue to
/// work; the typed slots (<see cref="TreeId"/>, <see cref="ShardIndex"/>,
/// <see cref="TimeoutSeconds"/>) carry the per-occurrence attribution.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardActivationTimeout)]
public sealed class ShardActivationTimeoutException : TimeoutException
{
    /// <summary>
    /// Initialises a new instance with no diagnostic context. Provided to
    /// satisfy the framework's exception construction contract; production
    /// throw sites use the message + inner-exception overload.
    /// </summary>
    public ShardActivationTimeoutException() { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message.
    /// </summary>
    public ShardActivationTimeoutException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception (typically the underlying
    /// <see cref="System.OperationCanceledException"/> raised when the
    /// deadline fired).
    /// </summary>
    public ShardActivationTimeoutException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>
    /// The tree whose shard-root seed was abandoned.
    /// </summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The physical shard index whose seed was abandoned.
    /// </summary>
    [Id(1)] public int ShardIndex { get; set; }

    /// <summary>
    /// The per-attempt deadline that fired, expressed in seconds for
    /// wire-format stability across hosts whose <see cref="System.TimeSpan"/>
    /// serialisation might differ.
    /// </summary>
    [Id(2)] public double TimeoutSeconds { get; set; }
}
