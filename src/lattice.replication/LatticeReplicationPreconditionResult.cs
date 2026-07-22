namespace Orleans.Lattice.Replication;

/// <summary>
/// The outcome of a runtime replication precondition check performed by
/// <see cref="ILatticeReplicationPreconditionValidator"/>. A satisfied result
/// means the <c>(treeId, mode)</c> pair is safe to enable given the host's
/// current replication context; a rejected result carries an actionable
/// <see cref="FailureReason"/> the caller can surface to the operator.
/// </summary>
public readonly record struct LatticeReplicationPreconditionResult
{
    /// <summary>
    /// <see langword="true"/> when every runtime precondition holds and the
    /// tree may be enabled under the requested merge mode.
    /// </summary>
    public bool IsSatisfied { get; init; }

    /// <summary>
    /// A human-readable explanation of why the precondition failed, or
    /// <see langword="null"/> when <see cref="IsSatisfied"/> is
    /// <see langword="true"/>.
    /// </summary>
    public string? FailureReason { get; init; }

    /// <summary>A satisfied result with no failure reason.</summary>
    public static LatticeReplicationPreconditionResult Satisfied { get; } =
        new() { IsSatisfied = true, FailureReason = null };

    /// <summary>
    /// Builds a rejected result carrying the supplied actionable
    /// <paramref name="reason"/>.
    /// </summary>
    /// <param name="reason">Why the precondition failed. Must be non-empty.</param>
    /// <returns>A rejected result.</returns>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is <see langword="null"/> or empty.</exception>
    public static LatticeReplicationPreconditionResult Rejected(string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        return new() { IsSatisfied = false, FailureReason = reason };
    }
}
