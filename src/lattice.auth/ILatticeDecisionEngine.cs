namespace Orleans.Lattice.Auth;

/// <summary>
/// The per-silo authorization decision engine: evaluates a request against the
/// compiled policy snapshot and returns an in-memory
/// <see cref="LatticeAccessDecision"/>. Evaluation is synchronous and does no
/// storage I/O once the snapshot is warm; the snapshot is refreshed off the
/// change-feed by the policy snapshot maintainer, so decisions reflect committed
/// policy edits without a restart (eventual snapshot consistency).
/// </summary>
/// <remarks>
/// <para>
/// This engine is a decision surface only. Registering it does not wire
/// enforcement: the core access gate stays the default no-op, and nothing on the
/// data path consults the engine until a later feature wires it in.
/// </para>
/// <para>
/// <see cref="Evaluate"/> consumes the subject's already-transitively-expanded
/// group closure (<see cref="LatticeSubject.GroupIds"/>) and never re-walks group
/// nesting, so a decision stays proportional to the number of the subject's
/// groups plus the key's prefix depth regardless of how deeply groups nest.
/// </para>
/// </remarks>
public interface ILatticeDecisionEngine
{
    /// <summary>
    /// The monotonically increasing epoch of the current compiled snapshot. It
    /// advances every time the snapshot is rebuilt from a committed policy change,
    /// so a caller can detect that its cached decisions may be stale. A later
    /// strict-consistency feature fences enforcement on this epoch; this engine
    /// only produces it.
    /// </summary>
    long CurrentEpoch { get; }

    /// <summary>
    /// Evaluates whether <paramref name="subject"/> may perform
    /// <paramref name="operation"/> on <paramref name="treeId"/>, synchronously
    /// and in-memory.
    /// </summary>
    /// <param name="subject">The requesting subject, carrying its flat group closure.</param>
    /// <param name="treeId">The target tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="key">
    /// The exact key for a point request, or <c>null</c> for a collection (range /
    /// whole-tree) request. For a collection request whose admission varies
    /// key-by-key the decision carries a <see cref="LatticeAccessDecision.KeyFilter"/>
    /// that admits a key iff the point decision for that key is an allow.
    /// </param>
    /// <param name="rangeStart">The inclusive range start for a range request, or <c>null</c>.</param>
    /// <param name="rangeEnd">The exclusive range end for a range request, or <c>null</c>.</param>
    /// <returns>The access decision, always carrying a human-readable reason on a denial or filter.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    LatticeAccessDecision Evaluate(
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key = null,
        string? rangeStart = null,
        string? rangeEnd = null);
}
