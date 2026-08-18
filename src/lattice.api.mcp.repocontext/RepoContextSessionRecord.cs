namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The per-session reuse-bookkeeping record behind the <c>repocontext_context</c>
/// tool's reuse economics, stored at the key
/// <c>repo/{repoId}/session/{sessionId}</c> (see
/// <see cref="RepoContextKeys.Session(string, string)"/>). It remembers, for one
/// named caller session, exactly what a prior bundle call already delivered so a
/// later call in the same session never pays twice for the same context:
/// <list type="bullet">
///   <item><description>
///     <see cref="Receipts"/> - the opaque, deterministic receipts of every
///     <b>unit</b> (a path pointer, a body span, or an outline symbol) already
///     delivered to this session, so a later call auto-suppresses each unit the
///     session already holds without the caller re-supplying it.
///   </description></item>
///   <item><description>
///     <see cref="Possession"/> - the <c>path\0hash</c> tokens of every file
///     <b>version delivered as a complete body</b> (a whole-file span). A partial
///     delivery (a path pointer or an outline symbol) is recorded in
///     <see cref="Receipts"/> only and <b>never</b> in <see cref="Possession"/>, so
///     partial evidence can never be promoted to a whole-file possession claim.
///   </description></item>
/// </list>
/// <para>
/// Both sets are grow-only <see cref="GSet"/> CRDTs, so two bundle calls that share
/// a session id and run concurrently converge on merge (their deliveries union)
/// under any delivery order - the merge is commutative, associative, and
/// idempotent. The record is bounded per call (a call adds at most its packed
/// units) and the store writes it with a finite time-to-live, so an abandoned
/// session's bookkeeping lapses on its own.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.SessionRecord)]
internal sealed record RepoContextSessionRecord
{
    /// <summary>The opaque caller session identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string SessionId { get; init; } = string.Empty;

    /// <summary>The repository identifier the session's bookkeeping is scoped to - immutable identity carried in the key.</summary>
    [Id(1)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>
    /// Grow-only set of opaque unit receipts already delivered to this session, each
    /// element the UTF-8 bytes of a receipt string. Membership suppresses exactly the
    /// unit that receipt identifies on a later call.
    /// </summary>
    [Id(2)]
    public GSet Receipts { get; init; } = new();

    /// <summary>
    /// Grow-only set of whole-file possession tokens (the UTF-8 bytes of a
    /// <c>path\0hash</c> token), one per file version delivered to this session as a
    /// complete body. Only a whole-file span delivery is recorded here; partial
    /// deliveries never are, which is the load-bearing partial-to-whole guard.
    /// </summary>
    [Id(3)]
    public GSet Possession { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same session record. Identity is
    /// preserved from <paramref name="left"/> (falling back to <paramref name="right"/>
    /// only when the left side is unset); the two grow-only sets are unioned, so the
    /// result is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public static RepoContextSessionRecord Merge(RepoContextSessionRecord left, RepoContextSessionRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new RepoContextSessionRecord
        {
            SessionId = left.SessionId.Length != 0 ? left.SessionId : right.SessionId,
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            Receipts = GSet.Merge(left.Receipts, right.Receipts),
            Possession = GSet.Merge(left.Possession, right.Possession),
        };
    }
}
