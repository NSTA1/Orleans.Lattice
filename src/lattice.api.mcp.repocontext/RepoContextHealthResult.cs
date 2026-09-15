namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The structured result of the <c>repocontext_health</c> probe tool: a small,
/// read-only report that the repository-context MCP surface is registered,
/// reachable, and that the caller cleared the fail-closed authorization gate,
/// <b>together with whether retrieval can actually serve</b>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why it carries readiness.</b> Reachability and capability are different
/// facts and a host can hold them in opposite states: the vector plane can be
/// unable to serve while the surface answers every call normally. Reporting only
/// the first made a degraded host indistinguishable from a healthy one at the
/// probe an agent is instructed to call first, so a caller that was being served
/// keyword recall over a two-day-stale index read green and proceeded. The
/// readiness fields close that gap by reporting the same
/// <see cref="RepoContextRetrievalReadinessState"/> the <c>/health/ready</c>
/// endpoint already reads, so the two consumers of that signal cannot disagree.
/// </para>
/// <para>
/// <b>Why <see cref="Available"/> does not become false when retrieval is
/// degraded.</b> It answers "is this surface reachable and am I authorized", and
/// on a degraded host that is still true: capture, recall, and scan all work, and
/// a caller that stopped using them would lose working capability for no reason.
/// Readiness is reported as a second, independent fact beside the verdict rather
/// than folded into it, exactly as
/// <see cref="RepoContextRetrievalReadinessState.Arming"/> is kept beside
/// <see cref="RepoContextRetrievalReadinessState.Phase"/> for the same reason.
/// Callers that need "can this host serve semantic retrieval" must read
/// <see cref="RetrievalReady"/>; <see cref="Available"/> has never answered that
/// question and still does not.
/// </para>
/// <para>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </para>
/// </remarks>
public sealed record RepoContextHealthResult
{
    /// <summary>
    /// Whether the repository-context surface is reachable and the caller is
    /// authorized. Always <see langword="true"/> when this result is returned: an
    /// unauthorized caller is never offered the tool, so reaching it at all confirms
    /// availability.
    /// <para>
    /// This is <b>not</b> a statement that retrieval can serve. Read
    /// <see cref="RetrievalReady"/> for that.
    /// </para>
    /// </summary>
    public required bool Available { get; init; }

    /// <summary>
    /// The stable name of the facade group this surface belongs to
    /// (<c>repocontext</c>), matching the group's entry in the
    /// <c>lattice_capabilities</c> report.
    /// </summary>
    public required string Group { get; init; }

    /// <summary>
    /// A short, human-readable status line describing the surface's readiness. It
    /// names the degradation when there is one, so the prose and the machine-readable
    /// fields cannot tell a reader different stories.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Whether the host can serve the retrieval it is configured for: the vector
    /// plane is serving, or no embedder is bound and keyword recall is the intended
    /// steady state. <see langword="false"/> means semantic retrieval is not being
    /// served and results will be degraded keyword recall.
    /// </summary>
    public required bool RetrievalReady { get; init; }

    /// <summary>
    /// The canonical readiness phase: <c>serving</c>, <c>keyword_only</c>,
    /// <c>nothing_registered</c>, or <c>building</c>. It discriminates the two
    /// reasons a host answers keyword recall: <c>keyword_only</c> is an intended
    /// keyword-only deployment and is ready, while <c>building</c> is a plane that
    /// cannot serve and is not.
    /// </summary>
    public required string RetrievalPhase { get; init; }
}
