namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The seam a host drives to prove the vector plane can actually serve semantic
/// retrieval, so its readiness probe reports demonstrated capability rather than
/// configuration.
/// <para>
/// <b>Why a driver is needed.</b> The only honest signal for "can this box serve a
/// semantic query" is a semantic query. Waiting for a client to supply one deadlocks:
/// an orchestrator holds traffic back until the box is ready, and the box only becomes
/// ready once traffic arrives. A host breaks that cycle by issuing the first query
/// through this seam. Probing the embedding provider instead would not do: an embedder
/// can be perfectly reachable while the vector plane is still replaying, which is
/// exactly the state that reported a false-ready in the field.
/// </para>
/// <para>
/// The implementation adds no retrieval logic of its own - it drives the ordinary
/// search path, which folds its resolved <see cref="RepoContextRetrievalPath"/> into
/// the shared <see cref="RepoContextRetrievalReadinessState"/> exactly as a client
/// query would - and it runs under whatever ambient credential the calling host has
/// established.
/// </para>
/// </summary>
public interface IRepoContextRetrievalWarmup
{
    /// <summary>
    /// Runs one warmup pass: issues a real semantic query against the indexed
    /// repositories until the retrieval plane answers, folding each outcome into the
    /// shared readiness state. Fail-closed: a fault never marks the plane ready and
    /// never propagates, so a caller simply retries.
    /// </summary>
    /// <param name="cancellationToken">Cancels the pass; cancellation is never swallowed.</param>
    /// <returns><see langword="true"/> once the retrieval plane reports ready; otherwise <see langword="false"/>, meaning the caller should retry later.</returns>
    Task<bool> TryWarmAsync(CancellationToken cancellationToken);
}
