namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What one file-arm vectorisation pass actually did, as opposed to merely how many
/// files it embedded. The extra two facts are what let the coordinator decide whether
/// the repository's embedding coverage is converged, and therefore whether the next
/// pass needs to re-probe the whole content-unchanged set at all (issue #2049).
/// </summary>
/// <param name="FilesEmbedded">The number of files whose vectors were embedded and
/// stored this pass, across both the changed set and any back-filled gaps.</param>
/// <param name="GapsSelected">How many of the offered <i>content-unchanged</i> files
/// were found to have no live vector and were therefore selected for back-fill. Zero
/// on a pass whose coverage probe found nothing missing - the signal that the
/// repository is converged.</param>
/// <param name="CoverageEstablished">Whether the pass actually determined coverage
/// over the offered unchanged set. False when no embedding provider is bound, when
/// the provider is unreachable, or when the coverage probe failed and the gap sweep
/// was deferred - in which case <see cref="GapsSelected"/> being zero says nothing
/// about the repository and must not be read as convergence.</param>
/// <param name="Deferred">Whether the pass gave up early because the vector plane
/// looked saturated, leaving batches it had not reached unembedded. Without this the
/// arm's one saturation signal was computed and then discarded, so a pass that
/// deferred work was indistinguishable from one that had none to do (issue #2272).
/// The deferred sources stay unmarked and are retried on the next reconcile, so this
/// reports incompleteness, not loss.</param>
internal readonly record struct RepoFileVectorIngestOutcome(
    int FilesEmbedded,
    int GapsSelected,
    bool CoverageEstablished,
    bool Deferred = false)
{
    /// <summary>
    /// A pass that embedded nothing and established nothing, which is what a binding
    /// that does not embed at all returns.
    /// </summary>
    public static RepoFileVectorIngestOutcome None { get; } = new(0, 0, CoverageEstablished: false);

    /// <summary>
    /// Whether this pass proved the repository's embedding coverage complete: it
    /// established coverage, found no unchanged file missing a vector, and did not
    /// defer any batch. A saturated pass can reach zero selected gaps simply by
    /// giving up before it looked at them, so convergence has to exclude it.
    /// </summary>
    public bool Converged => CoverageEstablished && GapsSelected == 0 && !Deferred;
}
