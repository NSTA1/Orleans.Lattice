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
internal readonly record struct RepoFileVectorIngestOutcome(
    int FilesEmbedded,
    int GapsSelected,
    bool CoverageEstablished)
{
    /// <summary>
    /// A pass that embedded nothing and established nothing, which is what a binding
    /// that does not embed at all returns.
    /// </summary>
    public static RepoFileVectorIngestOutcome None { get; } = new(0, 0, CoverageEstablished: false);

    /// <summary>
    /// Whether this pass proved the repository's embedding coverage complete: it
    /// established coverage and found no unchanged file missing a vector.
    /// </summary>
    public bool Converged => CoverageEstablished && GapsSelected == 0;
}
