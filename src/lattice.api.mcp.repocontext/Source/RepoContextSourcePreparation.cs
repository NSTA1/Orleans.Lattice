namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What a source strategy resolved for the next index generation: whether to
/// proceed, the job request to index (rewritten to point at staged content when the
/// source staged it), the commit SHA that anchors the generation, and a sanitised
/// reason when preparation failed.
/// <para>
/// An in-memory control artefact only - it never crosses an Orleans wire, so it
/// carries no serialization attributes. <see cref="FailureReason"/> is always
/// secret-redacted before it reaches this record, so it is safe to log.
/// </para>
/// </summary>
internal sealed record RepoContextSourcePreparation
{
    /// <summary>Which strategy produced this preparation.</summary>
    public required RepoContextSourceKind Kind { get; init; }

    /// <summary>Whether to index, skip as already current, or stand down.</summary>
    public required RepoContextSourceOutcome Outcome { get; init; }

    /// <summary>
    /// The job request to index. Non-<see langword="null"/> only when
    /// <see cref="Outcome"/> is <see cref="RepoContextSourceOutcome.Proceed"/>.
    /// </summary>
    public RepoIndexJobRequest? Request { get; init; }

    /// <summary>
    /// The resolved commit SHA anchoring this generation, or <see langword="null"/>
    /// for a mounted-workspace source (which has no verifiable anchor).
    /// </summary>
    public string? CommitSha { get; init; }

    /// <summary>
    /// A short, secret-redacted description of why preparation failed. Non-empty
    /// only when <see cref="Outcome"/> is
    /// <see cref="RepoContextSourceOutcome.Failed"/>.
    /// </summary>
    public string? FailureReason { get; init; }

    /// <summary>
    /// Creates a preparation that tells the caller to index
    /// <paramref name="request"/>.
    /// </summary>
    /// <param name="kind">The strategy that staged the content.</param>
    /// <param name="request">The job request to index. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="commitSha">The commit SHA anchoring the generation, or
    /// <see langword="null"/> when the source has no commit anchor.</param>
    public static RepoContextSourcePreparation Proceed(
        RepoContextSourceKind kind, RepoIndexJobRequest request, string? commitSha)
    {
        ArgumentNullException.ThrowIfNull(request);
        return new RepoContextSourcePreparation
        {
            Kind = kind,
            Outcome = RepoContextSourceOutcome.Proceed,
            Request = request,
            CommitSha = commitSha,
        };
    }

    /// <summary>
    /// Creates a preparation that reports the source is already at the indexed
    /// revision, so the run is a no-op and the last-good index keeps serving.
    /// </summary>
    /// <param name="kind">The strategy that resolved the revision.</param>
    /// <param name="commitSha">The resolved commit SHA, which equals the
    /// last-indexed SHA.</param>
    public static RepoContextSourcePreparation UpToDate(
        RepoContextSourceKind kind, string commitSha) =>
        new()
        {
            Kind = kind,
            Outcome = RepoContextSourceOutcome.UpToDate,
            CommitSha = commitSha,
        };

    /// <summary>
    /// Creates a fail-closed preparation: nothing is indexed, nothing is pruned, and
    /// no other source is attempted.
    /// </summary>
    /// <param name="kind">The strategy that failed.</param>
    /// <param name="reason">A short, already secret-redacted reason. Must not be
    /// <see langword="null"/>.</param>
    public static RepoContextSourcePreparation Failed(
        RepoContextSourceKind kind, string reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        return new RepoContextSourcePreparation
        {
            Kind = kind,
            Outcome = RepoContextSourceOutcome.Failed,
            FailureReason = reason,
        };
    }
}
