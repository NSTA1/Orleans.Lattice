using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A passive readiness snapshot for one repository on the answering server.
/// Unknown observations are not empty populations. No query, embedding, or breaker
/// probe is executed to produce this report.
/// </summary>
public sealed record RepoContextRepositoryReadiness
{
    /// <summary>The repository whose serving plane was observed.</summary>
    public required string RepoId { get; init; }

    /// <summary>The existing readiness vocabulary, resolved for this repository only.</summary>
    public required RepoContextRetrievalReadinessPhase Verdict { get; init; }

    /// <summary>The blocking predicate, or the reason nothing needs to serve; null when serving without a pending fault.</summary>
    public string? Reason { get; init; }

    /// <summary>Always false: this observation is never an entitlement or a correctness gate.</summary>
    public bool Authoritative => false;

    /// <summary>The ingest snapshot, or null when its metadata could not be read. Completion does not imply serving.</summary>
    public RepoIndexProgress? Ingest { get; init; }

    /// <summary>Why ingest metadata could not be read, or null when it was read.</summary>
    public string? IngestReason { get; init; }

    /// <summary>The last measured embedded-source count and its currency; an unknown count stays null. No refresh is scheduled.</summary>
    public RepoContextEmbeddedCount VectorCoverage { get; init; }

    /// <summary>The configured embedding space, or null on an intended keyword-only deployment.</summary>
    public EmbeddingSpace? EmbeddingSpace { get; init; }

    /// <summary>The current ANN build phase, generation and counts, or null when no local handle exists.</summary>
    public VectorIndexBuildProgress? Ann { get; init; }

    /// <summary>Whether the local ANN gate can answer; null when no handle exists or exact retrieval is configured.</summary>
    public bool? AnnCanServe { get; init; }

    /// <summary>The repository's exact-scan breaker state, read without consuming a half-open probe.</summary>
    public bool BreakerOpen { get; init; }

    /// <summary>Time until the next real query may obtain a recovery probe; null when no breaker episode exists.</summary>
    public TimeSpan? BreakerProbeDueIn { get; init; }

    /// <summary>The last semantic-path attribution observed for this repository, or null before its first query.</summary>
    public string? LastRetrievalPath { get; init; }

    /// <summary>When the last semantic-path outcome was observed, or null before its first query.</summary>
    public DateTimeOffset? LastQueryAt { get; init; }

    /// <summary>
    /// The most recent content-tree scan outcome: Serving for readable content,
    /// NothingRegistered for a completed empty scan, or Building for a fault or no observation.
    /// It is not a new scan and does not attest all content or hydration paths.
    /// </summary>
    public RepoContextRetrievalReadinessPhase ContentPhase { get; init; }

    /// <summary>The content-tree fault or missing-observation reason; null after a readable non-empty scan.</summary>
    public string? ContentReason { get; init; }

    /// <summary>When the content-tree scan outcome was observed, or null if none was observed.</summary>
    public DateTimeOffset? ContentObservedAt { get; init; }
}
