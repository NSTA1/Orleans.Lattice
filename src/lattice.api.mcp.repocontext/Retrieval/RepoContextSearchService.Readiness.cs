using System.Collections.Concurrent;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

internal sealed partial class RepoContextSearchService
{
    private readonly ConcurrentDictionary<string, RepositoryObservation> _repositoryObservations = new(StringComparer.Ordinal);

    private void ObserveRepository(string repoId, string path, string? reason)
    {
        var observation = _repositoryObservations.GetOrAdd(repoId, static _ => new());
        lock (observation)
        {
            var now = _timeProvider.GetUtcNow();
            observation.Path = path;
            observation.Reason = reason;
            observation.QueryAt = now;
            if (RepoContextRetrievalPath.IsSemantic(path))
            {
                observation.Served = true;
                observation.FaultSince = null;
            }
            else
            {
                observation.FaultSince ??= now;
            }
        }
    }

    private void ObserveContent(string repoId, RepoContextRetrievalReadinessPhase phase, string? reason)
    {
        var observation = _repositoryObservations.GetOrAdd(repoId, static _ => new());
        lock (observation)
        {
            observation.ContentPhase = phase;
            observation.ContentReason = reason;
            observation.ContentAt = _timeProvider.GetUtcNow();
        }
    }

    internal RepoContextRepositoryReadiness DescribeReadiness(
        string repoId, RepoIndexProgress? ingest, string? ingestReason, RepoContextEmbeddedCount coverage)
    {
        var space = _embeddingProvider?.Space;
        var index = space is null ? default : _index.DescribeReadiness(repoId, EmbeddingSpaceTag.FromSpace(space));
        var observed = new RepositoryObservation();
        if (_repositoryObservations.TryGetValue(repoId, out var current))
        {
            lock (current)
            {
                observed = current with { };
            }
        }

        var phase = RepoContextRetrievalReadinessPhase.Building;
        string? reason;
        if (_embeddingProvider is null)
        {
            phase = RepoContextRetrievalReadinessPhase.KeywordOnly;
            reason = "no_embedding_provider";
        }
        else if ((coverage is { Count: 0, Pending: false }
            || index is { AnnCanServe: true, Progress.VectorsIndexed: 0 })
            && ingest is { Status: RepoIndexStatus.Completed, FilesScanned: 0 })
        {
            phase = RepoContextRetrievalReadinessPhase.NothingRegistered;
            reason = "repository_has_no_indexed_files_or_vectors";
        }
        else if (index is { AnnCanServe: true, Progress.VectorsIndexed: 0 })
        {
            reason = "ann_contains_no_vectors_in_configured_space";
        }
        else if (index.Saturated && index.CanServe != true
            && (index.CanServe == false || !observed.Served || observed.Reason is not null))
        {
            phase = RepoContextRetrievalReadinessPhase.SaturatedUnavailable;
            reason = index.Blocker is null or "ann_open_saturated"
                ? "ann_open_saturated"
                : "ann_open_saturated;" + index.Blocker;
        }
        else if (observed.Reason is not null || index.CanServe == false)
        {
            reason = index.Blocker ?? observed.Reason;
        }
        else if (index.CanServe == true || observed.Served)
        {
            phase = RepoContextRetrievalReadinessPhase.Serving;
            reason = null;
        }
        else
        {
            reason = "semantic_serving_not_yet_demonstrated";
        }

        if (phase is RepoContextRetrievalReadinessPhase.Building or RepoContextRetrievalReadinessPhase.SaturatedUnavailable
            && observed.Served && observed.FaultSince is { } since
            && _timeProvider.GetUtcNow() - since < RepoContextRetrievalReadinessState.DefaultFaultHoldDown)
        {
            phase = RepoContextRetrievalReadinessPhase.Serving;
        }

        return new RepoContextRepositoryReadiness
        {
            RepoId = repoId,
            Verdict = phase,
            Reason = reason,
            Ingest = ingest,
            IngestReason = ingestReason,
            VectorCoverage = coverage,
            EmbeddingSpace = space,
            Ann = index.Progress,
            AnnCanServe = index.AnnCanServe,
            BreakerOpen = index.BreakerOpen,
            BreakerProbeDueIn = index.ProbeDueIn,
            LastRetrievalPath = observed.Path,
            LastQueryAt = observed.QueryAt,
            ContentPhase = observed.ContentPhase,
            ContentReason = observed.ContentReason,
            ContentObservedAt = observed.ContentAt,
        };
    }

    private sealed record RepositoryObservation
    {
        public string? Path { get; set; }
        public string? Reason { get; set; }
        public DateTimeOffset? QueryAt { get; set; }
        public bool Served { get; set; }
        public DateTimeOffset? FaultSince { get; set; }
        public RepoContextRetrievalReadinessPhase ContentPhase { get; set; }
        public string? ContentReason { get; set; } = "content_tree_not_observed";
        public DateTimeOffset? ContentAt { get; set; }
    }
}
