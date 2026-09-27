using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

internal readonly record struct RepoContextSemanticReadiness(
    bool? CanServe = null,
    string? Blocker = null,
    VectorIndexBuildProgress? Progress = null,
    bool Saturated = false,
    bool BreakerOpen = false,
    TimeSpan? ProbeDueIn = null,
    bool? AnnCanServe = null);
