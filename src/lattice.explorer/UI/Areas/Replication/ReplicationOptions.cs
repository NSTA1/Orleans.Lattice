namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>The Replication area's cadences and read bounds.</summary>
internal sealed class ReplicationOptions
{
    /// <summary>
    /// How long a read of the estate or the enrolment report is reused by the
    /// directory badge, Home and completions before it is read again. A page's own
    /// refresh always reads afresh.
    /// </summary>
    public TimeSpan CacheLifetime { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>How often a tree's detail page re-reads its links while the page is visible.</summary>
    public TimeSpan RefreshInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>The page size asked of the status facade.</summary>
    public int PageSize { get; init; } = 1000;

    /// <summary>The most status pages one read follows before it reports itself truncated.</summary>
    public int MaxPages { get; init; } = 50;
}
