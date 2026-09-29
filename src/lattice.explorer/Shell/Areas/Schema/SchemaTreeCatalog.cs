using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The circuit's list of trees, by logical id, read from Core's state connection
/// and remembered briefly, so the directory and completions do not page the
/// catalogue on every keystroke.
/// </summary>
/// <remarks>
/// The schema facade has no "list governed trees" verb, so the tree list comes
/// from the catalogue. Physical trees are filtered out: a resize or restore
/// shadow appears in the registry beside the logical tree that aliases it, and the
/// Schema area never shows a physical id.
/// </remarks>
/// <param name="facades">The area's facades.</param>
/// <param name="time">The clock the freshness window is measured on.</param>
internal sealed class SchemaTreeCatalog(SchemaFacades facades, TimeProvider time)
{
    /// <summary>How long a read catalogue is reused.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromSeconds(30);

    /// <summary>The most catalogue pages one read follows.</summary>
    public const int MaximumPages = 20;

    /// <summary>What the caller is told when no cluster connection is configured.</summary>
    public const string NotConnected = "Connect to a cluster to list its trees.";

    private IReadOnlyList<string>? _trees;
    private DateTimeOffset _readAt;

    /// <summary>Whether the last read stopped at <see cref="MaximumPages"/> with more to come.</summary>
    public bool Truncated { get; private set; }

    /// <summary>Lists the logical trees, ordered by id.</summary>
    /// <param name="refresh">Read again even when the remembered list is fresh.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The logical tree ids.</returns>
    /// <exception cref="InvalidOperationException">No cluster connection is configured.</exception>
    public async Task<IReadOnlyList<string>> GetAsync(bool refresh, CancellationToken cancellationToken)
    {
        if (!refresh && _trees is { } remembered && time.GetUtcNow() - _readAt < Freshness)
        {
            return remembered;
        }

        if (facades.Session is not { IsConfigured: true } session)
        {
            throw new InvalidOperationException(NotConnected);
        }

        var entries = new List<TreeCatalogEntry>();
        string? token = null;
        var pages = 0;
        do
        {
            var page = await session.Connection
                .ListTreesAsync(new CatalogRequest { PageSize = CatalogRequest.MaxPageSize, PageToken = token }, cancellationToken)
                .ConfigureAwait(false);
            entries.AddRange(page.Entries);
            token = page.NextPageToken;
            pages++;
        }
        while (token is not null && pages < MaximumPages);

        var trees = Project(entries);
        Truncated = token is not null;
        _trees = trees;
        _readAt = time.GetUtcNow();
        return trees;
    }

    /// <summary>Forgets the remembered list, so the next read goes to the cluster.</summary>
    public void Invalidate() => _trees = null;

    /// <summary>Projects catalogue entries onto logical tree ids, dropping every physical shadow.</summary>
    /// <param name="entries">The raw catalogue.</param>
    /// <returns>The logical tree ids, ordered.</returns>
    internal static IReadOnlyList<string> Project(IEnumerable<TreeCatalogEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var all = entries.ToList();
        var shadows = all
            .Where(entry => entry.IsAlias && entry.PhysicalTreeId is not null)
            .Select(entry => entry.PhysicalTreeId!)
            .ToHashSet(StringComparer.Ordinal);

        return all
            .Where(entry => entry.RestoreShadowOfTreeId is null && !shadows.Contains(entry.TreeId))
            .Select(entry => entry.TreeId)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
    }
}
