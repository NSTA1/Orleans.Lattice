using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// A scripted <see cref="ILatticeAppCatalog"/>: sources, per-source offers joined
/// with an installed-state map, descriptions and icons, with optional gates so a
/// test can hold a description open and observe the stages in between.
/// </summary>
internal sealed class FakeAppCatalog : ILatticeAppCatalog
{
    /// <summary>The advisory flags; everything is granted unless a test restricts it.</summary>
    public LatticeAppCatalogCapabilities Capabilities { get; set; } = new()
    {
        CanListSources = true,
        CanListAvailable = true,
        CanDescribeFromSource = true,
        CanGetIcon = true,
    };

    /// <summary>The configured sources.</summary>
    public List<AppSourceSummary> Sources { get; } = [];

    /// <summary>Every offered app, one per (source, slug).</summary>
    public List<AvailableAppSummary> Offers { get; } = [];

    /// <summary>Descriptions by (source, slug, version).</summary>
    public Dictionary<(string Source, string Slug, string Version), AppDescriptor> Descriptions { get; } = [];

    /// <summary>Icons by (source, slug).</summary>
    public Dictionary<(string Source, string Slug), AppIconAsset> Icons { get; } = [];

    /// <summary>When set, <see cref="DescribeFromSourceAsync"/> waits for it before answering.</summary>
    public TaskCompletionSource? DescribeGate { get; set; }

    /// <summary>When set, <see cref="GetIconAsync"/> waits for it before answering.</summary>
    public TaskCompletionSource? IconGate { get; set; }

    /// <summary>When set, every listing throws it.</summary>
    public Exception? ListFailure { get; set; }

    /// <summary>When set, listing available apps (but not sources) throws it.</summary>
    public Exception? AvailableFailure { get; set; }

    /// <summary>The page size listings are cut into.</summary>
    public int PageSize { get; set; } = 50;

    /// <summary>Every listing query received.</summary>
    public List<AvailableAppQuery> Queries { get; } = [];

    /// <summary>Every description request received.</summary>
    public List<(string Source, string Slug, string? Version)> Describes { get; } = [];

    /// <inheritdoc />
    public Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default) =>
        ListFailure is { } failure ? Task.FromException<ImmutableArray<AppSourceSummary>>(failure) : Task.FromResult<ImmutableArray<AppSourceSummary>>([.. Sources]);

    /// <inheritdoc />
    public Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)
    {
        Queries.Add(query);
        if ((AvailableFailure ?? ListFailure) is { } failure)
        {
            return Task.FromException<AvailableAppPage>(failure);
        }

        var matching = Offers
            .Where(app => query.SourceKey is null || app.SourceKey == query.SourceKey)
            .Where(app => query.Text is null || app.Slug.Contains(query.Text, StringComparison.OrdinalIgnoreCase))
            .Where(app => query.Filter switch
            {
                AvailableAppFilter.Installed => app.InstalledState is not null,
                AvailableAppFilter.Available => app.InstalledState is null,
                AvailableAppFilter.Updates => app.InstalledVersion is not null && app.InstalledVersion != app.NewestVersion,
                _ => true,
            })
            .OrderBy(app => app.Slug, StringComparer.Ordinal)
            .ThenBy(app => app.SourceKey, StringComparer.Ordinal)
            .ToArray();

        var skip = query.Continuation is null ? 0 : int.Parse(query.Continuation, System.Globalization.CultureInfo.InvariantCulture);
        var size = Math.Min(query.PageSize, PageSize);
        var page = matching.Skip(skip).Take(size).ToImmutableArray();
        var next = skip + size < matching.Length ? (skip + size).ToString(System.Globalization.CultureInfo.InvariantCulture) : null;
        return Task.FromResult(new AvailableAppPage { Apps = page, Continuation = next });
    }

    /// <inheritdoc />
    public async Task<AppDescriptor?> DescribeFromSourceAsync(string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)
    {
        Describes.Add((sourceKey, appSlug, version));
        if (DescribeGate is { } gate)
        {
            await gate.Task;
        }

        if (version is not null)
        {
            return Descriptions.GetValueOrDefault((sourceKey, appSlug, version));
        }

        return Descriptions
            .Where(pair => pair.Key.Source == sourceKey && pair.Key.Slug == appSlug)
            .OrderByDescending(pair => pair.Key.Version, StringComparer.Ordinal)
            .Select(pair => pair.Value)
            .FirstOrDefault();
    }

    /// <inheritdoc />
    public async Task<AppIconAsset?> GetIconAsync(string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)
    {
        if (IconGate is { } gate)
        {
            await gate.Task;
        }

        return Icons.GetValueOrDefault((sourceKey, appSlug));
    }

    /// <summary>When set, the capability probe throws it.</summary>
    public Exception? CapabilitiesFailure { get; set; }

    /// <inheritdoc />
    public Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) =>
        CapabilitiesFailure is { } failure ? Task.FromException<LatticeAppCatalogCapabilities>(failure) : Task.FromResult(Capabilities);
}
