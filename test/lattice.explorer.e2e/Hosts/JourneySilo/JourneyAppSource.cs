using Microsoft.Extensions.Options;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using TaskBoardApp = Orleans.Lattice.Samples.Explorer.TaskBoard.TaskBoardApp;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// A named, static app catalogue source over apps embedded in this host, the way an
/// operator publishes a curated catalogue: the in-image source's resolution, listing and
/// digest-verified asset serving, under a source key of its own. The journey estate
/// registers two, <see cref="Samples"/> and <see cref="SamplesNext"/>, so installing from a
/// named source, upgrading within a source and across sources, and re-consenting are all
/// reachable.
/// </summary>
/// <remarks>
/// <para>
/// Both sources advertise <see cref="AppSourceCapabilities.Search"/>, matching a query's text
/// against each app's slug, display name and summary, and <see cref="Samples"/> also lists
/// <see cref="FillerCount"/> catalogue fillers (see <see cref="Fillers"/>), so the catalogue's
/// text search and its "Load more apps" paging are reachable on the journey estate (A5, #5376).
/// </para>
/// <para>
/// <see cref="Samples"/> ships the task board at 1.0.0 and offers its next version, 1.1.0 (the
/// version <see cref="SamplesNext"/> offers), once its publisher has released it: when the
/// cluster-wide <see cref="IJourneyCatalogueGrain"/> says so, set through the fault seams
/// (<c>POST /e2e/apps/publish?source=samples</c>). Until then it is a one-version source; after,
/// the task board's entry lists both versions, newest first, so an install from
/// <see cref="Samples"/> shows under the catalogue's Updates filter.
/// </para>
/// </remarks>
internal sealed class JourneyAppSource : IAppCatalogSource
{
    /// <summary>The source of the task-board sample as it ships.</summary>
    public const string Samples = "samples";

    /// <summary>The source of the task board's next version.</summary>
    public const string SamplesNext = "samples-next";

    /// <summary>How many catalogue fillers <see cref="Samples"/> lists.</summary>
    public const int FillerCount = 60;

    /// <summary>The slug prefix of every catalogue filler: <c>workbench-01</c> to <c>workbench-60</c>.</summary>
    public const string FillerSlugPrefix = "workbench-";

    /// <summary>The summary every catalogue filler carries, so none can be mistaken for a real app.</summary>
    public const string FillerSummary = "Catalogue filler for journey A5 pagination";

    private static IGrainFactory? _grains;

    private readonly InImageAppSource _inner;
    private readonly InImageAppSource? _next;
    private readonly IReadOnlyList<AppSourceEntry> _fillers;
    private readonly Lock _gate = new();
    private AppSourceEntry[]? _listing;
    private AppSourceEntry[]? _publishedListing;

    private JourneyAppSource(string key, string displayName, InImageAppRegistration registration, bool publishesNext = false)
    {
        var capabilities = AppSourceCapabilities.Enumerate | AppSourceCapabilities.Search;
        Descriptor = new AppSourceDescriptor(key, displayName, AppSourceKind.Static, publishesNext ? capabilities | AppSourceCapabilities.MultipleVersions : capabilities);
        _inner = Single(registration);
        _next = publishesNext ? Single(NextRegistration()) : null;
        _fillers = publishesNext ? Fillers(key) : [];
    }

    /// <inheritdoc />
    public AppSourceDescriptor Descriptor { get; }

    /// <summary>The <see cref="Samples"/> source: the task-board sample at its shipped version, its next version once published, and the catalogue fillers.</summary>
    public static JourneyAppSource CreateSamples() =>
        new(Samples, "Samples", new InImageAppRegistration(
            AppSlug.Parse(TaskBoardApp.Slug),
            TaskBoardApp.Assembly,
            TaskBoardApp.ManifestResourceName),
            publishesNext: true);

    /// <summary>The <see cref="SamplesNext"/> source: the task board's next version, one added operation.</summary>
    public static JourneyAppSource CreateSamplesNext() => new(SamplesNext, "Samples (next)", NextRegistration());

    /// <summary>Gives every journey app source the cluster's grains, once the silo is built, so it can read what has been published.</summary>
    /// <param name="grains">The silo's grain factory.</param>
    public static void UseGrains(IGrainFactory grains) => _grains = grains;

    /// <inheritdoc />
    public async ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        if (_fillers.FirstOrDefault(filler => filler.Slug == slug) is { Manifest: { } fillerManifest, Provenance: { } fillerProvenance })
        {
            return version is { } requested && requested != fillerManifest.Identity.Version
                ? AppSourceResult.VersionMismatch(slug, requested, fillerManifest.Identity.Version)
                : AppSourceResult.Resolved(fillerManifest, fillerProvenance, new FillerActivation(fillerManifest.Identity));
        }

        var source = _inner;
        if (_next is not null && await NextPublishedAsync())
        {
            var next = await _next.ResolveAsync(slug, version: null, cancellationToken);
            if (next.Manifest is { } nextManifest && (version is null || version == nextManifest.Identity.Version))
            {
                source = _next;
            }
        }

        var result = await source.ResolveAsync(slug, version, cancellationToken);
        return result is { IsResolved: true, Manifest: { } manifest, Provenance: { } provenance, Activation: { } activation }
            ? AppSourceResult.Resolved(manifest, Rekey(provenance), activation)
            : result;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Lists every app in ordinal slug order, keeps those whose slug, display name or summary
    /// contains the query's text (ignoring case), and pages after the continuation's slug.
    /// </remarks>
    public async ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var listing = await ListingAsync(cancellationToken);
        var matching = listing.Where(entry => Matches(entry, query.Text)).ToList();
        var start = query.Continuation is { } after
            ? matching.FindIndex(entry => string.CompareOrdinal(entry.Slug.Value, after) > 0)
            : 0;
        if (start < 0)
        {
            return AppSourcePage.Empty;
        }

        var page = matching.Skip(start).Take(query.PageSize).ToArray();
        var more = start + page.Length < matching.Count;
        return AppSourcePage.Create(page, more ? page[^1].Slug.Value : null);
    }

    /// <inheritdoc />
    public async ValueTask<AppAssetResult> OpenAssetAsync(
        AppSlug slug,
        AppVersion version,
        string path,
        string expectedSha256,
        CancellationToken cancellationToken = default)
    {
        if (_next is not null && await NextPublishedAsync()
            && (await _next.ResolveAsync(slug, version, cancellationToken)).IsResolved)
        {
            return await _next.OpenAssetAsync(slug, version, path, expectedSha256, cancellationToken);
        }

        return await _inner.OpenAssetAsync(slug, version, path, expectedSha256, cancellationToken);
    }

    private static InImageAppSource Single(InImageAppRegistration registration)
    {
        var options = new InImageAppSourceOptions();
        options.Registrations.Add(registration);
        return new InImageAppSource(Options.Create(options));
    }

    private static InImageAppRegistration NextRegistration() => new(
        AppSlug.Parse(TaskBoardApp.Slug),
        typeof(JourneyAppSource).Assembly,
        "JourneySilo.TaskBoardNext.manifest.json")
    {
        AssetResourcePrefix = "JourneySilo.TaskBoardNext.ui.",
    };

    private async ValueTask<bool> NextPublishedAsync() =>
        _grains is { } grains && await grains.GetGrain<IJourneyCatalogueGrain>(Descriptor.Key).IsNextPublishedAsync();

    private AppProvenance Rekey(AppProvenance provenance) => new()
    {
        Source = Descriptor.Key,
        Publisher = provenance.Publisher,
        Reference = provenance.Reference,
    };

    private async ValueTask<AppSourceEntry[]> ListingAsync(CancellationToken cancellationToken)
    {
        var published = _next is not null && await NextPublishedAsync();
        lock (_gate)
        {
            if ((published ? _publishedListing : _listing) is { } cached)
            {
                return cached;
            }
        }

        var entries = new List<AppSourceEntry>(_fillers);
        foreach (var entry in await ListAllAsync(_inner, cancellationToken))
        {
            entries.Add(published ? await WithNextAsync(entry, cancellationToken) : entry);
        }

        var listing = entries.OrderBy(entry => entry.Slug.Value, StringComparer.Ordinal).ToArray();
        lock (_gate)
        {
            return published ? _publishedListing ??= listing : _listing ??= listing;
        }
    }

    // The task board's entry once its next version is published: both versions, newest first,
    // described by the newest version's manifest.
    private async ValueTask<AppSourceEntry> WithNextAsync(AppSourceEntry entry, CancellationToken cancellationToken)
    {
        var next = await _next!.ResolveAsync(entry.Slug, version: null, cancellationToken);
        return next is { IsResolved: true, Manifest: { } manifest, Provenance: { } provenance } && entry.IsAvailable
            ? AppSourceEntry.Available([manifest.Identity.Version, .. entry.Versions], manifest, Rekey(provenance))
            : entry;
    }

    private async ValueTask<List<AppSourceEntry>> ListAllAsync(InImageAppSource source, CancellationToken cancellationToken)
    {
        var entries = new List<AppSourceEntry>();
        string? continuation = null;
        do
        {
            var page = await source.ListAsync(new AppSourceQuery { PageSize = AppSourceQuery.MaxPageSize, Continuation = continuation }, cancellationToken);
            foreach (var entry in page.Entries)
            {
                entries.Add(entry is { Manifest: { } manifest, Provenance: { } provenance }
                    ? AppSourceEntry.Available(entry.Versions, manifest, Rekey(provenance))
                    : entry);
            }

            continuation = page.Continuation;
        }
        while (continuation is not null);

        return entries;
    }

    private static bool Matches(AppSourceEntry entry, string? text) =>
        string.IsNullOrWhiteSpace(text)
        || entry.Slug.Value.Contains(text.Trim(), StringComparison.OrdinalIgnoreCase)
        || entry.Manifest?.Presentation?.DisplayName.Contains(text.Trim(), StringComparison.OrdinalIgnoreCase) == true
        || entry.Manifest?.Presentation?.Summary?.Contains(text.Trim(), StringComparison.OrdinalIgnoreCase) == true;

    // ----- Catalogue fillers (A5, #5376) -----
    //
    // The catalogue pages at AvailableAppQuery.DefaultPageSize (50) rows, and the two sources
    // above offer one app each, so without these the "Load more apps" control can never render
    // on the journey estate. Each filler is unmistakably not a real app: its summary says what
    // it is for, it ships no UI, declares no tree and no role (so installing it grants nothing),
    // and its slug sorts after task-board, so the task board stays on the catalogue's first page.

    /// <summary>The catalogue fillers <paramref name="sourceKey"/> lists: <c>workbench-01</c> to <c>workbench-60</c>, version 1.0.0.</summary>
    /// <param name="sourceKey">The listing source's key, recorded as each filler's provenance.</param>
    private static IReadOnlyList<AppSourceEntry> Fillers(string sourceKey)
    {
        var version = AppVersion.Parse("1.0.0");
        var provenance = new AppProvenance
        {
            Source = sourceKey,
            Publisher = "Orleans.Lattice journey estate",
            Reference = "generated:journey-a5-catalogue-filler",
        };
        var fillers = new AppSourceEntry[FillerCount];
        for (var i = 0; i < fillers.Length; i++)
        {
            var number = (i + 1).ToString("D2", System.Globalization.CultureInfo.InvariantCulture);
            var manifest = new AppManifest
            {
                Identity = new AppIdentity { Slug = AppSlug.Parse(FillerSlugPrefix + number), Version = version, Provenance = provenance },
                Trees = [],
                Roles = [],
                Subscriptions = [],
                McpTools = [],
                Presentation = new AppPresentation
                {
                    DisplayName = "Workbench " + number,
                    Summary = FillerSummary,
                    PublisherDisplayName = "Orleans.Lattice journey estate",
                },
            };
            fillers[i] = AppSourceEntry.Available([version], manifest, provenance);
        }

        return fillers;
    }

    /// <summary>A filler's activation: it ships no code, so there is nothing to load.</summary>
    /// <param name="identity">The filler's identity.</param>
    private sealed class FillerActivation(AppIdentity identity) : IAppActivationHandle
    {
        public AppIdentity Identity { get; } = identity;

        public ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(AppActivationResult.Activated(typeof(JourneyAppSource).Assembly));
    }
}
