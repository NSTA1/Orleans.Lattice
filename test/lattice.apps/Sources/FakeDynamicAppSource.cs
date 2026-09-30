using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// A test-only <see cref="AppSourceKind.Dynamic"/> source with every capability, shaped like a future package
/// feed: it offers several versions per slug (offer them newest first), filters by text, pages with an
/// integer continuation, and serves assets and activation only after <see cref="Acquire"/>. It resolves and
/// lists asynchronously so callers exercise their asynchronous paths.
/// </summary>
internal sealed class FakeDynamicAppSource : IAppCatalogSource
{
    public const string DefaultKey = "fake-feed";

    public const AppSourceCapabilities AllCapabilities =
        AppSourceCapabilities.Enumerate
        | AppSourceCapabilities.Search
        | AppSourceCapabilities.MultipleVersions
        | AppSourceCapabilities.RequiresAcquisition;

    private readonly SortedDictionary<string, List<Offer>> offers = new(StringComparer.Ordinal);
    private readonly HashSet<(AppSlug Slug, AppVersion Version)> acquired = [];
    private readonly string provenanceKey;

    public FakeDynamicAppSource(string key = DefaultKey, string? provenanceKey = null)
    {
        Descriptor = new AppSourceDescriptor(key, "Fake feed", AppSourceKind.Dynamic, AllCapabilities);
        this.provenanceKey = provenanceKey ?? key;
    }

    public AppSourceDescriptor Descriptor { get; }

    public int ResolveCalls { get; private set; }

    /// <summary>Offers a version of a slug; call newest first.</summary>
    public FakeDynamicAppSource Add(string slug, string version, params (string Path, byte[] Content)[] assets)
    {
        var manifest = SourceTestManifests.Manifest(slug, version);
        if (!offers.TryGetValue(slug, out var versions))
            offers[slug] = versions = [];
        versions.Add(new Offer(manifest, assets.ToDictionary(a => a.Path, a => a.Content, StringComparer.Ordinal)));
        return this;
    }

    public void Acquire(AppSlug slug, AppVersion version) => acquired.Add((slug, version));

    public async ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        ResolveCalls++;
        if (slug.Value is null || !offers.TryGetValue(slug.Value, out var versions))
            return AppSourceResult.NotFound(slug);

        var offer = version is { } requested ? versions.Find(o => o.Manifest.Identity.Version == requested) : versions[0];
        if (offer is null)
            return AppSourceResult.VersionMismatch(slug, version!.Value, versions[0].Manifest.Identity.Version);

        return AppSourceResult.Resolved(offer.Manifest, Provenance(offer.Manifest.Identity), new AcquiringHandle(this, offer.Manifest.Identity));
    }

    public async ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        await Task.Yield();
        var start = 0;
        if (query.Continuation is { } continuation && (!int.TryParse(continuation, out start) || start < 0))
            return AppSourcePage.Empty;

        var matching = offers
            .Where(pair => query.Text is null || pair.Key.Contains(query.Text, StringComparison.Ordinal))
            .ToList();
        var page = matching.Skip(start).Take(query.PageSize)
            .Select(pair => AppSourceEntry.Available(
                pair.Value.Select(o => o.Manifest.Identity.Version).ToList(),
                pair.Value[0].Manifest,
                Provenance(pair.Value[0].Manifest.Identity)))
            .ToList();
        var next = start + page.Count;
        return AppSourcePage.Create(page, next < matching.Count ? next.ToString(System.Globalization.CultureInfo.InvariantCulture) : null);
    }

    public async ValueTask<AppAssetResult> OpenAssetAsync(
        AppSlug slug,
        AppVersion version,
        string path,
        string expectedSha256,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(expectedSha256);
        await Task.Yield();
        if (!AppAssetPath.IsValid(path) || slug.Value is null || !offers.TryGetValue(slug.Value, out var versions))
            return AppAssetResult.NotFound(path);
        var offer = versions.Find(o => o.Manifest.Identity.Version == version);
        if (offer is null)
            return AppAssetResult.NotFound(path);
        if (!acquired.Contains((slug, version)))
            return AppAssetResult.NotAvailable(path, "The artifact has not been acquired.");
        if (!offer.Assets.TryGetValue(path, out var content))
            return AppAssetResult.NotFound(path);
        return AppAssetResult.Verify(path, content, AppAssetPath.MediaTypeOf(path) ?? "application/octet-stream", expectedSha256);
    }

    private AppProvenance Provenance(AppIdentity identity) => new()
    {
        Source = provenanceKey,
        Publisher = "pinned-publisher",
        Reference = $"feed:{identity.Slug}@{identity.Version}",
    };

    private sealed record Offer(AppManifest Manifest, Dictionary<string, byte[]> Assets);

    private sealed class AcquiringHandle(FakeDynamicAppSource source, AppIdentity identity) : IAppActivationHandle
    {
        public AppIdentity Identity { get; } = identity;

        public ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default) =>
            new(source.acquired.Contains((Identity.Slug, Identity.Version))
                ? AppActivationResult.Activated(typeof(FakeDynamicAppSource).Assembly)
                : AppActivationResult.Failed([new("not-acquired", "$", "The artifact has not been acquired.")]));
    }
}
