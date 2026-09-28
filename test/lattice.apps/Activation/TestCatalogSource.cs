using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// A synchronous, named <see cref="IAppCatalogSource"/> serving the manifests and asset bytes a test publishes:
/// versions per slug (publish newest first), an integer listing continuation over ordinal slug order, verified
/// assets that a test can corrupt, and counters proving which source was asked. Shared with the facade test
/// project by file link.
/// </summary>
internal sealed class TestCatalogSource : IAppCatalogSource
{
    private readonly SortedDictionary<string, List<AppManifest>> _offers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, byte[]> _assets = new(StringComparer.Ordinal);

    public TestCatalogSource(
        string key,
        AppSourceKind kind = AppSourceKind.Static,
        AppSourceCapabilities capabilities = AppSourceCapabilities.Enumerate)
    {
        Descriptor = new AppSourceDescriptor(key, "Source " + key, kind, capabilities);
    }

    public AppSourceDescriptor Descriptor { get; }

    public int Resolutions { get; private set; }

    public int Listings { get; private set; }

    public int AssetOpens { get; private set; }

    public Exception? ListFault { get; set; }

    /// <summary>Offers a manifest version; publish newest first.</summary>
    public TestCatalogSource Publish(AppManifest manifest)
    {
        var slug = manifest.Identity.Slug.Value;
        if (!_offers.TryGetValue(slug, out var versions))
            _offers[slug] = versions = [];
        versions.Add(manifest);
        return this;
    }

    /// <summary>Serves <paramref name="content"/> for every app's asset at <paramref name="path"/>.</summary>
    public TestCatalogSource WithAsset(string path, byte[] content)
    {
        _assets[path] = content;
        return this;
    }

    /// <summary>Serves every asset of <see cref="UiTestManifests.Assets"/>.</summary>
    public TestCatalogSource WithUiAssets()
    {
        foreach (var (path, content) in UiTestManifests.Assets)
            _assets[path] = content;
        return this;
    }

    public ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        Resolutions++;
        if (slug.Value is null || !_offers.TryGetValue(slug.Value, out var versions))
            return new(AppSourceResult.NotFound(slug));
        var manifest = version is { } requested ? versions.Find(m => m.Identity.Version == requested) : versions[0];
        if (manifest is null)
            return new(AppSourceResult.VersionMismatch(slug, version!.Value, versions[0].Identity.Version));
        return new(AppSourceResult.Resolved(manifest, Provenance(), new NoCodeHandle(manifest.Identity)));
    }

    public ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        Listings++;
        if (ListFault is not null)
            throw ListFault;
        var start = 0;
        if (query.Continuation is { } continuation && (!int.TryParse(continuation, out start) || start < 0))
            return new(AppSourcePage.Empty);

        var all = _offers
            .Where(pair => query.Text is null || !Descriptor.Supports(AppSourceCapabilities.Search) || pair.Key.Contains(query.Text, StringComparison.Ordinal))
            .ToList();
        var page = all.Skip(start).Take(query.PageSize)
            .Select(pair => AppSourceEntry.Available(pair.Value.Select(m => m.Identity.Version).ToList(), pair.Value[0], Provenance()))
            .ToList();
        var next = start + page.Count;
        return new(AppSourcePage.Create(page, next < all.Count ? next.ToString(System.Globalization.CultureInfo.InvariantCulture) : null));
    }

    public ValueTask<AppAssetResult> OpenAssetAsync(
        AppSlug slug,
        AppVersion version,
        string path,
        string expectedSha256,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(expectedSha256);
        AssetOpens++;
        if (slug.Value is null || !_offers.TryGetValue(slug.Value, out var versions) || versions.TrueForAll(m => m.Identity.Version != version))
            return new(AppAssetResult.NotFound(path));
        if (!_assets.TryGetValue(path, out var content))
            return new(AppAssetResult.NotFound(path));
        var mediaType = path.EndsWith(".svg", StringComparison.Ordinal) ? "image/svg+xml"
            : path.EndsWith(".js", StringComparison.Ordinal) ? "text/javascript"
            : path.EndsWith(".css", StringComparison.Ordinal) ? "text/css"
            : "text/html";
        return new(AppAssetResult.Verify(path, content, mediaType, expectedSha256));
    }

    private AppProvenance Provenance() => new() { Source = Descriptor.Key, Publisher = "publisher-" + Descriptor.Key };

    private sealed class NoCodeHandle(AppIdentity identity) : IAppActivationHandle
    {
        public AppIdentity Identity { get; } = identity;

        public ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Resolution must not load app code.");
    }
}
