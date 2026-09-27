using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>An <see cref="IAppSource"/> over an in-memory manifest set that counts resolutions.</summary>
internal sealed class FakeAppSource : IAppSource
{
    private readonly Dictionary<(AppSlug, AppVersion), AppManifest> _manifests = new();

    public int Resolutions { get; private set; }

    public Func<AppSlug, AppSourceResult?>? Override { get; set; }

    public FakeAppSource Add(AppManifest manifest)
    {
        _manifests[(manifest.Identity.Slug, manifest.Identity.Version)] = manifest;
        return this;
    }

    public ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        Resolutions++;
        if (Override?.Invoke(slug) is { } overridden)
            return new ValueTask<AppSourceResult>(overridden);

        foreach (var (key, manifest) in _manifests)
        {
            if (key.Item1 == slug && (version is null || key.Item2 == version))
                return new ValueTask<AppSourceResult>(AppMcpTestData.Resolved(manifest));
        }

        return new ValueTask<AppSourceResult>(AppSourceResult.NotFound(slug));
    }
}
