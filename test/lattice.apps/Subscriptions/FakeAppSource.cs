using NSubstitute;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>An <see cref="IAppSource"/> serving fixed manifests, for subscription runtime tests.</summary>
internal sealed class FakeAppSource : IAppSource
{
    private readonly Dictionary<AppSlug, AppManifest> _manifests = new();

    public int ResolveCalls { get; private set; }

    public FakeAppSource Add(AppManifest manifest)
    {
        _manifests[manifest.Identity.Slug] = manifest;
        return this;
    }

    public ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        ResolveCalls++;
        if (!_manifests.TryGetValue(slug, out var manifest))
            return ValueTask.FromResult(AppSourceResult.NotFound(slug));
        if (version is { } requested && requested != manifest.Identity.Version)
            return ValueTask.FromResult(AppSourceResult.VersionMismatch(slug, requested, manifest.Identity.Version));
        return ValueTask.FromResult(AppSourceResult.Resolved(manifest, new AppProvenance(), Substitute.For<IAppActivationHandle>()));
    }
}
