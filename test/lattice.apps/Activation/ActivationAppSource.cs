using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>An <see cref="IAppSource"/> serving whatever manifest or failure a test publishes per slug.</summary>
internal sealed class ActivationAppSource : IAppSource
{
    private readonly Dictionary<AppSlug, AppSourceResult> _results = new();

    public int Resolutions { get; private set; }

    public void Publish(AppManifest manifest) =>
        _results[manifest.Identity.Slug] = AppSourceResult.Resolved(
            manifest,
            new AppProvenance { Source = "test" },
            new NoCodeActivationHandle(manifest.Identity));

    public void Fail(AppSourceResult result) => _results[result.Slug] = result;

    public ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        Resolutions++;
        if (!_results.TryGetValue(slug, out var result))
            return new(AppSourceResult.NotFound(slug));
        if (version is { } requested && result.Manifest is { } manifest && manifest.Identity.Version != requested)
            return new(AppSourceResult.VersionMismatch(slug, requested, manifest.Identity.Version));
        return new(result);
    }

    private sealed class NoCodeActivationHandle(AppIdentity identity) : IAppActivationHandle
    {
        public AppIdentity Identity { get; } = identity;

        public ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Activation must not load app code.");
    }
}
