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

    /// <summary>
    /// Serves <paramref name="result"/> for <paramref name="key"/> regardless of the
    /// slug the result itself names, so a test can model a source that returns the
    /// wrong app - the case the engine's identity re-check exists to refuse.
    /// </summary>
    public void PublishAs(AppSlug key, AppSourceResult result) => _results[key] = result;

    /// <summary>Builds a resolved result for <paramref name="manifest"/>.</summary>
    public static AppSourceResult ResolvedResult(AppManifest manifest) =>
        AppSourceResult.Resolved(
            manifest,
            new AppProvenance { Source = "test" },
            new NoCodeActivationHandle(manifest.Identity));

    public ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        Resolutions++;
        if (FailResolve?.Invoke(slug) is { } failure)
            throw failure;
        if (!_results.TryGetValue(slug, out var result))
            return new(AppSourceResult.NotFound(slug));
        if (SkipVersionCheck)
            return new(result);
        if (version is { } requested && result.Manifest is { } manifest && manifest.Identity.Version != requested)
            return new(AppSourceResult.VersionMismatch(slug, requested, manifest.Identity.Version));
        return new(result);
    }

    /// <summary>When set, <see cref="ResolveAsync"/> throws for the given slug.</summary>
    public Func<AppSlug, Exception?>? FailResolve { get; set; }

    /// <summary>
    /// Suppresses this fake's own version pre-check so a mismatched result reaches
    /// the engine, which owns the authoritative identity re-check.
    /// </summary>
    public bool SkipVersionCheck { get; set; }

    private sealed class NoCodeActivationHandle(AppIdentity identity) : IAppActivationHandle
    {
        public AppIdentity Identity { get; } = identity;

        public ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Activation must not load app code.");
    }
}
