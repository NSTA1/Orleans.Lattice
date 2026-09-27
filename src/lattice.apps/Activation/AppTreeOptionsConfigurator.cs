using System.Collections.Frozen;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Applies an in-image app manifest's per-tree <see cref="AppTreeDeclaration.SoftDeleteDuration"/>
/// to the named <see cref="LatticeOptions"/> of that app's structural trees
/// (<c>a/{slug}/{tree}</c> in every tenant), so uninstalling or dropping the tree honours the
/// declared recovery window. Registered as a configure step, so an operator's per-tree
/// configuration registered after <c>AddLatticeApps</c> still wins.
/// </summary>
internal sealed class AppTreeOptionsConfigurator : IConfigureNamedOptions<LatticeOptions>
{
    private readonly Lazy<FrozenDictionary<string, TimeSpan>> _softDeleteDurations;

    public AppTreeOptionsConfigurator(InImageAppManifestCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        _softDeleteDurations = new Lazy<FrozenDictionary<string, TimeSpan>>(
            () => Build(catalog.Manifests),
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public void Configure(LatticeOptions options) => Configure(Options.DefaultName, options);

    public void Configure(string? name, LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!AppActivationTreeNames.TrySplitStructuralTree(name, out var slug, out var tree))
        {
            return;
        }

        var durations = _softDeleteDurations.Value;
        if (durations.Count != 0
            && durations.TryGetValue(string.Concat(slug, "/", tree), out var duration))
        {
            options.SoftDeleteDuration = duration;
        }
    }

    private static FrozenDictionary<string, TimeSpan> Build(IReadOnlyList<AppManifest> manifests)
    {
        Dictionary<string, TimeSpan>? durations = null;
        foreach (var manifest in manifests)
        {
            foreach (var tree in manifest.Trees)
            {
                if (tree.AdoptedTreeId is null && tree.SoftDeleteDuration is { } duration)
                {
                    (durations ??= new(StringComparer.Ordinal))[string.Concat(manifest.Identity.Slug.Value, "/", tree.Name)] = duration;
                }
            }
        }

        return durations is null
            ? FrozenDictionary<string, TimeSpan>.Empty
            : durations.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
