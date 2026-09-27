using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Merges the replication intent declared by in-image app manifests into
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/>, additively: a tree the operator (or
/// another add-on) already enrolled keeps its entry and merge mode. Follows the merge pattern of
/// <c>ReplicateLatticeSystemTrees</c>, but never overwrites.
/// </summary>
/// <remarks>
/// <para>
/// Enrolled ids are the physical names an in-image app uses in the default tenant: its
/// structural <c>a/{slug}/{tree}</c> trees, or the adopted id for an adopted tree. Replication
/// membership is a static map evaluated at configuration time, so a tenant-composed
/// <c>t/{tenant}/a/{slug}/{tree}</c> tree, and an app that is not registered in the image, cannot
/// be enrolled here; enrol those explicitly.
/// </para>
/// <para>
/// When the replication add-on is not registered nothing resolves
/// <see cref="LatticeReplicationOptions"/>, so this post-configure is inert.
/// </para>
/// </remarks>
internal sealed class AppReplicationIntentPostConfigure : IPostConfigureOptions<LatticeReplicationOptions>
{
    private readonly InImageAppManifestCatalog _catalog;

    public AppReplicationIntentPostConfigure(InImageAppManifestCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        _catalog = catalog;
    }

    public void PostConfigure(string? name, LatticeReplicationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var existing = options.ReplicatedTrees;
        Dictionary<string, LatticeMergeMode>? merged = null;
        foreach (var manifest in _catalog.Manifests)
        {
            if (manifest.Replication is not { Length: > 0 } declarations)
            {
                continue;
            }

            foreach (var declaration in declarations)
            {
                var tree = Find(manifest, declaration.Tree);
                if (tree is null)
                {
                    continue;
                }

                var physical = tree.AdoptedTreeId ?? AppActivationTreeNames.LocalStructuralTree(manifest.Identity.Slug, tree.Name);
                if ((existing is not null && existing.ContainsKey(physical)) || (merged is not null && merged.ContainsKey(physical)))
                {
                    continue;
                }

                if (merged is null)
                {
                    merged = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
                    if (existing is not null)
                    {
                        foreach (var kv in existing)
                        {
                            merged[kv.Key] = kv.Value;
                        }
                    }
                }

                merged[physical] = declaration.MergeMode;
            }
        }

        if (merged is not null)
        {
            options.ReplicatedTrees = merged;
        }
    }

    private static AppTreeDeclaration? Find(AppManifest manifest, string name)
    {
        foreach (var tree in manifest.Trees)
        {
            if (string.Equals(tree.Name, name, StringComparison.Ordinal))
            {
                return tree;
            }
        }

        return null;
    }
}
