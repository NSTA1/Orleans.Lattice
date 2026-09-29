using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// The Replication area's address completions: region ids (this region and every
/// peer) and enrolled tree ids, in replication scope. Free text matches either by
/// substring; <c>a/</c> completes the enrolled trees of apps whose slug starts with
/// the text; a literal <c>/replication/trees/...</c> completes tree paths. It reads
/// the data source's cached reports, so it never dials more than once per cache
/// lifetime.
/// </summary>
internal sealed class ReplicationCompletionSource : IAddressCompletionSource
{
    private readonly ReplicationDataSource _data;

    /// <summary>Creates the source over the circuit's data source.</summary>
    /// <param name="data">The circuit's replication data source.</param>
    public ReplicationCompletionSource(ReplicationDataSource data)
    {
        ArgumentNullException.ThrowIfNull(data);
        _data = data;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        var estate = (await _data.GetEstateAsync(refresh: false, cancellationToken).ConfigureAwait(false)).Value;
        var config = (await _data.GetConfigAsync(refresh: false, cancellationToken).ConfigureAwait(false)).Value;
        var tenant = query.Current.Tenant;

        var trees = (config?.Trees.Where(tree => tree.Enabled).Select(tree => tree.TreeId) ?? [])
            .Concat(estate?.Links.Select(link => link.TreeId) ?? [])
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        var results = new List<AddressCompletion>(query.Limit);
        switch (query.Mode)
        {
            case AddressQueryMode.Search:
                if (estate is not null)
                {
                    AddRegions(results, estate, query, tenant);
                }

                AddTrees(results, trees.Where(tree => tree.Contains(query.Text, StringComparison.OrdinalIgnoreCase)), query, tenant);
                break;

            case AddressQueryMode.App:
                AddTrees(
                    results,
                    trees.Where(tree => ReplicationTreeOwnership.TryGetAppSlug(tree, out var slug)
                        && slug.StartsWith(query.Text, StringComparison.OrdinalIgnoreCase)),
                    query,
                    tenant);
                break;

            case AddressQueryMode.Address:
                var typed = query.Text;
                AddTrees(
                    results,
                    trees.Where(tree => ReplicationAddresses.ForTree(tree) is { } target
                        && target.Format().StartsWith(typed, StringComparison.OrdinalIgnoreCase)),
                    query,
                    tenant);
                break;
        }

        return results;
    }

    private static void AddRegions(List<AddressCompletion> results, ReplicationEstate estate, AddressQuery query, string? tenant)
    {
        if (estate.LocalRegionId.Length > 0 && estate.LocalRegionId.Contains(query.Text, StringComparison.OrdinalIgnoreCase))
        {
            results.Add(new AddressCompletion(estate.LocalRegionId, ReplicationAddresses.Estate.WithTenant(tenant), "This region"));
        }

        foreach (var region in estate.PeerRegions)
        {
            if (results.Count >= query.Limit)
            {
                return;
            }

            if (region.Contains(query.Text, StringComparison.OrdinalIgnoreCase))
            {
                results.Add(new AddressCompletion(region, ReplicationAddresses.ForRegion(region).WithTenant(tenant), "Peer region"));
            }
        }
    }

    private static void AddTrees(List<AddressCompletion> results, IEnumerable<string> trees, AddressQuery query, string? tenant)
    {
        foreach (var tree in trees)
        {
            if (results.Count >= query.Limit)
            {
                return;
            }

            if (ReplicationAddresses.ForTree(tree) is { } target)
            {
                results.Add(new AddressCompletion(tree, target.WithTenant(tenant), "Replicated tree"));
            }
        }
    }
}
