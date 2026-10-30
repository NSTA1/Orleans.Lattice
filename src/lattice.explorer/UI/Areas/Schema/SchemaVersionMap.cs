using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A tree's version catalog read against its version config: per family, the
/// versions the catalog names, how a value stamped with each reads at the target,
/// and what every registered hop changes. It mirrors the cluster's read path - a
/// value below the target follows the hop registered for its version, then the
/// next, until it reaches the target - and decides nothing the cluster does not.
/// </summary>
/// <remarks>
/// The catalog names registered versions, not the versions stored values carry:
/// the cluster does not report those, so the map says what would happen to a value
/// stamped with a version, not that such a value exists.
/// </remarks>
internal sealed class SchemaVersionMap
{
    private readonly Dictionary<(uint SchemaId, uint FromVersion), LatticeSchemaRuntimeUpcaster> _hops;
    private readonly HashSet<(uint SchemaId, uint Version)> _descriptors;
    private readonly LatticeSchemaVersionConfig? _config;

    private SchemaVersionMap(
        Dictionary<(uint, uint), LatticeSchemaRuntimeUpcaster> hops,
        HashSet<(uint, uint)> descriptors,
        LatticeSchemaVersionConfig? config,
        IReadOnlyList<SchemaVersionFamily> families)
    {
        _hops = hops;
        _descriptors = descriptors;
        _config = config;
        Families = families;
    }

    /// <summary>Each family the catalog or the config names; the configured family first.</summary>
    public IReadOnlyList<SchemaVersionFamily> Families { get; }

    /// <summary>Whether the catalog names no version and no hop.</summary>
    public bool IsEmpty => _descriptors.Count == 0 && _hops.Count == 0;

    /// <summary>Reads <paramref name="catalog"/> against <paramref name="config"/>.</summary>
    /// <param name="catalog">The tree's catalog, as the cluster reports it.</param>
    /// <param name="config">The tree's version config; <see langword="null"/> when it is unversioned.</param>
    /// <returns>The map.</returns>
    public static SchemaVersionMap Build(LatticeSchemaRuntimeRegistration catalog, LatticeSchemaVersionConfig? config)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        var supported = new HashSet<string>(catalog.SupportedTransformIds ?? [], StringComparer.Ordinal);
        var hops = new Dictionary<(uint, uint), LatticeSchemaRuntimeUpcaster>();
        foreach (var hop in catalog.Upcasters ?? [])
        {
            // The cluster keys hops by family and source version and refuses a conflicting
            // second one, so a duplicate here is the same hop repeated.
            hops.TryAdd((hop.SchemaId, hop.FromVersion), hop);
        }

        var names = new Dictionary<(uint, uint), string?>();
        foreach (var descriptor in catalog.Descriptors ?? [])
        {
            names.TryAdd((descriptor.SchemaId, descriptor.Version), descriptor.Name);
        }

        var families = new SortedSet<uint>(names.Keys.Select(key => key.Item1).Concat(hops.Keys.Select(key => key.Item1)));
        if (config is { } configured)
        {
            families.Add(configured.SchemaId);
        }

        var map = new SchemaVersionMap(hops, [.. names.Keys], config, []);
        var result = new List<SchemaVersionFamily>(families.Count);
        foreach (var family in families.OrderBy(id => config is { } c && c.SchemaId == id ? 0 : 1).ThenBy(id => id))
        {
            var isConfigured = config is { } c && c.SchemaId == family;
            var versions = new SortedSet<uint>();
            foreach (var key in names.Keys.Where(key => key.Item1 == family))
            {
                versions.Add(key.Item2);
            }

            foreach (var hop in hops.Values.Where(hop => hop.SchemaId == family))
            {
                versions.Add(hop.FromVersion);
                versions.Add(hop.ToVersion);
            }

            if (isConfigured)
            {
                versions.Add(config!.Value.TargetVersion);
            }

            var rows = versions
                .Select(version => map.Row(family, version, isConfigured, names.TryGetValue((family, version), out var name) ? name : null))
                .ToArray();
            var familyHops = hops.Values
                .Where(hop => hop.SchemaId == family)
                .OrderBy(hop => hop.FromVersion)
                .Select(hop => new SchemaVersionHop(
                    hop.FromVersion,
                    hop.ToVersion,
                    hop.Transform is { } transform ? SchemaTransformText.Steps(transform) : [],
                    hop.Transform is null ? hop.TransformId : null,
                    hop.Transform is not null || (hop.TransformId is { } id && supported.Contains(id))))
                .ToArray();
            result.Add(new SchemaVersionFamily(family, isConfigured, rows, familyHops));
        }

        return new SchemaVersionMap(hops, map._descriptors, config, result);
    }

    /// <summary>
    /// Whether a value stamped with <paramref name="fromVersion"/> can be upgraded to
    /// <paramref name="toVersion"/>, as the cluster's read path decides it.
    /// </summary>
    /// <param name="schemaId">The family.</param>
    /// <param name="fromVersion">The stamped version.</param>
    /// <param name="toVersion">The version to reach.</param>
    /// <returns><see langword="true"/> when the versions are equal or a chain of hops leads there.</returns>
    public bool CanUpgrade(uint schemaId, uint fromVersion, uint toVersion) => Route(schemaId, fromVersion, toVersion) is not null;

    /// <summary>Whether the catalog names <paramref name="version"/> of <paramref name="schemaId"/>.</summary>
    /// <param name="schemaId">The family.</param>
    /// <param name="version">The version.</param>
    /// <returns><see langword="true"/> when a descriptor names it.</returns>
    public bool IsRegistered(uint schemaId, uint version) => _descriptors.Contains((schemaId, version));

    /// <summary>
    /// The versions of the configured family the catalog names, the current target
    /// among them, that would have no route to <paramref name="newTarget"/>: a value
    /// stamped with one could not be read once the target advanced there.
    /// </summary>
    /// <param name="newTarget">The target being considered.</param>
    /// <returns>The versions, ascending; empty when the tree is unversioned.</returns>
    public IReadOnlyList<uint> StrandedBy(uint newTarget)
    {
        if (_config is not { } config)
        {
            return [];
        }

        return Families
            .Where(family => family.IsConfigured)
            .SelectMany(family => family.Versions)
            .Select(row => row.Version)
            .Where(version => version < newTarget && !CanUpgrade(config.SchemaId, version, newTarget))
            .ToArray();
    }

    private SchemaVersionRow Row(uint family, uint version, bool isConfigured, string? name)
    {
        if (!isConfigured)
        {
            return new SchemaVersionRow(version, name, SchemaVersionReach.Unconfigured, []);
        }

        var target = _config!.Value.TargetVersion;
        if (version == target)
        {
            return new SchemaVersionRow(version, name, SchemaVersionReach.Target, []);
        }

        if (version > target)
        {
            return new SchemaVersionRow(version, name, SchemaVersionReach.AboveTarget, []);
        }

        return Route(family, version, target) is { } route
            ? new SchemaVersionRow(version, name, SchemaVersionReach.Upgraded, route)
            : new SchemaVersionRow(version, name, SchemaVersionReach.NoPath, []);
    }

    private List<uint>? Route(uint schemaId, uint fromVersion, uint toVersion)
    {
        if (toVersion < fromVersion)
        {
            return null;
        }

        var route = new List<uint> { fromVersion };
        var current = fromVersion;
        while (current < toVersion)
        {
            // Every hop strictly raises the version, so this ends within the hop count.
            if (!_hops.TryGetValue((schemaId, current), out var hop) || hop.ToVersion > toVersion || hop.ToVersion <= current)
            {
                return null;
            }

            current = hop.ToVersion;
            route.Add(current);
        }

        return route;
    }
}
