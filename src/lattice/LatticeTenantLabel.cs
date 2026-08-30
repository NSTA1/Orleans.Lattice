using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// The single source of the derived <c>tenant</c> metric dimension. Every
/// <see cref="System.Diagnostics.Metrics"/> instrument published by Orleans.Lattice
/// and its add-on packages carries this dimension, so a telemetry query can be
/// scoped to one tenant without inspecting the <c>tree</c> label's value.
/// </summary>
/// <remarks>
/// <para>
/// <b>Always emitted.</b> The dimension is present on tenancy-on and tenancy-off
/// clusters alike, so a dashboard panel or a named query is byte-identical in both
/// deployment modes and there are no tenancy-on / tenancy-off query variants. On a
/// cluster with no tenancy add-on every tree id is bare, so every series resolves
/// to <see cref="DefaultTenant"/>.
/// </para>
/// <para>
/// <b>Why a <c>tree</c> regex is not a substitute.</b> Tenancy encodes the owning
/// tenant in the tree id (<see cref="LatticeTenantTrees.Compose(TenantId, string)"/>
/// produces <c>t/{tenantId}/{name}</c>) and re-derives ownership from that prefix.
/// Tenant <c>acme</c> maps cleanly to <c>tree=~"^t/acme/.*"</c>, but the default
/// tenant's adopted legacy ids are bare, so its matcher becomes
/// <c>tree!~"^t/.*"</c> - which also matches the <c>_lattice_</c> and <c>sys-</c>
/// platform namespaces and leaks platform-internal series into a tenant's view.
/// Tree ownership is a genuine three-way classification (tenant-owned,
/// default-adopted, platform-owned) that a single regex cannot reproduce, and an
/// instrument with no <c>tree</c> label cannot be scoped that way at all.
/// </para>
/// <para>
/// <b>Cardinality-neutral.</b> <c>tree -&gt; tenant</c> is a function, so the
/// derived label attaches to series that already exist rather than multiplying
/// them: two measurements that shared a series before still share one after,
/// because equal tree ids always derive equal tenant labels.
/// </para>
/// <para>
/// <b>Allocation-free on the measurement path.</b> <see cref="ForTree(string)"/>
/// classifies with ordinal span comparisons and returns one of two frozen
/// singletons for the platform and default cases - the overwhelming majority of
/// measurements on any cluster. A genuinely tenant-scoped id is resolved through a
/// per-tenant tag cache keyed by a <see cref="ReadOnlySpan{T}"/> alternate lookup,
/// so the tenant id substring is materialised once per tenant for the process
/// lifetime and never per measurement. The cache is bounded by the number of
/// distinct tenants, which is exactly the cardinality of the emitted label.
/// </para>
/// </remarks>
public static class LatticeTenantLabel
{
    /// <summary>
    /// The tag key carrying the derived owning tenant on every instrument. Every
    /// metrics class in every package must reference this constant rather than
    /// hard-coding the string.
    /// </summary>
    public const string TagTenant = "tenant";

    /// <summary>
    /// The reserved platform sentinel (<c>_platform_</c>): the <see cref="TagTenant"/>
    /// value for a series that belongs to the platform rather than to any tenant -
    /// the <c>_lattice_</c> system-internal and <c>sys-</c> system-data tree
    /// namespaces, and every instrument that carries no tree dimension at all.
    /// </summary>
    /// <remarks>
    /// Deliberately distinct from <see cref="DefaultTenant"/>: the default tenant
    /// adopts bare legacy tree ids and is a real, queryable tenant, whereas the
    /// sentinel names state no tenant may ever see. The value opens with an
    /// underscore, which <see cref="TenantId"/>'s grammar forbids, so it can never
    /// collide with a real tenant id.
    /// </remarks>
    public const string PlatformTenant = "_platform_";

    /// <summary>
    /// The <see cref="TagTenant"/> value for the reserved legacy-adoption tenant
    /// (<see cref="TenantId.DefaultId"/>): the owner of every bare, unsegmented
    /// tree id, and therefore of every series on a cluster with tenancy off.
    /// </summary>
    public const string DefaultTenant = TenantId.DefaultId;

    /// <summary>
    /// The frozen <see cref="TagTenant"/> tag naming the platform sentinel. Emit
    /// this on any instrument that carries no tree dimension, and on any
    /// platform-owned tree.
    /// </summary>
    public static readonly KeyValuePair<string, object?> Platform = new(TagTenant, PlatformTenant);

    /// <summary>
    /// The frozen <see cref="TagTenant"/> tag naming the default (legacy-adoption)
    /// tenant - the tag every series on a tenancy-off cluster carries.
    /// </summary>
    public static readonly KeyValuePair<string, object?> Default = new(TagTenant, DefaultTenant);

    /// <summary>
    /// Per-tenant frozen tags, keyed by tenant id. Bounded by the number of
    /// distinct tenants that have ever emitted a measurement in this process,
    /// which is exactly the cardinality the <see cref="TagTenant"/> label already
    /// carries, so the cache can never grow faster than the metric surface it
    /// serves.
    /// </summary>
    private static readonly ConcurrentDictionary<string, KeyValuePair<string, object?>> TenantTags =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Span-keyed view over <see cref="TenantTags"/>, so a tenant id sliced out of
    /// a tree id is looked up without first materialising it as a string. Cached
    /// in a static field because constructing the lookup is not free.
    /// </summary>
    private static readonly ConcurrentDictionary<string, KeyValuePair<string, object?>>.AlternateLookup<ReadOnlySpan<char>> TenantTagLookup =
        TenantTags.GetAlternateLookup<ReadOnlySpan<char>>();

    /// <summary>
    /// Derives the <see cref="TagTenant"/> tag for a tree id.
    /// </summary>
    /// <param name="treeId">
    /// The tree id whose owning tenant to derive. A <c>null</c> id classifies as
    /// the platform sentinel: a measurement must never throw, and an absent tree
    /// is definitionally not attributable to a tenant.
    /// </param>
    /// <returns>
    /// The owning tenant's tag for a well-formed <c>t/{tenantId}/{name}</c> id;
    /// <see cref="Platform"/> for a <c>_lattice_</c> or <c>sys-</c> id (or a
    /// malformed id in the reserved <c>t/</c> namespace); otherwise
    /// <see cref="Default"/>.
    /// </returns>
    public static KeyValuePair<string, object?> ForTree(string? treeId) =>
        treeId is null ? Platform : ForTree(treeId.AsSpan());

    /// <summary>
    /// Span overload of <see cref="ForTree(string)"/>, for a caller holding a
    /// slice of a larger id. Allocation-free for every already-seen tenant.
    /// </summary>
    /// <param name="treeId">The tree id whose owning tenant to derive.</param>
    /// <returns>The derived <see cref="TagTenant"/> tag.</returns>
    public static KeyValuePair<string, object?> ForTree(ReadOnlySpan<char> treeId)
    {
        // System namespaces are platform-owned and sit outside every tenant.
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            || treeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal))
        {
            return Platform;
        }

        if (!treeId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            // A bare, unsegmented legacy id is adopted by the default tenant. This
            // is every id on a tenancy-off cluster, so it is the path that must
            // stay free of both allocation and dictionary work.
            return Default;
        }

        var rest = treeId[LatticeTenantTrees.SegmentPrefix.Length..];
        var slash = rest.IndexOf('/');

        // A malformed id in the reserved t/ namespace is not a bare legacy id, so
        // it is not adopted by the default tenant; it classifies as platform-owned
        // and can never leak into a tenant's view.
        if (slash <= 0 || slash >= rest.Length - 1)
        {
            return Platform;
        }

        var idSpan = rest[..slash];
        if (!TenantId.IsValid(idSpan))
        {
            return Platform;
        }

        if (TenantTagLookup.TryGetValue(idSpan, out var tag))
        {
            return tag;
        }

        return AddTenantTag(idSpan);
    }

    /// <summary>
    /// Returns the frozen <see cref="TagTenant"/> tag for an already-parsed tenant.
    /// </summary>
    /// <param name="tenant">
    /// The owning tenant. The uninitialised <c>default(TenantId)</c> ("no tenant")
    /// resolves to <see cref="Platform"/>.
    /// </param>
    /// <returns>The tenant's frozen tag.</returns>
    public static KeyValuePair<string, object?> ForTenant(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            return Platform;
        }

        return TenantTags.TryGetValue(tenant.Value, out var tag)
            ? tag
            : TenantTags.GetOrAdd(tenant.Value, static id => new KeyValuePair<string, object?>(TagTenant, id));
    }

    /// <summary>
    /// Returns the derived <see cref="TagTenant"/> <em>value</em> for a tree id -
    /// the label a series for that tree carries.
    /// </summary>
    /// <param name="treeId">The tree id to classify. A <c>null</c> id resolves to <see cref="PlatformTenant"/>.</param>
    /// <returns>The tenant id, <see cref="DefaultTenant"/>, or <see cref="PlatformTenant"/>.</returns>
    public static string Resolve(string? treeId) => (string)ForTree(treeId).Value!;

    /// <summary>
    /// Wraps <paramref name="value"/> as an observable-gauge measurement carrying
    /// the <see cref="Platform"/> sentinel - the shape every instrument that has
    /// no tree dimension uses, so the <see cref="TagTenant"/> label is present on
    /// every series in the repository rather than only on tree-scoped ones.
    /// </summary>
    /// <typeparam name="T">The measurement's numeric type.</typeparam>
    /// <param name="value">The observed value.</param>
    /// <returns>A measurement tagged with the platform sentinel.</returns>
    public static Measurement<T> PlatformMeasurement<T>(T value) where T : struct =>
        new(value, Platform);

    private static KeyValuePair<string, object?> AddTenantTag(ReadOnlySpan<char> tenantId)
    {
        // Reached at most once per distinct tenant per process: the single string
        // materialisation is amortised over every subsequent measurement.
        var id = new string(tenantId);
        var tag = new KeyValuePair<string, object?>(TagTenant, id);
        return TenantTags.GetOrAdd(id, tag);
    }
}
