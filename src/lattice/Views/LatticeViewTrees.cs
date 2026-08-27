using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Views;

/// <summary>
/// The single place that composes, classifies, and parses materialised-view tree
/// ids, mirroring how <see cref="LatticeTenantTrees"/> centralises the reserved
/// <c>t/</c> tenant namespace. A view tree is named
/// <c>{tenantSegment}view-{name}</c>, where the tenant segment is empty for a
/// cluster-global (default-tenant) view and <c>t/{tenant}/</c> for a view a
/// tenant owns, so a view's id carries both its kind and its owner.
/// </summary>
/// <remarks>
/// <para>
/// Centralising this is what keeps <em>classification</em> correct after
/// <em>composition</em>. The view prefix is a leading-prefix marker, and scoping
/// a name into <c>t/{tenant}/</c> moves that marker off the front of the string.
/// Every ad-hoc <c>treeId.StartsWith(ViewTreePrefix)</c> therefore turns silently
/// negative for a tenant's view - retiring, among others, the guards that stop a
/// caller writing to a view tree directly. Routing every decision through
/// <see cref="IsViewTree"/> (which classifies the tenant-local name) makes the
/// answer independent of whether the id has been composed.
/// </para>
/// <para>
/// Every operation is a no-op string-wise for a non-tenant id, so a cluster with
/// tenancy off sees exactly the ids it saw before: <c>view-{name}</c> in,
/// <c>view-{name}</c> out, with the same string reference wherever possible.
/// </para>
/// </remarks>
internal static class LatticeViewTrees
{
    /// <summary>
    /// The reserved prefix (<c>view-</c>) that marks the tenant-local portion of
    /// every materialised-view tree id.
    /// </summary>
    public const string SegmentPrefix = LatticeConstants.ViewTreePrefix;

    /// <summary>
    /// Separates a view tree id from its explicit generation suffix
    /// (<c>{sep}g{N}</c>), which a shadow-swap rebuild appends to address a
    /// non-active generation.
    /// </summary>
    /// <remarks>
    /// Storage-safe by construction. The composed tree id is an Orleans grain
    /// primary key and is carried into <c>ShardRootGrain</c>'s composite key - a
    /// persistent grain - and keyed storage backends reject <c>/</c>, <c>\</c>,
    /// <c>#</c> and <c>?</c> there. <c>~</c> is outside that set and rare enough
    /// in identifiers to be reserved cheaply, which
    /// <see cref="ViewNameValidator"/> does, keeping the composed id unambiguous.
    /// </remarks>
    public const char GenerationSeparator = '~';

    /// <summary>
    /// The generation separator used before the storage-safe one was adopted.
    /// A view that was already past generation 0 keeps addressing its existing
    /// generations through this character, so the change strands no data; the
    /// next rebuild moves it onto <see cref="GenerationSeparator"/>. Still parsed
    /// so a legacy id resolves to its view name.
    /// </summary>
    public const char LegacyGenerationSeparator = '#';

    /// <summary>
    /// Composes the view tree id for <paramref name="viewName"/>, preserving any
    /// tenant segment the name carries: <c>orders</c> yields <c>view-orders</c>,
    /// and <c>t/acme/orders</c> yields <c>t/acme/view-orders</c>, so a tenant's
    /// view tree lands inside that tenant's own structural namespace and is
    /// owned, enumerated, and cascaded exactly like its other trees.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A tenant segment on the name can only have been put there by tenant
    /// scoping: <see cref="ViewNameValidator"/> rejects a caller-supplied name
    /// containing <c>/</c>, so a caller cannot name another tenant's namespace and
    /// have the tree planted there.
    /// </para>
    /// <para>
    /// Marked as a grain-key builder because the composed id is an Orleans grain
    /// primary key: the reflection-driven storage-safety guard in the shared
    /// testing library audits it automatically.
    /// </para>
    /// </remarks>
    /// <param name="viewName">The (possibly tenant-qualified) view name. Must not be <c>null</c> or empty.</param>
    /// <returns>The view tree id.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    [GrainKeyBuilder]
    public static string ComposeTreeId(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);

        if (!LatticeTenantTrees.TryGetTenant(viewName, out var tenant))
        {
            return string.Concat(SegmentPrefix, viewName);
        }

        // Written once into an exactly-sized buffer. Concatenating the pieces
        // would materialise the intermediate local name and the intermediate
        // "view-{local}" only to copy them again; this is the same result with a
        // single allocation, which matters because a maintainer resolves its tree
        // id on every drain.
        var local = LatticeTenantTrees.LocalName(viewName.AsSpan());
        var tenantValue = tenant.Value!;
        var length = LatticeTenantTrees.SegmentPrefix.Length
            + tenantValue.Length
            + 1
            + SegmentPrefix.Length
            + local.Length;

        return string.Create(length, (tenantValue, viewName), static (destination, state) =>
        {
            var (tenantId, name) = state;
            var localName = LatticeTenantTrees.LocalName(name.AsSpan());

            var at = 0;
            LatticeTenantTrees.SegmentPrefix.CopyTo(destination[at..]);
            at += LatticeTenantTrees.SegmentPrefix.Length;
            tenantId.CopyTo(destination[at..]);
            at += tenantId.Length;
            destination[at++] = '/';
            SegmentPrefix.CopyTo(destination[at..]);
            at += SegmentPrefix.Length;
            localName.CopyTo(destination[at..]);
        });
    }

    /// <summary>
    /// Composes the view tree id for a specific shadow-swap generation.
    /// Generation <c>0</c> (and below) maps to the stable, unsuffixed id for
    /// backward compatibility; a higher generation appends <c>{sep}g{N}</c>.
    /// </summary>
    /// <param name="viewName">The view name. Must not be <c>null</c> or empty.</param>
    /// <param name="generation">The generation number.</param>
    /// <param name="useLegacySeparator">
    /// When <see langword="true"/>, addresses the generation through
    /// <see cref="LegacyGenerationSeparator"/> instead of
    /// <see cref="GenerationSeparator"/>, so a view that already built this
    /// generation under the old naming still resolves its existing tree.
    /// </param>
    /// <returns>The generation's view tree id.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    public static string ComposeTreeId(string viewName, long generation, bool useLegacySeparator = false)
    {
        var stable = ComposeTreeId(viewName);
        if (generation <= 0)
        {
            return stable;
        }

        var separator = useLegacySeparator ? LegacyGenerationSeparator : GenerationSeparator;
        return string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"{stable}{separator}g{generation}");
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="treeId"/> names a
    /// materialised-view tree, whether or not it has been tenant-composed. This
    /// is the classification every view guard and view-aware read path must use
    /// in place of a raw leading-prefix test.
    /// </summary>
    /// <remarks>
    /// Allocation-free: it classifies the tenant-local <em>slice</em> rather than
    /// materialising it. The view write and read guards run this on every
    /// mutation and every content read, so a copy here would be a per-operation
    /// allocation on the data plane.
    /// </remarks>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the id names a view tree; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static bool IsViewTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return IsViewTree(treeId.AsSpan());
    }

    /// <summary>
    /// Span overload of <see cref="IsViewTree(string)"/>. Allocation-free.
    /// </summary>
    /// <param name="treeId">The candidate tree id.</param>
    /// <returns><c>true</c> when the id names a view tree; otherwise <c>false</c>.</returns>
    public static bool IsViewTree(ReadOnlySpan<char> treeId)
    {
        // Fast path: an uncomposed view id (every id on a tenancy-off cluster)
        // answers on the leading prefix alone, with no tenant parse at all.
        if (treeId.StartsWith(SegmentPrefix, StringComparison.Ordinal))
        {
            return true;
        }

        return LatticeTenantTrees.LocalName(treeId).StartsWith(SegmentPrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Recovers the view name - the maintainer grain key, tenant segment
    /// included - from a view tree id, stripping the view prefix and any
    /// <c>#g{N}</c> generation suffix. The exact inverse of
    /// <see cref="ComposeTreeId(string)"/>: <c>view-orders</c> yields
    /// <c>orders</c> and <c>t/acme/view-orders#g2</c> yields <c>t/acme/orders</c>.
    /// </summary>
    /// <remarks>
    /// Returns <see cref="string.Empty"/> when <paramref name="treeId"/> carries
    /// no recoverable name, so a caller can distinguish an unusable id from a
    /// real one rather than dialing a maintainer keyed by an empty string.
    /// </remarks>
    /// <param name="treeId">The view tree id. Must not be <c>null</c>.</param>
    /// <returns>The view name, or <see cref="string.Empty"/> when none is recoverable.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static string ViewNameFromTreeId(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var full = treeId.AsSpan();
        if (!IsViewTree(full))
        {
            return string.Empty;
        }

        // Sliced, not copied: the intermediate local name is never materialised,
        // so the only allocation is the single result string.
        var tenantScoped = LatticeTenantTrees.TryGetTenant(treeId, out var tenant);
        var local = tenantScoped ? LatticeTenantTrees.LocalName(full) : full;

        var name = local[SegmentPrefix.Length..];

        // Either separator terminates the name: a view that predates the
        // storage-safe separator still addresses its existing generations through
        // the legacy one, so both must resolve to the same view name.
        var separator = name.IndexOfAny(GenerationSeparator, LegacyGenerationSeparator);
        if (separator >= 0)
        {
            name = name[..separator];
        }

        if (name.IsEmpty)
        {
            return string.Empty;
        }

        // One allocation either way: the composed form when the id is scoped, the
        // bare name otherwise.
        return tenantScoped
            ? LatticeTenantTrees.Compose(tenant, name)
            : new string(name);
    }
}
