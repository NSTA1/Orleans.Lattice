using System.Globalization;

namespace Orleans.Lattice.Views;

/// <summary>
/// Validates a materialised view's logical name. The name is not merely a label:
/// it is the view maintainer's grain key, and it is interpolated into the view
/// tree id, which is itself the <see cref="ILattice"/> grain key and is carried
/// into <c>ShardRootGrain</c>'s composite key
/// (<c>{treeId}/{shardIndex}</c>) - a <b>persistent</b> grain. A name is
/// therefore held to the same contract as any other grain-key part.
/// </summary>
/// <remarks>
/// <para>
/// <b>Storage safety.</b> Azure Table grain storage carries a grain key into the
/// Partition/Row key columns and the request URL, which reject the control
/// characters <c>0x00-0x1F</c> / <c>0x7F-0x9F</c> and the characters <c>/</c>,
/// <c>\</c>, <c>#</c> and <c>?</c>. A view named <c>a/b</c> would yield the
/// persistent shard-root key <c>view-a/b/0</c>. The historical failure mode is an
/// opaque HTTP 400 on read/write state that no in-memory test storage reproduces
/// (see <c>.github/instructions/grains.instructions.md</c> and issue #1529).
/// </para>
/// <para>
/// <b>Unambiguity.</b> The generation suffix
/// (<see cref="LatticeViewTrees.GenerationSeparator"/>) is appended to the name,
/// so a name that itself contains the separator would make the composed id
/// ambiguous - <c>orders</c> at generation 2 and a view literally named
/// <c>orders{sep}g2</c> would produce one id, and therefore one grain identity
/// and one persistent state row.
/// </para>
/// <para>
/// <b>Namespace containment.</b> Rejecting <c>/</c> also closes a tenancy hazard
/// for free: without it a caller could name a view <c>t/other/orders</c>, and
/// tenant-aware composition of the view tree id would place the tree inside
/// another tenant's reserved <see cref="LatticeTenantTrees.SegmentPrefix"/>
/// namespace. No separate tenant-prefix rule is needed, because a well-formed
/// tenant segment cannot exist without a slash.
/// </para>
/// <para>
/// Validation is applied where a view is <em>created</em>, never where a
/// previously-persisted registration is rehydrated: adopting this rule must not
/// strand a view an older build already created. A legacy name is reported by
/// the rehydration path instead, so it is visible without breaking a running
/// deployment.
/// </para>
/// </remarks>
internal static class ViewNameValidator
{
    /// <summary>
    /// The characters a keyed storage backend rejects in a grain primary key, and
    /// which therefore may not appear in a view name.
    /// </summary>
    private static readonly char[] StorageUnsafeCharacters = ['/', '\\', '#', '?'];

    /// <summary>
    /// Throws <see cref="ArgumentException"/> when <paramref name="viewName"/> is
    /// null, empty, or contains a character that would make the composed view tree
    /// id unusable or ambiguous as a grain key.
    /// </summary>
    /// <param name="viewName">The candidate view name.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException">The name is null, empty, or invalid.</exception>
    public static void ThrowIfInvalid(string viewName, string paramName = "viewName")
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName, paramName);

        if (!TryValidate(viewName, out var reason))
        {
            throw new ArgumentException(reason, paramName);
        }
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> when the tenant-local portion of
    /// <paramref name="viewName"/> is invalid, ignoring a leading
    /// <c>t/{tenant}/</c> segment the tenancy layer itself composed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this at any seam reached <em>after</em> tenant composition. Composition
    /// introduces the very <c>/</c> this validator rejects, so applying the plain
    /// <see cref="ThrowIfInvalid(string, string)"/> to a composed name refuses every
    /// tenant-scoped view outright - the name the caller supplied is legal and the
    /// separator belongs to the platform (issue #1707).
    /// </para>
    /// <para>
    /// This does not weaken the rule. The caller-supplied name is validated whole,
    /// before composition, at the facade entry point, which is what stops a caller
    /// naming a view <c>t/other/orders</c> and having composition plant its tree in
    /// another tenant's namespace. By the time a name reaches here that check has
    /// already run, so only the platform's own prefix is being excused. A bare,
    /// uncomposed name is validated unchanged, so a non-tenancy cluster behaves
    /// exactly as before.
    /// </para>
    /// </remarks>
    /// <param name="viewName">The possibly tenant-composed view name.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException">The tenant-local name is null, empty, or invalid.</exception>
    public static void ThrowIfComposedInvalid(string viewName, string paramName = "viewName")
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName, paramName);

        var local = LatticeTenantTrees.LocalName(viewName.AsSpan());
        var localName = local.Length == viewName.Length ? viewName : new string(local);

        ThrowIfInvalid(localName, paramName);
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="viewName"/> is a legal view name;
    /// otherwise <c>false</c> with <paramref name="reason"/> describing the first
    /// violation. Used by the rehydration path, which reports a legacy name rather
    /// than refusing to restore it.
    /// </summary>
    /// <param name="viewName">The candidate view name. Must not be <c>null</c> or empty.</param>
    /// <param name="reason">The first violation found, or <c>null</c> when the name is legal.</param>
    /// <returns><c>true</c> when the name is legal; otherwise <c>false</c>.</returns>
    public static bool TryValidate(string viewName, out string? reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);

        foreach (var c in viewName)
        {
            if (char.IsControl(c))
            {
                reason = string.Format(
                    CultureInfo.InvariantCulture,
                    "View name '{0}' contains the control character U+{1:X4}. A view name becomes part of a "
                    + "persistent grain's key, and keyed storage backends reject control characters there.",
                    Describe(viewName),
                    (int)c);
                return false;
            }
        }

        var unsafeIndex = viewName.IndexOfAny(StorageUnsafeCharacters);
        if (unsafeIndex >= 0)
        {
            reason = string.Format(
                CultureInfo.InvariantCulture,
                "View name '{0}' contains the reserved character '{1}'. A view name becomes part of a persistent "
                + "grain's key, and keyed storage backends (Azure Table grain storage in particular) reject "
                + "'/', '\\', '#' and '?' there. Choose a name without them.",
                Describe(viewName),
                viewName[unsafeIndex]);
            return false;
        }

        if (viewName.IndexOf(LatticeViewTrees.GenerationSeparator) >= 0)
        {
            reason = string.Format(
                CultureInfo.InvariantCulture,
                "View name '{0}' contains the reserved generation separator '{1}'. The separator distinguishes a "
                + "view's rebuild generations in its tree id, so a name carrying it would make two different views "
                + "resolve to the same tree. Choose a name without it.",
                Describe(viewName),
                LatticeViewTrees.GenerationSeparator);
            return false;
        }

        reason = null;
        return true;
    }

    /// <summary>
    /// Renders a name for an error message with control characters escaped, so a
    /// diagnostic is readable and cannot itself smuggle control bytes into a log.
    /// </summary>
    private static string Describe(string viewName)
    {
        if (!viewName.Any(char.IsControl))
        {
            return viewName;
        }

        return string.Concat(viewName.Select(c =>
            char.IsControl(c)
                ? string.Format(CultureInfo.InvariantCulture, "\\u{0:X4}", (int)c)
                : c.ToString()));
    }
}
