namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Decides whether an admin-set or member-set entry may count for a tenant once
/// delegated tenant access administration is enabled. The administration facades
/// refuse a foreign or malformed tenant-group entry on write (D4), but a record can
/// also arrive by replication or restore without passing through them, so the
/// compiled policy and the record's group-aware probes apply the same rule again
/// as defence in depth.
/// </summary>
internal static class TenantAccessEntries
{
    /// <summary>
    /// <c>true</c> when <paramref name="subjectId"/> occupies the reserved
    /// <see cref="LatticeTenantTrees.SegmentPrefix"/> namespace, so it names a
    /// tenant <em>group</em> rather than a principal. Group entries share the admin
    /// and member slot maps with subject ids, so an exact-id authorization probe
    /// must screen them out: a principal whose asserted <c>sub</c> is literally
    /// <c>t/{tenant}/{name}</c> would otherwise match a group entry directly and
    /// act as a tenant admin without ever belonging to the group. Every write seam
    /// already refuses a non-group id in this namespace; this is the read-side half
    /// of that rule. Allocation-free.
    /// </summary>
    /// <param name="subjectId">The subject id being authorized. A <c>null</c> id is not group-shaped.</param>
    /// <returns><c>true</c> when the id must not be matched against a slot map directly.</returns>
    public static bool IsGroupShapedSubject(string? subjectId) =>
        subjectId is not null
        && subjectId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal);

    /// <summary>
    /// <c>true</c> when <paramref name="entry"/> may count for
    /// <paramref name="tenant"/>: any entry outside the reserved
    /// <see cref="LatticeTenantTrees.SegmentPrefix"/> namespace (a user id or a
    /// cluster group), or a well-formed tenant group <c>t/{tenant}/{name}</c> of
    /// <paramref name="tenant"/> itself. Another tenant's group, and every malformed
    /// <c>t/</c> entry, never count. Allocation-free.
    /// </summary>
    /// <param name="entry">The admin or member entry. A <c>null</c> entry never counts.</param>
    /// <param name="tenant">The tenant that holds the entry.</param>
    /// <returns><c>true</c> when the entry may admit a subject to the tenant.</returns>
    public static bool IsAdmissible(string? entry, TenantId tenant)
    {
        if (entry is null)
        {
            return false;
        }

        if (!entry.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            return true;
        }

        if (tenant.Value is not { } owner)
        {
            return false;
        }

        var rest = entry.AsSpan(LatticeTenantTrees.SegmentPrefix.Length);
        return rest.Length > owner.Length + 1
            && rest.StartsWith(owner, StringComparison.Ordinal)
            && rest[owner.Length] == '/'
            && LatticeTenantGroupId.IsValidName(rest[(owner.Length + 1)..]);
    }
}
