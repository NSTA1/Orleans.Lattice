using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The <see cref="ITenantScopedMembershipStore"/> half of the directory: tenant
/// counts, the paged tenant group listing, the cascading group removal and the
/// tenant purge. Every operation is a prefix scan or range count over the
/// tenant's slice of the <c>sys-membership-*</c> trees.
/// </summary>
internal sealed partial class LatticeMembershipDirectory
{
    /// <summary>
    /// Keys deleted per scan pass. Each pass re-scans the prefix from its start,
    /// so a pass never continues an enumeration over rows it has just removed.
    /// </summary>
    internal const int RemovalBatchSize = 256;

    /// <inheritdoc />
    public async Task<int> CountTenantGroupsAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var prefix = TenantGroupPrefix(tenant);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Groups.CountAsync(prefix, PrefixUpperBound(prefix), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> CountTenantEdgesAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var prefix = EdgeScopePrefix(MembershipConstants.ReverseEdge, TenantGroupPrefix(tenant));
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Edges.CountAsync(prefix, PrefixUpperBound(prefix), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<TenantGroupPage> ListTenantGroupsAsync(
        TenantId tenant,
        string? afterGroupId,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        var prefix = TenantGroupPrefix(tenant);
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(pageSize, ITenantScopedMembershipStore.MaxTenantGroupPageSize);
        if (afterGroupId is not null && !afterGroupId.StartsWith(prefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"The continuation group id '{afterGroupId}' is outside tenant '{tenant.Value}''s group scope.",
                nameof(afterGroupId));
        }

        // The smallest key strictly greater than afterGroupId.
        var start = afterGroupId is null ? prefix : afterGroupId + '\0';
        var groups = new List<MembershipGroup>(Math.Min(pageSize, 64));
        string? lastKey = null;
        var more = false;

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Groups
                .ScanEntriesAsync<MembershipGroup>(start, PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is not { } group)
                {
                    continue;
                }

                if (groups.Count == pageSize)
                {
                    more = true;
                    break;
                }

                groups.Add(group);
                lastKey = entry.Key;
            }
        }

        return new TenantGroupPage(groups, more ? lastKey : null);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<MembershipEdge>> RemoveGroupCascadeAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var removed = new List<MembershipEdge>();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // Its parents (forward rows keyed by the group as member), then its
            // members (reverse rows keyed by the group as parent).
            await DrainEdgesAsync(ForwardPrefix(groupId), removed, cancellationToken).ConfigureAwait(false);
            await DrainEdgesAsync(ReversePrefix(groupId), removed, cancellationToken).ConfigureAwait(false);

            // The record goes last: an interrupted call leaves it in place, so the
            // group is still visible and a re-run finds any edge left behind.
            await Groups.DeleteAsync(groupId, cancellationToken).ConfigureAwait(false);
        }

        return removed;
    }

    /// <inheritdoc />
    public async Task<TenantMembershipPurgeResult> PurgeTenantAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var groupPrefix = TenantGroupPrefix(tenant);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        int edgesRemoved;
        int groupsRemoved;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // Edges whose member is one of the tenant's groups, then edges whose
            // group is. An edge between two of the tenant's groups is removed by
            // the first pass (both rows), so the second never counts it again.
            edgesRemoved = await DrainEdgesAsync(
                EdgeScopePrefix(MembershipConstants.ForwardEdge, groupPrefix), removed: null, cancellationToken).ConfigureAwait(false);
            edgesRemoved += await DrainEdgesAsync(
                EdgeScopePrefix(MembershipConstants.ReverseEdge, groupPrefix), removed: null, cancellationToken).ConfigureAwait(false);

            groupsRemoved = await DrainKeysAsync(Groups, groupPrefix, cancellationToken).ConfigureAwait(false);
        }

        return new TenantMembershipPurgeResult(groupsRemoved, edgesRemoved);
    }

    /// <summary>
    /// Removes every edge row under <paramref name="prefix"/> (a forward or reverse
    /// edge prefix) together with its counterpart row. The counterpart is removed
    /// first, so the row that located it survives an interruption and a re-run
    /// finds the edge again; removing the located row first could orphan a
    /// counterpart outside every prefix the purge scans. Must run under system
    /// origin.
    /// </summary>
    /// <returns>The number of edges removed.</returns>
    private async Task<int> DrainEdgesAsync(string prefix, List<MembershipEdge>? removed, CancellationToken cancellationToken)
    {
        var upper = PrefixUpperBound(prefix);
        var batch = new List<string>(RemovalBatchSize);
        var count = 0;
        string? stalledAt = null;

        while (true)
        {
            await CollectKeysAsync(Edges, prefix, upper, batch, cancellationToken).ConfigureAwait(false);
            if (batch.Count == 0)
            {
                return count;
            }

            var progressed = false;
            foreach (var key in batch)
            {
                if (TryParseEdgeKey(key, out var edge))
                {
                    var counterpart = key[0] == MembershipConstants.ForwardEdge
                        ? ReverseKey(edge.GroupId, edge.MemberId)
                        : ForwardKey(edge.MemberId, edge.GroupId);
                    await Edges.DeleteAsync(counterpart, cancellationToken).ConfigureAwait(false);
                }

                // A row with no parsable edge has no counterpart; it still lies in
                // the scope being cleared, so it is removed rather than re-scanned
                // forever.
                if (await Edges.DeleteAsync(key, cancellationToken).ConfigureAwait(false))
                {
                    progressed = true;
                    if (edge.GroupId is not null)
                    {
                        count++;
                        removed?.Add(edge);
                    }
                }
            }

            stalledAt = EnsureProgress(progressed, batch[0], stalledAt, prefix);
        }
    }

    /// <summary>
    /// Fails loudly, rather than spinning, when two consecutive passes over the same
    /// range removed nothing and re-read the same first key. A single pass that
    /// removes nothing is tolerated: a concurrent remover may have taken the rows.
    /// </summary>
    /// <returns>The stall marker for the next pass.</returns>
    private static string? EnsureProgress(bool progressed, string firstKey, string? stalledAt, string prefix)
    {
        if (progressed)
        {
            return null;
        }

        if (string.Equals(stalledAt, firstKey, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Removing the membership rows under '{prefix}' made no progress: the key '{firstKey}' is still listed "
                + "after it was deleted. The operation is resumable; re-run it once the tree is healthy.");
        }

        return firstKey;
    }

    /// <summary>Removes every key under <paramref name="prefix"/> in <paramref name="tree"/>. Must run under system origin.</summary>
    /// <returns>The number of keys removed.</returns>
    private static async Task<int> DrainKeysAsync(ILattice tree, string prefix, CancellationToken cancellationToken)
    {
        var upper = PrefixUpperBound(prefix);
        var batch = new List<string>(RemovalBatchSize);
        var count = 0;
        string? stalledAt = null;

        while (true)
        {
            await CollectKeysAsync(tree, prefix, upper, batch, cancellationToken).ConfigureAwait(false);
            if (batch.Count == 0)
            {
                return count;
            }

            var progressed = false;
            foreach (var key in batch)
            {
                if (await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false))
                {
                    progressed = true;
                    count++;
                }
            }

            stalledAt = EnsureProgress(progressed, batch[0], stalledAt, prefix);
        }
    }

    /// <summary>Fills <paramref name="batch"/> with up to <see cref="RemovalBatchSize"/> keys from the start of the range.</summary>
    private static async Task CollectKeysAsync(
        ILattice tree,
        string prefix,
        string? upper,
        List<string> batch,
        CancellationToken cancellationToken)
    {
        batch.Clear();

        // ScanKeysAsync: see WalkForwardClosureAsync. A truncated scan only
        // shortens this pass; the next pass re-scans from the prefix start.
        await foreach (var key in tree
            .ScanKeysAsync(prefix, upper, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            batch.Add(key);
            if (batch.Count == RemovalBatchSize)
            {
                break;
            }
        }
    }

    /// <summary>
    /// The group-id prefix of <paramref name="tenant"/>'s scope, <c>t/{tenant}/</c>,
    /// after refusing the uninitialised and the reserved default tenant.
    /// </summary>
    internal static string TenantGroupPrefix(TenantId tenant)
    {
        if (tenant.Value is null || !TenantId.IsValid(tenant.Value.AsSpan()))
        {
            throw new ArgumentException("The tenant must be an initialised tenant id.", nameof(tenant));
        }

        if (tenant.IsDefault)
        {
            throw new ArgumentException(
                $"The reserved '{TenantId.DefaultId}' tenant has no tenant groups; its access is operator-administered.",
                nameof(tenant));
        }

        return LatticeTenantTrees.ComposePrefix(tenant);
    }

    /// <summary>The edge-key prefix of every row whose first id starts with <paramref name="idPrefix"/>.</summary>
    internal static string EdgeScopePrefix(char direction, string idPrefix) =>
        string.Concat(direction.ToString(), MembershipConstants.EdgeSeparator.ToString(), idPrefix);

    /// <summary>
    /// Parses a forward (<c>f|member|group</c>) or reverse (<c>r|group|member</c>)
    /// edge key into its edge. Returns <c>false</c> for a key that is not an edge
    /// key.
    /// </summary>
    internal static bool TryParseEdgeKey(string key, out MembershipEdge edge)
    {
        edge = default;
        if (key.Length < 2 || key[1] != MembershipConstants.EdgeSeparator)
        {
            return false;
        }

        var secondSep = key.IndexOf(MembershipConstants.EdgeSeparator, 2);
        if (secondSep < 0)
        {
            return false;
        }

        var first = key[2..secondSep];
        var second = key[(secondSep + 1)..];
        switch (key[0])
        {
            case MembershipConstants.ForwardEdge:
                edge = new MembershipEdge(second, first);
                return true;
            case MembershipConstants.ReverseEdge:
                edge = new MembershipEdge(first, second);
                return true;
            default:
                return false;
        }
    }
}
