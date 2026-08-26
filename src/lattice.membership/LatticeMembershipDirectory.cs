using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The default <see cref="ILatticeMembershipDirectory"/>. Dogfoods two reserved
/// <c>ILattice</c> trees: <c>sys-membership-groups</c> holds JSON group records
/// keyed by id, and <c>sys-membership-edges</c> holds each membership edge twice
/// (a forward row keyed by member and a reverse row keyed by group) so both
/// <see cref="GroupsOfAsync"/> and <see cref="MembersOfAsync"/> are prefix
/// scans. Every mutation runs through the standard write path, so it is
/// observed by the resolution-cache invalidator and captured by the per-key
/// history view.
/// </summary>
internal sealed class LatticeMembershipDirectory(
    IGrainFactory grainFactory,
    MembershipInitializer initializer) : ILatticeMembershipDirectory
{
    private static readonly byte[] UserMarker = "u"u8.ToArray();
    private static readonly byte[] GroupMarker = "g"u8.ToArray();

    private ILattice Groups => grainFactory.GetGrain<ILattice>(MembershipConstants.GroupsTree);

    private ILattice Edges => grainFactory.GetGrain<ILattice>(MembershipConstants.EdgesTree);

    /// <inheritdoc />
    public async Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(group);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Groups.SetAsync(group.GroupId, group, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Groups.GetAsync<MembershipGroup>(groupId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<MembershipGroup> ListGroupsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Groups.ScanEntriesAsync<MembershipGroup>(cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (entry.Value is { } group)
                {
                    yield return group;
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Groups.DeleteAsync(groupId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        ArgumentNullException.ThrowIfNull(memberId);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var marker = memberKind == MembershipMemberKind.Group ? GroupMarker : UserMarker;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Edges.SetAsync(ForwardKey(memberId, groupId), marker, cancellationToken).ConfigureAwait(false);
            await Edges.SetAsync(ReverseKey(groupId, memberId), marker, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        ArgumentNullException.ThrowIfNull(memberId);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Edges.DeleteAsync(ForwardKey(memberId, groupId), cancellationToken).ConfigureAwait(false);
            await Edges.DeleteAsync(ReverseKey(groupId, memberId), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(memberId);

        var closure = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal) { memberId };
        var frontier = new Queue<string>();
        frontier.Enqueue(memberId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await WalkForwardClosureAsync(closure, visited, frontier, cancellationToken).ConfigureAwait(false);
        }
        return closure;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(seedGroups);
        if (seedGroups.Count == 0)
        {
            return Array.Empty<string>();
        }

        // Unlike GroupsOfAsync (whose member seed is a user that is not itself a
        // group), the seeds here are groups the subject already belongs to, so
        // they are part of the closure.
        var closure = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var frontier = new Queue<string>();
        foreach (var seed in seedGroups)
        {
            if (visited.Add(seed))
            {
                closure.Add(seed);
                frontier.Enqueue(seed);
            }
        }

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await WalkForwardClosureAsync(closure, visited, frontier, cancellationToken).ConfigureAwait(false);
        }
        return closure;
    }

    /// <summary>
    /// Walks forward membership edges from every id on <paramref name="frontier"/>,
    /// adding each reachable parent group to <paramref name="closure"/> and
    /// enqueueing newly-seen groups. <paramref name="visited"/> provides cycle
    /// detection so an A-in-B, B-in-A cycle terminates.
    /// </summary>
    private async Task WalkForwardClosureAsync(
        HashSet<string> closure,
        HashSet<string> visited,
        Queue<string> frontier,
        CancellationToken cancellationToken)
    {
        while (frontier.Count > 0)
        {
            var current = frontier.Dequeue();
            var prefix = ForwardPrefix(current);
            await foreach (var key in Edges
                .KeysAsync(prefix, PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                var groupId = ThirdField(key);
                if (groupId.Length == 0)
                {
                    continue;
                }

                closure.Add(groupId);
                if (visited.Add(groupId))
                {
                    frontier.Enqueue(groupId);
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupId);

        var members = new List<string>();
        var prefix = ReversePrefix(groupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var key in Edges
                .KeysAsync(prefix, PrefixUpperBound(prefix), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                var memberId = ThirdField(key);
                if (memberId.Length > 0)
                {
                    members.Add(memberId);
                }
            }
        }

        return members;
    }

    private static string ForwardKey(string memberId, string groupId) =>
        string.Create(
            2 + memberId.Length + 1 + groupId.Length,
            (memberId, groupId),
            static (span, state) =>
            {
                var pos = 0;
                span[pos++] = MembershipConstants.ForwardEdge;
                span[pos++] = MembershipConstants.EdgeSeparator;
                state.memberId.AsSpan().CopyTo(span[pos..]);
                pos += state.memberId.Length;
                span[pos++] = MembershipConstants.EdgeSeparator;
                state.groupId.AsSpan().CopyTo(span[pos..]);
            });

    private static string ReverseKey(string groupId, string memberId) =>
        string.Create(
            2 + groupId.Length + 1 + memberId.Length,
            (groupId, memberId),
            static (span, state) =>
            {
                var pos = 0;
                span[pos++] = MembershipConstants.ReverseEdge;
                span[pos++] = MembershipConstants.EdgeSeparator;
                state.groupId.AsSpan().CopyTo(span[pos..]);
                pos += state.groupId.Length;
                span[pos++] = MembershipConstants.EdgeSeparator;
                state.memberId.AsSpan().CopyTo(span[pos..]);
            });

    private static string ForwardPrefix(string memberId) =>
        $"{MembershipConstants.ForwardEdge}{MembershipConstants.EdgeSeparator}{memberId}{MembershipConstants.EdgeSeparator}";

    private static string ReversePrefix(string groupId) =>
        $"{MembershipConstants.ReverseEdge}{MembershipConstants.EdgeSeparator}{groupId}{MembershipConstants.EdgeSeparator}";

    /// <summary>
    /// The exclusive upper bound of every key sharing <paramref name="prefix"/>,
    /// or <see langword="null"/> when the prefix has no finite upper bound
    /// (every code unit is <see cref="char.MaxValue"/>), meaning the edge scan
    /// is open-ended above. Delegates to the shared
    /// <see cref="LatticeKeyRange.PrefixUpperBound(string)"/> so the rollover-safe
    /// algorithm has a single definition.
    /// </summary>
    internal static string? PrefixUpperBound(string prefix) =>
        LatticeKeyRange.PrefixUpperBound(prefix);

    /// <summary>Extracts the third separator-delimited field (the group id in a forward key, the member id in a reverse key).</summary>
    private static string ThirdField(string key)
    {
        var firstSep = key.IndexOf(MembershipConstants.EdgeSeparator);
        if (firstSep < 0)
        {
            return string.Empty;
        }

        var secondSep = key.IndexOf(MembershipConstants.EdgeSeparator, firstSep + 1);
        return secondSep < 0 ? string.Empty : key[(secondSep + 1)..];
    }
}
