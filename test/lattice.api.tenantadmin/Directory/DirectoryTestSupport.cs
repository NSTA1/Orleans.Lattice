using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// Deterministic doubles for the tenant directory facade unit tests: an in-memory
/// membership underlay with call counters, an in-memory tenant-tier rule cascade,
/// and a settable delegated-access flag. No cluster, no timing, no ordering
/// assumptions.
/// </summary>
internal static class DirectoryTestSupport
{
    /// <summary>A settable stand-in for the tenancy add-on's live delegated-access flag.</summary>
    internal sealed class SettableFlag
    {
        public SettableFlag(bool enabled) => Enabled = enabled;

        public bool Enabled { get; set; }

        public int Reads { get; private set; }

        public bool Read()
        {
            Reads++;
            return Enabled;
        }
    }

    /// <summary>An in-memory <see cref="ITenantDirectoryStore"/>.</summary>
    internal sealed class FakeTenantDirectoryStore : ITenantDirectoryStore
    {
        private readonly SortedDictionary<string, MembershipGroup> _groups = new(StringComparer.Ordinal);
        private readonly List<(string GroupId, string MemberId, MembershipMemberKind Kind)> _edges = [];

        public int Writes { get; private set; }

        public int Cascades { get; private set; }

        /// <summary>When set, the next <see cref="AddMemberAsync"/> throws it and writes nothing.</summary>
        public Exception? NextAddMemberFailure { get; set; }

        public IReadOnlyList<(string GroupId, string MemberId, MembershipMemberKind Kind)> Edges => _edges;

        public bool HasGroup(string groupId) => _groups.ContainsKey(groupId);

        public void SeedGroup(string groupId, string? displayName = null) => _groups[groupId] = new MembershipGroup(groupId, displayName);

        public void SeedEdge(string groupId, string memberId, MembershipMemberKind kind = MembershipMemberKind.User) =>
            _edges.Add((groupId, memberId, kind));

        public Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken) =>
            Task.FromResult(_groups.TryGetValue(groupId, out var group) ? group : null);

        public Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken)
        {
            Writes++;
            _groups[group.GroupId] = group;
            return Task.CompletedTask;
        }

        public Task<int> CountTenantGroupsAsync(TenantId tenant, CancellationToken cancellationToken)
        {
            var prefix = Prefix(tenant);
            return Task.FromResult(_groups.Keys.Count(k => k.StartsWith(prefix, StringComparison.Ordinal)));
        }

        public Task<int> CountTenantEdgesAsync(TenantId tenant, CancellationToken cancellationToken)
        {
            var prefix = Prefix(tenant);
            return Task.FromResult(_edges.Count(e => e.GroupId.StartsWith(prefix, StringComparison.Ordinal)));
        }

        public Task<DirectoryGroupSlice> ListTenantGroupsAsync(
            TenantId tenant, string? afterGroupId, int pageSize, CancellationToken cancellationToken)
        {
            var prefix = Prefix(tenant);
            var candidates = _groups.Values
                .Where(g => g.GroupId.StartsWith(prefix, StringComparison.Ordinal))
                .Where(g => afterGroupId is null || string.CompareOrdinal(g.GroupId, afterGroupId) > 0)
                .ToList();
            var page = candidates.Take(pageSize).ToList();
            var next = candidates.Count > pageSize ? page[^1].GroupId : null;
            return Task.FromResult(new DirectoryGroupSlice(page, next));
        }

        public Task<int> RemoveGroupCascadeAsync(string groupId, CancellationToken cancellationToken)
        {
            Cascades++;
            var removed = _edges.RemoveAll(e => e.GroupId == groupId || e.MemberId == groupId);
            _groups.Remove(groupId);
            return Task.FromResult(removed);
        }

        public Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind, CancellationToken cancellationToken)
        {
            if (NextAddMemberFailure is { } failure)
            {
                NextAddMemberFailure = null;
                throw failure;
            }

            Writes++;
            if (!_edges.Any(e => e.GroupId == groupId && e.MemberId == memberId))
            {
                _edges.Add((groupId, memberId, memberKind));
            }

            return Task.CompletedTask;
        }

        public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken)
        {
            Writes++;
            _edges.RemoveAll(e => e.GroupId == groupId && e.MemberId == memberId);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyCollection<string>>(_edges.Where(e => e.GroupId == groupId).Select(e => e.MemberId).ToList());

        public Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyCollection<string>>(Closure([memberId], includeSeeds: false));

        public Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyCollection<string>>(Closure(seedGroups, includeSeeds: true));

        private HashSet<string> Closure(IEnumerable<string> seeds, bool includeSeeds)
        {
            var closure = new HashSet<string>(StringComparer.Ordinal);
            var visited = new HashSet<string>(StringComparer.Ordinal);
            var frontier = new Queue<string>();
            foreach (var seed in seeds)
            {
                if (visited.Add(seed))
                {
                    if (includeSeeds)
                    {
                        closure.Add(seed);
                    }

                    frontier.Enqueue(seed);
                }
            }

            while (frontier.Count > 0)
            {
                var current = frontier.Dequeue();
                foreach (var edge in _edges.Where(e => e.MemberId == current))
                {
                    closure.Add(edge.GroupId);
                    if (visited.Add(edge.GroupId))
                    {
                        frontier.Enqueue(edge.GroupId);
                    }
                }
            }

            return closure;
        }

        private static string Prefix(TenantId tenant) => $"t/{tenant.Value}/";
    }

    /// <summary>An in-memory tenant-tier rule cascade holding (rule id, subject group id) pairs.</summary>
    internal sealed class FakeTenantGroupRuleCascade : ITenantGroupRuleCascade
    {
        private readonly List<(string RuleId, string GroupId)> _rules = [];

        public int Calls { get; private set; }

        public IReadOnlyList<(string RuleId, string GroupId)> Rules => _rules;

        public void Seed(string ruleId, string groupId) => _rules.Add((ruleId, groupId));

        public Task<IReadOnlyList<string>> RemoveRulesNamingGroupAsync(
            TenantId tenant, string groupId, CancellationToken cancellationToken)
        {
            Calls++;
            var prefix = $"tenant:{tenant.Value}:";
            var removed = _rules
                .Where(r => r.GroupId == groupId && r.RuleId.StartsWith(prefix, StringComparison.Ordinal))
                .Select(r => r.RuleId)
                .OrderBy(r => r, StringComparer.Ordinal)
                .ToList();
            _rules.RemoveAll(r => removed.Contains(r.RuleId));
            return Task.FromResult<IReadOnlyList<string>>(removed);
        }
    }
}
