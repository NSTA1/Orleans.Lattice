using System.Runtime.CompilerServices;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// Fakes and a facade factory for the <see cref="LatticeTenantPolicyAdmin"/> unit
/// tests: an ordered in-memory policy store that records the origin of every
/// tenant-tier write, a membership directory with fixed group closures, a tenancy
/// policy engine with fixed active-tenant verdicts, a scripted decision source, and
/// fixed membership usage.
/// </summary>
internal static class TenantPolicyTestSupport
{
    public const string Tenant = "acme";
    public const string OtherTenant = "globex";
    public const string Admin = "alice";
    public const string Operator = "root";
    public const string Member = "bob";
    public const string Stranger = "mallory";

    /// <summary>A facade wired to fakes, with the fakes exposed for arrangement and assertions.</summary>
    internal sealed class Harness
    {
        public FakePolicyStore Store { get; } = new();

        public FakeMembershipDirectory Directory { get; } = new();

        public FakeTenantPolicyEngine TenantPolicy { get; } = new();

        public ScriptedDecisionSource Decisions { get; } = new();

        public FixedMembershipUsage Usage { get; } = new();

        public FakeTenantRegistry Registry { get; } = new();

        public bool Enabled { get; set; } = true;

        public LatticeSubject Caller { get; set; } = new(Admin);

        public TenantRecord Record { get; }

        public Harness(TenantQuotas quotas = default)
        {
            Record = SeedTenant(Tenant, quotas, Admin);
            SeedTenant(OtherTenant, default, "olivia");
        }

        public TenantRecord SeedTenant(string tenantId, TenantQuotas quotas, params string[] admins)
        {
            var record = TenantRecord.Create(
                TenantId.Parse(tenantId),
                TenantStatus.Active,
                quotas,
                TenantPlacement.Shared,
                new HybridLogicalClock { WallClockTicks = 1 },
                "seed");
            var tick = 2L;
            foreach (var admin in admins)
            {
                record.AddAdminSubject(admin, new HybridLogicalClock { WallClockTicks = tick++ }, "seed");
            }

            Registry.Seed(record);
            return record;
        }

        public LatticeTenantPolicyAdmin Create(bool withUsage = true)
        {
            var membership = new CallerContext(this);
            var gate = new AdminSubjectGate(Operator);
            return new LatticeTenantPolicyAdmin(
                new TenantRegionResidencyAuthorizer(gate, Registry, membership),
                Store,
                Directory,
                TenantPolicy,
                Decisions,
                gate,
                () => Enabled,
                membership,
                withUsage ? Usage : null);
        }

        private sealed class CallerContext(Harness harness) : ILatticeMembershipContext
        {
            public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
                new(harness.Caller);

            public bool TryResolveCurrent(out LatticeSubject subject)
            {
                subject = harness.Caller;
                return true;
            }
        }
    }

    /// <summary>
    /// An in-memory <see cref="ILatticeAuthorizationPolicyStore"/> keyed and scanned in
    /// (tree id, rule id) order like the real store, recording whether each tenant-tier
    /// write ran under system origin and counting full scans.
    /// </summary>
    internal sealed class FakePolicyStore : ILatticeAuthorizationPolicyStore
    {
        private readonly SortedDictionary<string, LatticeAuthorizationRule> _rules = new(StringComparer.Ordinal);

        public int FullScans { get; private set; }

        public int Writes { get; private set; }

        public List<bool> TenantWriteOrigins { get; } = [];

        public IEnumerable<LatticeAuthorizationRule> All => _rules.Values;

        public void Seed(LatticeAuthorizationRule rule) => _rules[Key(rule.Scope.TreeId, rule.RuleId)] = rule;

        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(rule);
            RecordOrigin(rule.RuleId);
            Writes++;
            Seed(rule);
            return Task.CompletedTask;
        }

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rules.TryGetValue(Key(treeId, ruleId), out var rule) ? rule : null);

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
        {
            RecordOrigin(ruleId);
            Writes++;
            return Task.FromResult(_rules.Remove(Key(treeId, ruleId)));
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
            string treeId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var prefix = treeId + "\u001f";
            foreach (var pair in _rules.ToArray())
            {
                if (pair.Key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    yield return pair.Value;
                }
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            FullScans++;
            foreach (var rule in _rules.Values.ToArray())
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return rule;
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        private void RecordOrigin(string ruleId)
        {
            if (LatticeTenantRuleIds.IsTenantOwned(ruleId))
            {
                TenantWriteOrigins.Add(LatticeAccessGateContext.IsSystemOrigin);
            }
        }

        private static string Key(string treeId, string ruleId) => treeId + "\u001f" + ruleId;
    }

    /// <summary>A membership directory with fixed per-member and per-group closures.</summary>
    internal sealed class FakeMembershipDirectory : ILatticeMembershipDirectory
    {
        private readonly Dictionary<string, string[]> _groupsOf = new(StringComparer.Ordinal);
        private readonly Dictionary<string, string[]> _closures = new(StringComparer.Ordinal);

        public List<string> Expanded { get; } = [];

        public FakeMembershipDirectory WithGroups(string memberId, params string[] groups)
        {
            _groupsOf[memberId] = groups;
            return this;
        }

        public FakeMembershipDirectory WithClosure(string groupId, params string[] ancestors)
        {
            _closures[groupId] = [groupId, .. ancestors];
            return this;
        }

        public Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<string>>(_groupsOf.TryGetValue(memberId, out var groups) ? groups : []);

        public Task<IReadOnlyCollection<string>> ExpandGroupsAsync(
            IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default)
        {
            var result = new List<string>();
            foreach (var seed in seedGroups)
            {
                Expanded.Add(seed);
                result.AddRange(_closures.TryGetValue(seed, out var closure) ? closure : [seed]);
            }

            return Task.FromResult<IReadOnlyCollection<string>>(result);
        }

        public Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<MembershipGroup> ListGroupsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task AddMemberAsync(
            string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    /// <summary>
    /// A tenancy policy engine whose active-tenant verdict admits a subject when it,
    /// or one of its groups, was listed for the tenant. Both overloads are
    /// implemented, so neither falls back to the interface default.
    /// </summary>
    internal sealed class FakeTenantPolicyEngine : ITenantPolicyEngine
    {
        private readonly HashSet<string> _admitted = new(StringComparer.Ordinal);

        public List<(string SubjectId, int GroupCount)> Validations { get; } = [];

        public long CurrentEpoch => 1;

        public FakeTenantPolicyEngine Admit(string tenantId, string subjectOrGroupId)
        {
            _admitted.Add(tenantId + "|" + subjectOrGroupId);
            return this;
        }

        public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId) => [];

        public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId, IReadOnlyCollection<string> groupIds) => [];

        public TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant) =>
            ValidateActiveTenant(subjectId, [], activeTenant);

        public TenantAccessDecision ValidateActiveTenant(
            string subjectId, IReadOnlyCollection<string> groupIds, TenantId activeTenant)
        {
            Validations.Add((subjectId, groupIds.Count));
            if (_admitted.Contains(activeTenant.Value + "|" + subjectId)
                || groupIds.Any(g => _admitted.Contains(activeTenant.Value + "|" + g)))
            {
                return TenantAccessDecision.Allow();
            }

            return TenantAccessDecision.Deny("not a member of the tenant");
        }

        public TenantAccessDecision ResolveCrossTenantGrant(
            TenantId sourceTenant, TenantId targetTenant, string scope, TenantGrantOperations operation) =>
            TenantAccessDecision.Deny("no grants in this fake");
    }

    /// <summary>A decision source returning a scripted verdict and recording each evaluation.</summary>
    internal sealed class ScriptedDecisionSource : ITenantPolicyDecisionSource
    {
        public TenantPolicyVerdict Verdict { get; set; } =
            new(false, false, "No rule matched.", null, null, LatticeEffect.Deny, false, false);

        public LatticeEffect DefaultEffect { get; set; } = LatticeEffect.Deny;

        public List<(LatticeSubject Subject, string TreeId, LatticeOperation Operation, string? Key)> Evaluations { get; } = [];

        public TenantPolicyVerdict Evaluate(LatticeSubject subject, string treeId, LatticeOperation operation, string? key)
        {
            Evaluations.Add((subject, treeId, operation, key));
            return Verdict;
        }
    }

    /// <summary>Fixed tenant group and edge counts.</summary>
    internal sealed class FixedMembershipUsage : ITenantMembershipUsage
    {
        public long? Groups { get; set; } = 3;

        public long? Edges { get; set; } = 7;

        public Task<long?> CountGroupsAsync(TenantId tenant, CancellationToken cancellationToken) => Task.FromResult(Groups);

        public Task<long?> CountEdgesAsync(TenantId tenant, CancellationToken cancellationToken) => Task.FromResult(Edges);
    }

    /// <summary>A tenant-tier rule as the facade would store it.</summary>
    public static LatticeAuthorizationRule TenantRule(
        string tenantId,
        string localId,
        string treeName,
        LatticeSubjectSelector? subject = null,
        LatticeOperation operations = LatticeOperation.Read,
        LatticeEffect effect = LatticeEffect.Allow) =>
        new(
            $"tenant:{tenantId}:{localId}",
            subject ?? LatticeSubjectSelector.User(Member),
            LatticeScope.Tree($"t/{tenantId}/{treeName}"),
            operations,
            effect);

    /// <summary>An operator rule on a tree.</summary>
    public static LatticeAuthorizationRule OperatorRule(
        string ruleId,
        string treeId,
        LatticeSubjectSelector? subject = null,
        LatticeOperation operations = LatticeOperation.Read,
        LatticeEffect effect = LatticeEffect.Allow) =>
        new(ruleId, subject ?? LatticeSubjectSelector.User(Member), LatticeScope.Tree(treeId), operations, effect);

    /// <summary>A tree-scoped draft for <see cref="Member"/>.</summary>
    public static TenantRuleDraft Draft(
        string ruleId = "r1",
        string? treeName = "orders",
        TenantRuleScopeKind scopeKind = TenantRuleScopeKind.Tree,
        string? keyOrPrefix = null,
        string subjectId = Member,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        LatticeOperation operations = LatticeOperation.Read,
        LatticeEffect effect = LatticeEffect.Allow) =>
        new()
        {
            RuleId = ruleId,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            ScopeKind = scopeKind,
            TreeName = treeName,
            KeyOrPrefix = keyOrPrefix,
            Operations = operations,
            Effect = effect,
        };
}
