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

        /// <summary>
        /// Records <paramref name="subjectOrGroupId"/> in the tenant's member set, which
        /// is what lets it act as the tenant.
        /// </summary>
        public Harness AdmitMember(string subjectOrGroupId)
        {
            Record.AddMemberSubject(subjectOrGroupId, new HybridLogicalClock { WallClockTicks = _memberTick++ }, "seed");
            return this;
        }

        private long _memberTick = 100;

        public LatticeTenantPolicyAdmin Create(
            bool withUsage = true, Microsoft.Extensions.Logging.ILogger<LatticeTenantPolicyAdmin>? logger = null)
        {
            var membership = new CallerContext(this);
            var gate = new AdminSubjectGate(Operator);
            return new LatticeTenantPolicyAdmin(
                new TenantRegionResidencyAuthorizer(gate, Registry, membership),
                Store,
                Directory,
                Decisions,
                gate,
                () => Enabled,
                membership,
                withUsage ? Usage : null,
                logger);
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

        private int _fullScans;

        public int FullScans => Volatile.Read(ref _fullScans);

        public int Writes { get; private set; }

        public List<bool> TenantWriteOrigins { get; } = [];

        public IEnumerable<LatticeAuthorizationRule> All => _rules.Values;

        public void Seed(LatticeAuthorizationRule rule) => _rules[Key(rule.Scope.TreeId, rule.RuleId)] = rule;

        /// <summary>
        /// When set, every tenant-tier write waits on this task before it lands, so a
        /// test can hold several concurrent puts after their checks and before their
        /// writes, then release them together.
        /// </summary>
        public Task? WriteGate { get; set; }

        /// <summary>Completes once <paramref name="count"/> tenant-tier writes are waiting on <see cref="WriteGate"/>.</summary>
        public Task WaitForHeldWritesAsync(int count)
        {
            lock (_held)
            {
                _heldTarget = count;
                _heldReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                if (_heldCount >= count)
                {
                    _heldReached.SetResult();
                }

                return _heldReached.Task;
            }
        }

        public async Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(rule);
            RecordOrigin(rule.RuleId);
            if (WriteGate is { } gate && LatticeTenantRuleIds.IsTenantOwned(rule.RuleId))
            {
                lock (_held)
                {
                    _heldCount++;
                    if (_heldCount >= _heldTarget)
                    {
                        _heldReached?.TrySetResult();
                    }
                }

                await gate.ConfigureAwait(false);
            }

            lock (_rules)
            {
                Writes++;
                Seed(rule);
            }
        }

        private readonly object _held = new();
        private int _heldCount;
        private int _heldTarget = int.MaxValue;
        private TaskCompletionSource? _heldReached;

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rules.TryGetValue(Key(treeId, ruleId), out var rule) ? rule : null);

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
        {
            RecordOrigin(ruleId);
            lock (_rules)
            {
                RemoveAttempts++;
                if (RemoveRuleFailures > 0)
                {
                    RemoveRuleFailures--;
                    throw new InvalidOperationException("transient store failure");
                }

                Writes++;
                return Task.FromResult(_rules.Remove(Key(treeId, ruleId)));
            }
        }

        /// <summary>How many upcoming <see cref="RemoveRuleAsync"/> calls fail (transiently) before one succeeds.</summary>
        public int RemoveRuleFailures { get; set; }

        /// <summary>The <see cref="RemoveRuleAsync"/> calls made, failed or not.</summary>
        public int RemoveAttempts { get; private set; }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
            string treeId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var prefix = treeId + "\u001f";
            KeyValuePair<string, LatticeAuthorizationRule>[] pairs;
            lock (_rules)
            {
                pairs = _rules.ToArray();
            }

            foreach (var pair in pairs)
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
            Interlocked.Increment(ref _fullScans);
            LatticeAuthorizationRule[] rules;
            lock (_rules)
            {
                rules = _rules.Values.ToArray();
            }

            foreach (var rule in rules)
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
                lock (TenantWriteOrigins)
                {
                    TenantWriteOrigins.Add(LatticeAccessGateContext.IsSystemOrigin);
                }
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
