using System.Runtime.CompilerServices;
using Orleans.Lattice;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// Pins <see cref="PolicyStoreTenantGroupRuleCascade"/>: a removed tenant group takes
/// with it exactly the tenant's own tier rules whose subject is that group, deleted
/// under system origin (the only origin the policy store admits for a tenant-tier rule
/// id), and nothing else - not an operator rule naming the group, not another
/// tenant's rule, not a user rule.
/// </summary>
[TestFixture]
public sealed class PolicyStoreTenantGroupRuleCascadeTests
{
    private const string Group = "t/acme/eng";

    private static LatticeAuthorizationRule Rule(string ruleId, LatticeSubjectSelector subject, string treeId = "t/acme/orders") =>
        new(ruleId, subject, LatticeScope.Tree(treeId), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public async Task Removes_only_the_tenants_own_rules_naming_the_group_under_system_origin()
    {
        var store = new InMemoryPolicyStore(
            Rule("tenant:acme:read-eng-b", LatticeSubjectSelector.Group(Group), "t/acme/b"),
            Rule("tenant:acme:read-eng-a", LatticeSubjectSelector.Group(Group)),
            Rule("tenant:acme:user", LatticeSubjectSelector.User(Group)),
            Rule("tenant:acme:other", LatticeSubjectSelector.Group("t/acme/other")),
            Rule("operator-rule", LatticeSubjectSelector.Group(Group)),
            Rule("tenant:globex:x", LatticeSubjectSelector.Group(Group)));
        var cascade = new PolicyStoreTenantGroupRuleCascade(store);

        var removed = await cascade.RemoveRulesNamingGroupAsync(TenantId.Parse("acme"), Group, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.EqualTo(new[] { "tenant:acme:read-eng-a", "tenant:acme:read-eng-b" }));
            Assert.That(store.RemovedUnderSystemOrigin, Is.EqualTo(new[] { true, true }));
            Assert.That(
                store.Remaining.Select(r => r.RuleId),
                Is.EquivalentTo(new[] { "tenant:acme:user", "tenant:acme:other", "operator-rule", "tenant:globex:x" }));
        });
    }

    [Test]
    public async Task Is_idempotent()
    {
        var store = new InMemoryPolicyStore(Rule("tenant:acme:r", LatticeSubjectSelector.Group(Group)));
        var cascade = new PolicyStoreTenantGroupRuleCascade(store);
        await cascade.RemoveRulesNamingGroupAsync(TenantId.Parse("acme"), Group, CancellationToken.None);

        Assert.That(await cascade.RemoveRulesNamingGroupAsync(TenantId.Parse("acme"), Group, CancellationToken.None), Is.Empty);
    }

    [Test]
    public void Refuses_the_reserved_default_tenant()
    {
        var cascade = new PolicyStoreTenantGroupRuleCascade(new InMemoryPolicyStore());

        Assert.That(
            async () => await cascade.RemoveRulesNamingGroupAsync(TenantId.Default, Group, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void Rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new PolicyStoreTenantGroupRuleCascade(null!), Throws.ArgumentNullException);
            Assert.That(
                async () => await new PolicyStoreTenantGroupRuleCascade(new InMemoryPolicyStore())
                    .RemoveRulesNamingGroupAsync(TenantId.Parse("acme"), null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>An in-memory policy store recording the origin of each removal.</summary>
    private sealed class InMemoryPolicyStore(params LatticeAuthorizationRule[] rules) : ILatticeAuthorizationPolicyStore
    {
        private readonly List<LatticeAuthorizationRule> _rules = [.. rules];

        public List<bool> RemovedUnderSystemOrigin { get; } = [];

        public IReadOnlyList<LatticeAuthorizationRule> Remaining => _rules;

        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rules.FirstOrDefault(r => r.Scope.TreeId == treeId && r.RuleId == ruleId));

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
        {
            RemovedUnderSystemOrigin.Add(LatticeSystemOrigin.IsActive);
            return Task.FromResult(_rules.RemoveAll(r => r.Scope.TreeId == treeId && r.RuleId == ruleId) > 0);
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
            string treeId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in _rules.Where(r => r.Scope.TreeId == treeId).ToList())
            {
                yield return rule;
            }

            await Task.CompletedTask;
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in _rules.ToList())
            {
                yield return rule;
            }

            await Task.CompletedTask;
        }
    }
}
