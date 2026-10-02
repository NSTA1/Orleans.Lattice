using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the tenant-tier rule purge
/// (<see cref="ITenantPolicyRuleStore.PurgeTenantRulesAsync"/>, implemented by
/// <see cref="TenantRulePurge"/>) the tenant deletion pipeline calls: it removes
/// exactly the deleted tenant's <c>tenant:{T}:</c> rules wherever they are scoped,
/// runs its deletes under system origin, is idempotent, and refuses the default and
/// uninitialised tenants.
/// </summary>
[TestFixture]
public sealed class TenantRulePurgeTests
{
    private static readonly TenantId Contoso = TenantId.Parse("contoso");
    private static readonly TenantId Fabrikam = TenantId.Parse("fabrikam");

    private sealed class RecordingStore : ILatticeAuthorizationPolicyStore
    {
        public List<LatticeAuthorizationRule> Rules { get; } = new();

        public List<bool> RemoveOrigins { get; } = new();

        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
        {
            Rules.Add(rule);
            return Task.CompletedTask;
        }

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(Rules.FirstOrDefault(r => r.Scope.TreeId == treeId && r.RuleId == ruleId));

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
        {
            RemoveOrigins.Add(LatticeSystemOrigin.IsActive);
            return Task.FromResult(Rules.RemoveAll(r => r.Scope.TreeId == treeId && r.RuleId == ruleId) > 0);
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
            string treeId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in Rules.Where(r => r.Scope.TreeId == treeId).ToArray())
            {
                yield return rule;
            }

            await Task.CompletedTask;
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var rule in Rules.ToArray())
            {
                yield return rule;
            }

            await Task.CompletedTask;
        }
    }

    private static LatticeAuthorizationRule Rule(string id, string treeId) =>
        new(id, LatticeSubjectSelector.User("alice"), LatticeScope.Tree(treeId), LatticeOperation.Read, LatticeEffect.Allow);

    private static RecordingStore Seeded() => new()
    {
        Rules =
        {
            Rule(LatticeTenantRuleIds.For(Contoso, "a"), "t/contoso/orders"),
            Rule(LatticeTenantRuleIds.For(Contoso, "b"), "t/contoso/*"),
            Rule("tenant:contoso:", "t/contoso/legacy"),
            Rule(LatticeTenantRuleIds.For(Fabrikam, "a"), "t/fabrikam/orders"),
            Rule("tenant:contosoplus:a", "t/contosoplus/orders"),
            Rule("ops", "t/contoso/orders"),
            Rule(LatticeAppRuleIds.Prefix + "x", "t/contoso/a/x/y"),
        },
    };

    [Test]
    public async Task PurgeAsync_removes_exactly_the_tenants_rules_wherever_scoped()
    {
        var store = Seeded();

        var removed = await TenantRulePurge.PurgeAsync(store, Contoso, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.EqualTo(3));
            Assert.That(
                store.Rules.Select(r => r.RuleId),
                Is.EquivalentTo(new[] { LatticeTenantRuleIds.For(Fabrikam, "a"), "tenant:contosoplus:a", "ops", LatticeAppRuleIds.Prefix + "x" }));
        });
    }

    [Test]
    public async Task PurgeAsync_deletes_under_system_origin_and_leaves_none_behind()
    {
        var store = Seeded();

        await TenantRulePurge.PurgeAsync(store, Contoso, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.RemoveOrigins, Is.Not.Empty.And.All.True);
            Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the purge's system-origin scope must not leak");
        });
    }

    [Test]
    public async Task PurgeAsync_is_idempotent()
    {
        var store = Seeded();
        await TenantRulePurge.PurgeAsync(store, Contoso, CancellationToken.None);

        var second = await TenantRulePurge.PurgeAsync(store, Contoso, CancellationToken.None);

        Assert.That(second, Is.Zero);
    }

    [Test]
    public void PurgeAsync_refuses_the_default_and_uninitialised_tenants()
    {
        var store = Seeded();

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentException>(() => TenantRulePurge.PurgeAsync(store, TenantId.Default, CancellationToken.None));
            Assert.ThrowsAsync<ArgumentException>(() => TenantRulePurge.PurgeAsync(store, default, CancellationToken.None));
            Assert.That(store.RemoveOrigins, Is.Empty);
        });
    }

    [Test]
    public void PurgeAsync_null_store_throws()
    {
        Assert.ThrowsAsync<ArgumentNullException>(() => TenantRulePurge.PurgeAsync(null!, Contoso, CancellationToken.None));
    }

    [Test]
    public void PurgeAsync_observes_cancellation_before_deleting()
    {
        var store = Seeded();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => TenantRulePurge.PurgeAsync(store, Contoso, cts.Token));
        Assert.That(store.RemoveOrigins, Is.Empty);
    }
}
