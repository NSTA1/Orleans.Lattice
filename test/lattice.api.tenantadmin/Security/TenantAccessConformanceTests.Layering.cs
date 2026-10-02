using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>Layering cases: operator rules are final (D8) and tenant-wide scope is bounded (D9).</summary>
public sealed partial class TenantAccessConformanceTests
{
    [Test]
    public async Task Case01_a_tenant_key_scoped_allow_cannot_override_an_operator_tree_deny()
    {
        const string Admin = "c01-alice";
        const string Bob = "c01-bob";
        var a = await _fixture.SeedTenantAsync("c01-a", Admin);
        var orders = TreeOf(a, "orders");

        using (As(Admin))
        {
            await _fixture.Directory.AddMemberAsync(a.Value, Bob);
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("bob-k1", Bob, "orders") with
            {
                ScopeKind = TenantRuleScopeKind.Key,
                KeyOrPrefix = "k1",
            });
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("bob-prefix", Bob, "orders") with
            {
                ScopeKind = TenantRuleScopeKind.Prefix,
                KeyOrPrefix = "k",
            });
        }

        await WaitAllowedAsync(Bob, a, orders, "the tenant key allow admits the member while no operator rule governs the tree");

        await _fixture.PutOperatorRuleAsync(
            "c01-op-deny", LatticeSubjectSelector.User(Bob), LatticeScope.Tree(orders), LatticeOperation.Read, LatticeEffect.Deny);

        await WaitDeniedAsync(Bob, a, orders, "the operator tree deny is final over the tenant key allow");

        var otherKey = await AllowsAsync(Bob, a, orders, key: "k2");
        LatticeAccessDecision range;
        using (As(Bob))
        {
            range = await _fixture.DecideAsync(a.Value, orders, LatticeOperation.Read, key: null);
        }

        TenantExplanation explanation;
        using (As(Admin))
        {
            explanation = await _fixture.Policy.ExplainAsync(a.Value, Bob, "orders", "k1", LatticeOperation.Read);
        }

        Assert.Multiple(() =>
        {
            Assert.That(otherKey, Is.False, "the tenant prefix allow cannot carve a hole in the operator deny either");
            Assert.That(
                range.Allowed && (range.KeyFilter is null || range.KeyFilter("k1")),
                Is.False,
                "a range read admits no key the operator denies");
            Assert.That(explanation.Allowed, Is.False);
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("c01-op-deny"));
        });
    }

    [Test]
    public async Task Case02_a_tenant_deny_cannot_revoke_an_operator_allow_including_an_operator_Tree_star_allow()
    {
        const string Admin = "c02-alice";
        const string Bob = "c02-bob";
        const string Carol = "c02-carol";
        var a = await _fixture.SeedTenantAsync("c02-a", Admin);
        var orders = TreeOf(a, "orders");
        var invoices = TreeOf(a, "invoices");

        using (As(Admin))
        {
            await _fixture.Directory.AddMemberAsync(a.Value, Bob);
            await _fixture.Directory.AddMemberAsync(a.Value, Carol);
        }

        await _fixture.PutOperatorRuleAsync(
            "c02-op-allow-bob", LatticeSubjectSelector.User(Bob), LatticeScope.Tree(orders), LatticeOperation.Read, LatticeEffect.Allow);
        await _fixture.PutOperatorRuleAsync(
            "c02-op-allow-carol-everywhere", LatticeSubjectSelector.User(Carol), LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow);

        await WaitAllowedAsync(Bob, a, orders, "the operator tree allow admits bob");
        await WaitAllowedAsync(Carol, a, orders, "the operator Tree:* allow admits carol");

        using (As(Admin))
        {
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("bob-deny-k1", Bob, "orders", LatticeEffect.Deny) with
            {
                ScopeKind = TenantRuleScopeKind.Key,
                KeyOrPrefix = "k1",
            });
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("bob-deny-tree", Bob, "orders", LatticeEffect.Deny));
            await _fixture.Policy.PutRuleAsync(a.Value, TenantWideRule("bob-deny-wide", Bob, LatticeEffect.Deny));
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("carol-deny-tree", Carol, "orders", LatticeEffect.Deny));
            await _fixture.Policy.PutRuleAsync(a.Value, TenantWideRule("carol-deny-wide", Carol, LatticeEffect.Deny));

            // Sentinel, written last: once it is in force every deny above is too.
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("sentinel", Bob, "sentinel", operations: LatticeOperation.Write));
        }

        await WaitAllowedAsync(
            Bob, a, TreeOf(a, "sentinel"), "the sentinel written after the tenant denies is in force", LatticeOperation.Write);

        var bobK1 = await AllowsAsync(Bob, a, orders, key: "k1");
        var bobK2 = await AllowsAsync(Bob, a, orders, key: "k2");
        var carolOrders = await AllowsAsync(Carol, a, orders);
        var carolInvoices = await AllowsAsync(Carol, a, invoices);

        TenantExplanation explanation;
        using (As(Admin))
        {
            explanation = await _fixture.Policy.ExplainAsync(a.Value, Carol, "orders", "k1", LatticeOperation.Read);
        }

        Assert.Multiple(() =>
        {
            Assert.That(bobK1, Is.True, "a tenant key deny cannot revoke an operator tree allow");
            Assert.That(bobK2, Is.True, "nor can a tenant tree or tenant-wide deny");
            Assert.That(carolOrders, Is.True, "a tenant deny cannot revoke an operator Tree:* allow");
            Assert.That(carolInvoices, Is.True, "nor can a tenant-wide deny, on any tenant tree");
            Assert.That(explanation.Allowed, Is.True);
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
            Assert.That(explanation.DecidingRule!.Origin, Is.EqualTo(TenantRuleOrigin.PlatformWide));
        });
    }

    [Test]
    public async Task Case03_a_tenant_wide_allow_never_reaches_the_tenants_app_trees_any_sys_tree_or_another_tenants_trees()
    {
        const string AdminA = "c03-alice";
        const string AdminB = "c03-bea";
        const string Bob = "c03-bob";
        var a = await _fixture.SeedTenantAsync("c03-a", AdminA);
        var b = await _fixture.SeedTenantAsync("c03-b", AdminB);
        var bOrders = TreeOf(b, "orders");

        using (As(AdminA))
        {
            await _fixture.Directory.AddMemberAsync(a.Value, Bob);
            await _fixture.Policy.PutRuleAsync(
                a.Value, TenantWideRule("bob-everything", Bob, operations: LatticeOperation.Read | LatticeOperation.Write));
        }

        // An active grant lets bob, acting as A, cross into B's orders tree, so the
        // only thing left to refuse him there is the policy layer.
        using (As(AdminB))
        {
            await _fixture.GrantAdmin.OfferGrantAsync(b.Value, a.Value, bOrders, TenantGrantAccess.Read);
        }

        using (As(AdminA))
        {
            await _fixture.GrantAdmin.ApproveGrantAsync(b.Value, a.Value, bOrders);
        }

        await WaitAllowedAsync(Bob, a, TreeOf(a, "orders"), "the tenant-wide allow reaches the tenant's own tree");
        var ownOther = await AllowsAsync(Bob, a, TreeOf(a, "invoices"), LatticeOperation.Write);

        var appTree = await AllowsAsync(Bob, a, TreeOf(a, "a/shop/items"));
        var tenantSysTree = await AllowsAsync(Bob, a, TreeOf(a, "sys-notes"));
        var platformSysTree = await AllowsAsync(Bob, a, "sys-audit");
        var reservedTree = await AllowsAsync(Bob, a, LatticeAuthReservedTrees.PolicyTreeId);
        var foreignTree = await AllowsAsync(Bob, a, bOrders);

        Assert.Multiple(() =>
        {
            Assert.That(ownOther, Is.True, "and every other tree of the tenant");
            Assert.That(appTree, Is.False, "never the tenant's app-owned trees");
            Assert.That(tenantSysTree, Is.False, "never a sys- tree under the tenant's prefix");
            Assert.That(platformSysTree, Is.False, "never a platform sys- tree");
            Assert.That(reservedTree, Is.False, "never the reserved policy tree");
            Assert.That(foreignTree, Is.False, "never another tenant's tree, even across an active grant");
        });

        // Prove the crossing itself was admitted: once B's own tenant layer allows
        // bob, the same request succeeds, so the refusal above was the policy layer.
        using (As(AdminB))
        {
            await _fixture.Policy.PutRuleAsync(b.Value, TreeRule("grantee-bob-read", Bob, "orders"));
        }

        await WaitAllowedAsync(Bob, a, bOrders, "B's own tenant rule admits bob across the active grant");
    }
}
