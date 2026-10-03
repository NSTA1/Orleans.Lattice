using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>Lifecycle cases: tenant deletion purges access data (D12), and operator break-glass is audited (F3, D19).</summary>
public sealed partial class TenantAccessConformanceTests
{
    [Test]
    public async Task Case11_tenant_deletion_leaves_no_tenant_group_edge_member_or_tenant_rule()
    {
        const string AdminA = "c11-alice";
        const string AdminB = "c11-bea";
        const string Bob = "c11-bob";
        const string Carl = "c11-carl";
        const string ClusterGroup = "c11-entra";
        var a = await _fixture.SeedTenantAsync("c11-a", AdminA);
        var b = await _fixture.SeedTenantAsync("c11-b", AdminB);
        var prefix = $"t/{a.Value}/";
        var rulePrefix = LatticeTenantRuleIds.Prefix + a.Value + ":";
        await _fixture.SeedClusterGroupAsync(ClusterGroup, "c11-dave");

        using (As(AdminA))
        {
            await _fixture.Directory.UpsertGroupAsync(a.Value, new TenantGroupDescriptor { Name = "eng" });
            await _fixture.Directory.UpsertGroupAsync(a.Value, new TenantGroupDescriptor { Name = "all" });
            await _fixture.Directory.AddGroupMemberAsync(a.Value, "eng", Bob);
            await _fixture.Directory.AddGroupMemberAsync(a.Value, "eng", ClusterGroup, TenantSubjectKind.ClusterGroup);
            await _fixture.Directory.AddGroupMemberAsync(a.Value, "all", "eng", TenantSubjectKind.TenantGroup);
            await _fixture.Directory.AddMemberAsync(a.Value, "all", TenantSubjectKind.TenantGroup);
            await _fixture.Directory.AddMemberAsync(a.Value, Carl);
            await _fixture.AccessAdmin.AddAdminSubjectAsync(a.Value, GroupOf(a, "eng"));
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("eng-read", "eng", "orders", subjectKind: TenantSubjectKind.TenantGroup));
            await _fixture.Policy.PutRuleAsync(a.Value, TenantWideRule("carl-read", Carl));
        }

        using (As(AdminB))
        {
            await _fixture.Directory.UpsertGroupAsync(b.Value, new TenantGroupDescriptor { Name = "eng" });
            await _fixture.Directory.AddGroupMemberAsync(b.Value, "eng", Bob);
            await _fixture.Policy.PutRuleAsync(b.Value, TreeRule("eng-read", "eng", "orders", subjectKind: TenantSubjectKind.TenantGroup));
        }

        // The access data exists before the delete.
        using (LatticeSystemOrigin.Enter())
        {
            var bobGroups = await _fixture.Membership.GroupsOfAsync(Bob);
            var clusterGroupParents = await _fixture.Membership.GroupsOfAsync(ClusterGroup);
            var tenantRules = await TenantRuleIdsAsync(rulePrefix);
            Assert.Multiple(() =>
            {
                Assert.That(bobGroups, Does.Contain(GroupOf(a, "eng")).And.Contain(GroupOf(a, "all")));
                Assert.That(clusterGroupParents, Does.Contain(GroupOf(a, "eng")));
                Assert.That(tenantRules, Has.Count.EqualTo(2));
            });
        }

        using (As(Operator))
        {
            await _fixture.TenantAdmin.DeleteTenantAsync(a.Value);
        }

        using (LatticeSystemOrigin.Enter())
        {
            var eng = await _fixture.Membership.GetGroupAsync(GroupOf(a, "eng"));
            var all = await _fixture.Membership.GetGroupAsync(GroupOf(a, "all"));
            var engMembers = await _fixture.Membership.MembersOfAsync(GroupOf(a, "eng"));
            var allMembers = await _fixture.Membership.MembersOfAsync(GroupOf(a, "all"));
            var bobGroups = await _fixture.Membership.GroupsOfAsync(Bob);
            var clusterGroupParents = await _fixture.Membership.GroupsOfAsync(ClusterGroup);
            var clusterGroup = await _fixture.Membership.GetGroupAsync(ClusterGroup);
            var record = await _fixture.Registry.GetAsync(a);
            var tenantRules = await TenantRuleIdsAsync(rulePrefix);
            var otherTenantRules = await TenantRuleIdsAsync(LatticeTenantRuleIds.Prefix + b.Value + ":");
            var otherGroup = await _fixture.Membership.GetGroupAsync(GroupOf(b, "eng"));

            Assert.Multiple(() =>
            {
                Assert.That(eng, Is.Null, "no tenant group survives");
                Assert.That(all, Is.Null);
                Assert.That(engMembers, Is.Empty, "no edge survives in the forward direction");
                Assert.That(allMembers, Is.Empty);
                Assert.That(bobGroups.Where(g => g.StartsWith(prefix, StringComparison.Ordinal)), Is.Empty, "no edge survives in the reverse direction");
                Assert.That(clusterGroupParents.Where(g => g.StartsWith(prefix, StringComparison.Ordinal)), Is.Empty, "a nested cluster group keeps no edge into the tenant");
                Assert.That(record, Is.Null, "the member set went with the tenant record");
                Assert.That(tenantRules, Is.Empty, "no tenant rule survives");
                Assert.That(clusterGroup, Is.Not.Null, "the cluster group itself is not the tenant's to purge");
                Assert.That(bobGroups, Does.Contain(GroupOf(b, "eng")), "another tenant's groups are untouched");
                Assert.That(otherGroup, Is.Not.Null);
                Assert.That(otherTenantRules, Has.Count.EqualTo(1), "another tenant's rules are untouched");
            });
        }
    }

    [Test]
    public async Task Case12_an_operator_break_glass_removal_of_a_tenant_rule_works_and_is_visible_in_history()
    {
        const string Admin = "c12-alice";
        const string Bob = "c12-bob";
        var a = await _fixture.SeedTenantAsync("c12-a", Admin);
        var orders = TreeOf(a, "orders");
        var fullId = LatticeTenantRuleIds.For(a, "bob-read");

        using (As(Admin))
        {
            await _fixture.Directory.AddMemberAsync(a.Value, Bob);
            await _fixture.Policy.PutRuleAsync(a.Value, TreeRule("bob-read", Bob, "orders"));
        }

        await WaitAllowedAsync(Bob, a, orders, "the tenant rule admits bob");

        // Only the break-glass path removes it: a tenant admin has no cluster facade
        // authority, and the store refuses a tenant-tier id off system origin.
        using (As(Admin))
        {
            Assert.That(
                () => _fixture.AuthAdmin.RemoveRuleAsync(orders, fullId),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>());
        }

        using (As(Operator))
        {
            Assert.That(
                () => _fixture.Store.RemoveRuleAsync(orders, fullId),
                Throws.TypeOf<LatticeTenantOwnedRuleException>(),
                "the store itself is not a break-glass path");

            Assert.That(await _fixture.AuthAdmin.RemoveRuleAsync(orders, fullId), Is.True, "the operator removes the tenant rule through the cluster facade");
        }

        await WaitDeniedAsync(Bob, a, orders, "the removed rule grants nothing");

        TenantRuleView? afterRemoval;
        using (As(Admin))
        {
            afterRemoval = await _fixture.Policy.GetRuleAsync(a.Value, "bob-read");
        }

        Assert.That(afterRemoval, Is.Null, "the tenant admin sees the rule gone");

        IReadOnlyList<EntryRevision> history = [];
        await TestPoll.UntilAsync(
            async () =>
            {
                history = await _fixture.RuleHistoryAsync(orders, fullId);
                return history.Any(r => r.Kind == HistoryRowKind.Delete);
            },
            "the policy tree's per-key history records the break-glass delete",
            Deadline);

        var kinds = history.Select(r => r.Kind).ToList();
        Assert.That(
            kinds.IndexOf(HistoryRowKind.Set),
            Is.GreaterThanOrEqualTo(0).And.LessThan(kinds.LastIndexOf(HistoryRowKind.Delete)),
            "the rule's audit trail shows its write and then its removal");
    }

    private async Task<IReadOnlyList<string>> TenantRuleIdsAsync(string rulePrefix)
    {
        var ids = new List<string>();
        await foreach (var rule in _fixture.Store.ListRulesAsync())
        {
            if (rule.RuleId.StartsWith(rulePrefix, StringComparison.Ordinal))
            {
                ids.Add(rule.RuleId);
            }
        }

        return ids;
    }
}
