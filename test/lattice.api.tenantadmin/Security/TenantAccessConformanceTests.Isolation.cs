using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>Isolation cases: a tenant admin sees nothing of another tenant (D14), and tenant access never bypasses the tenant gate (D15).</summary>
public sealed partial class TenantAccessConformanceTests
{
    [Test]
    public async Task Case07_a_tenant_admin_of_A_cannot_list_read_write_or_discover_anything_of_B()
    {
        const string AdminA = "c07-alice";
        const string AdminB = "c07-bea";
        const string BobOfB = "c07-bob";
        const string Ghost = "c07-ghost";
        var a = await _fixture.SeedTenantAsync("c07-a", AdminA);
        var b = await _fixture.SeedTenantAsync("c07-b", AdminB);

        using (As(AdminB))
        {
            await _fixture.Directory.UpsertGroupAsync(b.Value, new TenantGroupDescriptor { Name = "staff" });
            await _fixture.Directory.AddGroupMemberAsync(b.Value, "staff", BobOfB);
            await _fixture.Directory.AddMemberAsync(b.Value, "staff", TenantSubjectKind.TenantGroup);
            await _fixture.Policy.PutRuleAsync(b.Value, TreeRule("staff-read", "staff", "orders", subjectKind: TenantSubjectKind.TenantGroup));
            await _fixture.Policy.PutRuleAsync(b.Value, TreeRule("bob-read", BobOfB, "invoices"));
        }

        var page = new TenantAccessPageRequest();
        var operations = new (string Name, Func<string, Task> Call)[]
        {
            ("list groups", t => _fixture.Directory.ListGroupsAsync(t, page)),
            ("read a group", t => _fixture.Directory.GetGroupAsync(t, "staff")),
            ("write a group", t => _fixture.Directory.UpsertGroupAsync(t, new TenantGroupDescriptor { Name = "staff", DisplayName = "pwned" })),
            ("remove a group", t => _fixture.Directory.RemoveGroupAsync(t, "staff")),
            ("list group members", t => _fixture.Directory.ListGroupMembersAsync(t, "staff")),
            ("add a group member", t => _fixture.Directory.AddGroupMemberAsync(t, "staff", AdminA)),
            ("remove a group member", t => _fixture.Directory.RemoveGroupMemberAsync(t, "staff", BobOfB)),
            ("list members", t => _fixture.Directory.ListMembersAsync(t, page)),
            ("add a member", t => _fixture.Directory.AddMemberAsync(t, AdminA)),
            ("remove a member", t => _fixture.Directory.RemoveMemberAsync(t, "staff", TenantSubjectKind.TenantGroup)),
            ("resolve a subject", t => _fixture.Directory.ResolveSubjectAsync(t, BobOfB)),
            ("list rules", t => _fixture.Policy.ListRulesAsync(t, page)),
            ("read a rule", t => _fixture.Policy.GetRuleAsync(t, "staff-read")),
            ("write a rule", t => _fixture.Policy.PutRuleAsync(t, TreeRule("alice-read", AdminA, "orders"))),
            ("remove a rule", t => _fixture.Policy.RemoveRuleAsync(t, "staff-read")),
            ("explain", t => _fixture.Policy.ExplainAsync(t, BobOfB, "orders", "k1", LatticeOperation.Read)),
            ("effective permissions", t => _fixture.Policy.EffectivePermissionsAsync(t, BobOfB)),
            ("posture", t => _fixture.Policy.GetPostureAsync(t)),
            ("list admin subjects", t => _fixture.AccessAdmin.ListAdminSubjectsAsync(t)),
            ("add an admin subject", t => _fixture.AccessAdmin.AddAdminSubjectAsync(t, AdminA)),
        };

        using (As(AdminA))
        {
            var results = new List<(string Name, Exception? OnB, Exception? OnGhost)>();
            foreach (var (name, call) in operations)
            {
                results.Add((name, await CaptureAsync(() => call(b.Value)), await CaptureAsync(() => call(Ghost))));
            }

            Assert.Multiple(() =>
            {
                foreach (var (name, onB, onGhost) in results)
                {
                    Assert.That(onB, Is.TypeOf<LatticeAuthorizationDeniedException>(), $"{name}: denied on B");
                    Assert.That(onGhost, Is.TypeOf<LatticeAuthorizationDeniedException>(), $"{name}: denied on a tenant that does not exist");
                    Assert.That(
                        onB?.Message.Replace(b.Value, "{tenant}", StringComparison.Ordinal),
                        Is.EqualTo(onGhost?.Message.Replace(Ghost, "{tenant}", StringComparison.Ordinal)),
                        $"{name}: B's denial is indistinguishable from a missing tenant's, so B's existence does not leak");
                }
            });

            // A's own surfaces reveal nothing of B either: B's group reads as not
            // found under A's name, and B's rules and members never appear.
            var groupByName = await _fixture.Directory.GetGroupAsync(a.Value, "staff");
            var groups = await _fixture.Directory.ListGroupsAsync(a.Value, page);
            var rules = await _fixture.Policy.ListRulesAsync(a.Value, page);
            var members = await _fixture.Directory.ListMembersAsync(a.Value, page);
            var permissions = await _fixture.Policy.EffectivePermissionsAsync(a.Value, BobOfB);
            Assert.Multiple(() =>
            {
                Assert.That(groupByName, Is.Null, "B's group reads as not found");
                Assert.That(groups.Entries, Is.Empty);
                Assert.That(rules.Entries, Is.Empty);
                Assert.That(members.Entries, Is.Empty);
                Assert.That(permissions.Rules, Is.Empty, "B's rules naming B's member are not disclosed through A");
            });
        }

        using (As(Operator))
        {
            Assert.That(
                () => _fixture.Directory.ListGroupsAsync(TenantId.DefaultId, page),
                Throws.TypeOf<ReservedTenantOperationException>(),
                "the reserved default tenant has no tenant tier, even for an operator");
        }

        // Nothing of B changed.
        using (As(AdminB))
        {
            var staff = await _fixture.Directory.GetGroupAsync(b.Value, "staff");
            var staffMembers = await _fixture.Directory.ListGroupMembersAsync(b.Value, "staff");
            var bRules = await _fixture.Policy.ListRulesAsync(b.Value, page);
            var bMembers = await _fixture.Directory.ListMembersAsync(b.Value, page);
            Assert.Multiple(() =>
            {
                Assert.That(staff!.DisplayName, Is.Not.EqualTo("pwned"));
                Assert.That(staffMembers.Select(m => m.MemberId), Is.EqualTo(new[] { BobOfB }));
                Assert.That(bRules.Entries.Select(r => r.RuleId), Is.EquivalentTo(new[] { "staff-read", "bob-read" }));
                Assert.That(bMembers.Entries.Select(m => m.SubjectId), Is.EqualTo(new[] { "staff" }));
            });
        }
    }

    [Test]
    public async Task Case08_a_member_of_A_cannot_reach_Bs_tree_without_an_active_cross_tenant_grant_and_with_one_Bs_tenant_layer_decides()
    {
        const string AdminA = "c08-alice";
        const string AdminB = "c08-bea";
        const string Bob = "c08-bob";
        var a = await _fixture.SeedTenantAsync("c08-a", AdminA);
        var b = await _fixture.SeedTenantAsync("c08-b", AdminB);
        var bOrders = TreeOf(b, "orders");

        using (As(AdminB))
        {
            await _fixture.Policy.PutRuleAsync(b.Value, TreeRule("bob-read", Bob, "orders"));
        }

        using (As(AdminA))
        {
            await _fixture.Directory.AddMemberAsync(a.Value, Bob);
            await _fixture.Policy.PutRuleAsync(a.Value, TenantWideRule("bob-everything", Bob));
        }

        // A's rule was written after B's, so once it is in force B's is too.
        await WaitAllowedAsync(Bob, a, TreeOf(a, "orders"), "bob acts as A on A's own tree");

        var noGrant = await AllowsAsync(Bob, a, bOrders);
        var asB = await AllowsAsync(Bob, b, bOrders);

        using (As(AdminB))
        {
            await _fixture.GrantAdmin.OfferGrantAsync(b.Value, a.Value, bOrders, TenantGrantAccess.Read);
        }

        var pending = await AllowsAsync(Bob, a, bOrders);
        Assert.Multiple(() =>
        {
            Assert.That(noGrant, Is.False, "without a grant, neither B's rule nor A's tenant-wide allow lets A's member in");
            Assert.That(asB, Is.False, "bob cannot assert B, of which he is not a member");
            Assert.That(pending, Is.False, "an offered but unapproved grant admits nothing");
        });

        using (As(AdminA))
        {
            await _fixture.GrantAdmin.ApproveGrantAsync(b.Value, a.Value, bOrders);
        }

        await WaitAllowedAsync(Bob, a, bOrders, "with the grant active, B's tenant rule admits A's member");

        // B's tenant layer decides, at key granularity; A's tenant-wide allow, which
        // would admit every key, has no say on B's tree.
        using (As(AdminB))
        {
            await _fixture.Policy.PutRuleAsync(b.Value, TreeRule("bob-not-k2", Bob, "orders", LatticeEffect.Deny) with
            {
                ScopeKind = TenantRuleScopeKind.Key,
                KeyOrPrefix = "k2",
            });
        }

        await WaitDeniedAsync(Bob, a, bOrders, "B's key deny refuses k2", key: "k2");
        var k1 = await AllowsAsync(Bob, a, bOrders, key: "k1");
        var write = await AllowsAsync(Bob, a, bOrders, LatticeOperation.Write);
        Assert.Multiple(() =>
        {
            Assert.That(k1, Is.True, "B's tree allow still admits k1");
            Assert.That(write, Is.False, "the grant covers reads only");
        });

        // Revoking the grant ends the crossing on the very next request.
        using (As(AdminA))
        {
            await _fixture.GrantAdmin.RevokeGrantAsync(b.Value, a.Value, bOrders);
        }

        Assert.That(await AllowsAsync(Bob, a, bOrders, key: "k1"), Is.False, "a revoked grant admits nothing on the next request");
    }
}
