using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>
/// Flag-off cases: with delegated tenant access administration off, the facades
/// refuse, and the epic's data - tenant rules, member entries, tenant groups - is
/// inert, so decisions match a baseline taken before that data existed (D10, D11).
/// </summary>
public sealed partial class TenantAccessConformanceTests
{
    [Test]
    public async Task Case10a_with_the_flag_off_every_facade_member_refuses_except_posture()
    {
        const string Admin = "c10a-pat";
        var t = await _fixture.SeedTenantAsync("c10a-t", Admin);
        using (As(Admin))
        {
            await _fixture.Directory.UpsertGroupAsync(t.Value, new TenantGroupDescriptor { Name = "eng" });
            await _fixture.Policy.PutRuleAsync(t.Value, TreeRule("pat-read", Admin, "orders"));
        }

        var page = new TenantAccessPageRequest();
        var tenant = t.Value;
        var operations = new (string Name, Func<Task> Call)[]
        {
            ("ListGroups", () => _fixture.Directory.ListGroupsAsync(tenant, page)),
            ("GetGroup", () => _fixture.Directory.GetGroupAsync(tenant, "eng")),
            ("UpsertGroup", () => _fixture.Directory.UpsertGroupAsync(tenant, new TenantGroupDescriptor { Name = "ops" })),
            ("RemoveGroup", () => _fixture.Directory.RemoveGroupAsync(tenant, "eng")),
            ("ListGroupMembers", () => _fixture.Directory.ListGroupMembersAsync(tenant, "eng")),
            ("AddGroupMember", () => _fixture.Directory.AddGroupMemberAsync(tenant, "eng", "c10a-bob")),
            ("RemoveGroupMember", () => _fixture.Directory.RemoveGroupMemberAsync(tenant, "eng", "c10a-bob")),
            ("ListMembers", () => _fixture.Directory.ListMembersAsync(tenant, page)),
            ("AddMember", () => _fixture.Directory.AddMemberAsync(tenant, "c10a-bob")),
            ("RemoveMember", () => _fixture.Directory.RemoveMemberAsync(tenant, "c10a-bob")),
            ("ResolveSubject", () => _fixture.Directory.ResolveSubjectAsync(tenant, "c10a-bob")),
            ("PutRule", () => _fixture.Policy.PutRuleAsync(tenant, TreeRule("bob-read", "c10a-bob", "orders"))),
            ("GetRule", () => _fixture.Policy.GetRuleAsync(tenant, "pat-read")),
            ("RemoveRule", () => _fixture.Policy.RemoveRuleAsync(tenant, "pat-read")),
            ("ListRules", () => _fixture.Policy.ListRulesAsync(tenant, page)),
            ("Explain", () => _fixture.Policy.ExplainAsync(tenant, Admin, "orders", "k1", LatticeOperation.Read)),
            ("EffectivePermissions", () => _fixture.Policy.EffectivePermissionsAsync(tenant, Admin)),
        };

        TenantAccessConformanceClusterFixture.Switch.Set(false);
        try
        {
            foreach (var caller in new[] { Admin, Operator })
            {
                var results = new List<(string Name, Exception? Thrown)>();
                TenantAccessPosture posture;
                using (As(caller))
                {
                    foreach (var (name, call) in operations)
                    {
                        results.Add((name, await CaptureAsync(call)));
                    }

                    posture = await _fixture.Policy.GetPostureAsync(tenant);
                }

                Assert.Multiple(() =>
                {
                    foreach (var (name, thrown) in results)
                    {
                        Assert.That(thrown, Is.TypeOf<TenantAccessAdministrationDisabledException>(), $"{caller}: {name}");
                    }

                    Assert.That(posture.Enabled, Is.False, $"{caller}: the posture answers, and reports the flag off");
                    Assert.That(posture.CallerIsTenantAdmin || posture.CallerIsPlatformOperator, Is.True, caller);
                });
            }

            using (As("c10a-mallory"))
            {
                Assert.Multiple(() =>
                {
                    Assert.That(
                        () => _fixture.Directory.ListGroupsAsync(tenant, page),
                        Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                        "an unauthorized caller is denied, so the flag is not disclosed to it");
                    Assert.That(() => _fixture.Policy.GetPostureAsync(tenant), Throws.TypeOf<LatticeAuthorizationDeniedException>());
                });
            }
        }
        finally
        {
            TenantAccessConformanceClusterFixture.Switch.Set(true);
        }

        using (As(Admin))
        {
            var group = await _fixture.Directory.GetGroupAsync(tenant, "eng");
            var rule = await _fixture.Policy.GetRuleAsync(tenant, "pat-read");
            Assert.Multiple(() =>
            {
                Assert.That(group, Is.Not.Null, "nothing was removed while the flag was off");
                Assert.That(rule, Is.Not.Null);
            });
        }
    }

    [Test]
    public async Task Case10b_with_the_flag_off_a_pre_existing_tenant_rule_is_inert()
    {
        const string Admin = "c10b-pat";
        var t = await _fixture.SeedTenantAsync("c10b-t", Admin);
        var orders = TreeOf(t, "orders");

        using (As(Admin))
        {
            await _fixture.Policy.PutRuleAsync(t.Value, TreeRule("pat-read", Admin, "orders"));
            await _fixture.Policy.PutRuleAsync(t.Value, TenantWideRule("pat-write", Admin, operations: LatticeOperation.Write));
        }

        await WaitAllowedAsync(Admin, t, orders, "the tenant tree rule admits the admin while the flag is on");
        await WaitAllowedAsync(Admin, t, TreeOf(t, "invoices"), "the tenant-wide rule admits the admin while the flag is on", LatticeOperation.Write);

        bool read;
        bool write;
        TenantAccessConformanceClusterFixture.Switch.Set(false);
        try
        {
            read = await AllowsAsync(Admin, t, orders);
            write = await AllowsAsync(Admin, t, TreeOf(t, "invoices"), LatticeOperation.Write);
        }
        finally
        {
            TenantAccessConformanceClusterFixture.Switch.Set(true);
        }

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.False, "a tenant tree rule grants nothing once the flag is off, on the next decision");
            Assert.That(write, Is.False, "nor does a tenant-wide rule");
        });

        await WaitAllowedAsync(Admin, t, orders, "turning the flag back on restores the rule, so the flag was the only cause");
    }

    [Test]
    public async Task Case10c_with_the_flag_off_a_member_entry_admits_nobody()
    {
        const string Admin = "c10c-pat";
        const string Mia = "c10c-mia";
        const string Dan = "c10c-dan";
        const string Erin = "c10c-erin";
        const string Adam = "c10c-adam";
        const string ClusterGroup = "c10c-entra";
        var t = await _fixture.SeedTenantAsync("c10c-t", Admin);
        var orders = TreeOf(t, "orders");
        await _fixture.SeedClusterGroupAsync(ClusterGroup, Erin);

        using (As(Admin))
        {
            await _fixture.Directory.UpsertGroupAsync(t.Value, new TenantGroupDescriptor { Name = "eng" });
            await _fixture.Directory.AddGroupMemberAsync(t.Value, "eng", Dan);
            await _fixture.Directory.UpsertGroupAsync(t.Value, new TenantGroupDescriptor { Name = "admins" });
            await _fixture.Directory.AddGroupMemberAsync(t.Value, "admins", Adam);
            await _fixture.Directory.AddMemberAsync(t.Value, Mia);
            await _fixture.Directory.AddMemberAsync(t.Value, "eng", TenantSubjectKind.TenantGroup);
            await _fixture.Directory.AddMemberAsync(t.Value, ClusterGroup, TenantSubjectKind.ClusterGroup);
            await _fixture.AccessAdmin.AddAdminSubjectAsync(t.Value, GroupOf(t, "admins"));
        }

        // Operator rules grant every one of them the read, so only the tenant gate's
        // decision on who may act as the tenant is under test.
        foreach (var subject in new[] { Mia, Dan, Erin, Adam })
        {
            await _fixture.PutOperatorRuleAsync(
                $"c10c-op-{subject}", LatticeSubjectSelector.User(subject), LatticeScope.Tree(orders), LatticeOperation.Read, LatticeEffect.Allow);
        }

        await WaitAllowedAsync(Mia, t, orders, "a direct member entry admits mia while the flag is on");
        await WaitAllowedAsync(Dan, t, orders, "a tenant group member entry admits dan while the flag is on");
        await WaitAllowedAsync(Erin, t, orders, "a cluster group member entry admits erin while the flag is on");
        await WaitAllowedAsync(Adam, t, orders, "an admin-set group entry admits adam while the flag is on");

        var after = new Dictionary<string, bool>();
        TenantAccessConformanceClusterFixture.Switch.Set(false);
        try
        {
            foreach (var subject in new[] { Mia, Dan, Erin, Adam })
            {
                after[subject] = await AllowsAsync(subject, t, orders);
            }
        }
        finally
        {
            TenantAccessConformanceClusterFixture.Switch.Set(true);
        }

        Assert.Multiple(() =>
        {
            Assert.That(after[Mia], Is.False, "a direct member entry admits nobody once the flag is off");
            Assert.That(after[Dan], Is.False, "nor does a tenant group entry");
            Assert.That(after[Erin], Is.False, "nor does a cluster group entry");
            Assert.That(after[Adam], Is.False, "nor does an admin-set group entry");
        });
    }

    [Test]
    public async Task Case10d_with_the_flag_off_decisions_match_a_no_epic_baseline_over_a_generated_request_set()
    {
        const string PatP = "c10d-pat";
        const string QuinnQ = "c10d-quinn";
        const string U1 = "c10d-u1";
        const string U2 = "c10d-u2";
        const string U3 = "c10d-u3";
        const string Outsider = "c10d-outsider";
        const string ClusterGroup = "c10d-entra";
        var p = await _fixture.SeedTenantAsync("c10d-p", PatP);
        var q = await _fixture.SeedTenantAsync("c10d-q", QuinnQ);
        await _fixture.SeedClusterGroupAsync(ClusterGroup, U2);
        const string Plain = "c10d-plain";

        await _fixture.PutOperatorRuleAsync("c10d-1", LatticeSubjectSelector.User(U1), LatticeScope.Tree(TreeOf(p, "orders")), LatticeOperation.Read, LatticeEffect.Allow);
        await _fixture.PutOperatorRuleAsync("c10d-2", LatticeSubjectSelector.User(U2), LatticeScope.Tree(TreeOf(p, "orders")), LatticeOperation.Write, LatticeEffect.Deny);
        await _fixture.PutOperatorRuleAsync("c10d-3", LatticeSubjectSelector.Group(ClusterGroup), LatticeScope.Prefix(TreeOf(p, "invoices"), "k"), LatticeOperation.Read | LatticeOperation.Write, LatticeEffect.Allow);
        await _fixture.PutOperatorRuleAsync("c10d-4", LatticeSubjectSelector.User(QuinnQ), LatticeScope.Key(TreeOf(q, "orders"), "k1"), LatticeOperation.Read, LatticeEffect.Allow);
        await _fixture.PutOperatorRuleAsync("c10d-5", LatticeSubjectSelector.User(U3), LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow);
        await _fixture.PutOperatorRuleAsync("c10d-6", LatticeSubjectSelector.User(Outsider), LatticeScope.Tree(Plain), LatticeOperation.Read, LatticeEffect.Allow);
        await _fixture.PutOperatorRuleAsync("c10d-7", LatticeSubjectSelector.User(PatP), LatticeScope.Tree(TreeOf(p, "orders")), LatticeOperation.Read | LatticeOperation.Write, LatticeEffect.Allow);

        // The baseline: the flag off and none of the epic's data in either tenant.
        TenantAccessConformanceClusterFixture.Switch.Set(false);
        IReadOnlyList<string> baseline;
        try
        {
            await WaitAllowedAsync(PatP, p, TreeOf(p, "orders"), "the operator rules, the last written first, are in force", LatticeOperation.Write);
            baseline = await DecideRequestSetAsync();
        }
        finally
        {
            TenantAccessConformanceClusterFixture.Switch.Set(true);
        }

        // The epic's data, written through the facades while the flag is on.
        using (As(PatP))
        {
            await _fixture.Directory.UpsertGroupAsync(p.Value, new TenantGroupDescriptor { Name = "eng" });
            await _fixture.Directory.AddGroupMemberAsync(p.Value, "eng", U1);
            await _fixture.Directory.AddGroupMemberAsync(p.Value, "eng", ClusterGroup, TenantSubjectKind.ClusterGroup);
            await _fixture.Directory.UpsertGroupAsync(p.Value, new TenantGroupDescriptor { Name = "admins" });
            await _fixture.Directory.AddGroupMemberAsync(p.Value, "admins", U2);
            await _fixture.AccessAdmin.AddAdminSubjectAsync(p.Value, GroupOf(p, "admins"));
            await _fixture.Directory.AddMemberAsync(p.Value, "eng", TenantSubjectKind.TenantGroup);
            await _fixture.Directory.AddMemberAsync(p.Value, U3);
            await _fixture.Directory.AddMemberAsync(p.Value, ClusterGroup, TenantSubjectKind.ClusterGroup);
            await _fixture.Policy.PutRuleAsync(p.Value, TenantWideRule("eng-all", "eng", operations: LatticeOperation.Read | LatticeOperation.Write, subjectKind: TenantSubjectKind.TenantGroup));
            await _fixture.Policy.PutRuleAsync(p.Value, TreeRule("u3-no-orders", U3, "orders", LatticeEffect.Deny));
            await _fixture.Policy.PutRuleAsync(p.Value, TreeRule("u2-k1", U2, "orders", operations: LatticeOperation.Write) with
            {
                ScopeKind = TenantRuleScopeKind.Key,
                KeyOrPrefix = "k1",
            });
            await _fixture.Policy.PutRuleAsync(p.Value, TenantWideRule("u1-no-write", U1, LatticeEffect.Deny, LatticeOperation.Write));
            await _fixture.Policy.PutRuleAsync(p.Value, TreeRule("u1-x-invoices", U1, "invoices") with
            {
                ScopeKind = TenantRuleScopeKind.Prefix,
                KeyOrPrefix = "x",
            });
        }

        using (As(QuinnQ))
        {
            await _fixture.Directory.UpsertGroupAsync(q.Value, new TenantGroupDescriptor { Name = "ops" });
            await _fixture.Directory.AddGroupMemberAsync(q.Value, "ops", U1);
            await _fixture.Directory.AddMemberAsync(q.Value, "ops", TenantSubjectKind.TenantGroup);
            await _fixture.Policy.PutRuleAsync(q.Value, TenantWideRule("ops-read", "ops", subjectKind: TenantSubjectKind.TenantGroup));
        }

        // The data is live: it changes decisions while the flag is on.
        await WaitAllowedAsync(U1, p, TreeOf(p, "invoices"), "the tenant group, member entry and tenant-wide rule admit u1 in P");
        await WaitAllowedAsync(U1, q, TreeOf(q, "orders"), "the tenant group, member entry and tenant-wide rule admit u1 in Q", key: "k2");

        IReadOnlyList<string> withEpicData;
        TenantAccessConformanceClusterFixture.Switch.Set(false);
        try
        {
            withEpicData = await DecideRequestSetAsync();
        }
        finally
        {
            TenantAccessConformanceClusterFixture.Switch.Set(true);
        }

        var differences = baseline
            .Zip(withEpicData, (before, after) => (before, after))
            .Where(pair => pair.before != pair.after)
            .Select(pair => $"{pair.before}  ->  {pair.after}")
            .ToList();
        Assert.Multiple(() =>
        {
            Assert.That(withEpicData, Has.Count.EqualTo(baseline.Count));
            Assert.That(baseline.Count(d => d.Contains("=allow", StringComparison.Ordinal)), Is.GreaterThan(0), "the request set is not all denials");
            Assert.That(differences, Is.Empty, "with the flag off, the epic's data changes no decision");
        });

        async Task<IReadOnlyList<string>> DecideRequestSetAsync()
        {
            var subjects = new[] { PatP, QuinnQ, U1, U2, U3, Outsider };
            var activeTenants = new string?[] { null, p.Value, q.Value };
            var trees = new[] { TreeOf(p, "orders"), TreeOf(p, "invoices"), TreeOf(p, "a/shop/items"), TreeOf(q, "orders"), Plain };
            var operations = new[] { LatticeOperation.Read, LatticeOperation.Write };
            var keys = new string?[] { "k1", "x1", null };
            var probes = new[] { "k1", "k2", "x1" };

            var decisions = new List<string>();
            foreach (var subject in subjects)
            {
                using (As(subject))
                {
                    foreach (var active in activeTenants)
                    {
                        foreach (var tree in trees)
                        {
                            foreach (var operation in operations)
                            {
                                foreach (var key in keys)
                                {
                                    var decision = await _fixture.DecideAsync(active, tree, operation, key);
                                    var line = new StringBuilder()
                                        .Append(subject).Append('|').Append(active ?? "-").Append('|').Append(tree)
                                        .Append('|').Append(operation).Append('|').Append(key ?? "*").Append('=')
                                        .Append(decision.Allowed ? "allow" : "deny");
                                    if (decision.Allowed && decision.KeyFilter is { } filter)
                                    {
                                        line.Append(" filter:");
                                        foreach (var probe in probes)
                                        {
                                            line.Append(filter(probe) ? '1' : '0');
                                        }
                                    }

                                    decisions.Add(line.ToString());
                                }
                            }
                        }
                    }
                }
            }

            return decisions;
        }
    }
}
