using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>Confinement cases: the group nesting invariant (D3) and rule-subject confinement (D4, A1).</summary>
public sealed partial class TenantAccessConformanceTests
{
    [Test]
    public async Task Case04_neither_a_tenant_admin_nor_an_operator_can_make_a_tenant_group_a_member_of_a_cluster_group_or_of_another_tenants_group()
    {
        const string AdminA = "c04-alice";
        const string AdminB = "c04-bea";
        const string Dual = "c04-dual";
        const string ClusterGroup = "c04-entra";
        var a = await _fixture.SeedTenantAsync("c04-a", AdminA);
        var b = await _fixture.SeedTenantAsync("c04-b", AdminB);
        var x = GroupOf(a, "x");
        var staff = GroupOf(b, "staff");
        await _fixture.SeedClusterGroupAsync(ClusterGroup);

        using (As(AdminA))
        {
            await _fixture.Directory.UpsertGroupAsync(a.Value, new TenantGroupDescriptor { Name = "x" });
        }

        using (As(AdminB))
        {
            await _fixture.Directory.UpsertGroupAsync(b.Value, new TenantGroupDescriptor { Name = "staff" });
        }

        await _fixture.AddAdminAsync(a, Dual);
        await _fixture.AddAdminAsync(b, Dual);

        // Positive control: the same nesting in the allowed direction is accepted, so
        // the refusals below are the invariant, not a broken directory.
        using (As(AdminA))
        {
            await _fixture.Directory.AddGroupMemberAsync(a.Value, "x", ClusterGroup, TenantSubjectKind.ClusterGroup);
        }

        // The operator, through the membership directory itself (system origin).
        using (LatticeSystemOrigin.Enter())
        {
            Assert.Multiple(() =>
            {
                Assert.That(
                    () => _fixture.Membership.AddMemberAsync(ClusterGroup, x, MembershipMemberKind.Group),
                    Throws.TypeOf<LatticeTenantGroupNestingException>(),
                    "the directory refuses a tenant group inside a cluster group, even under system origin");
                Assert.That(
                    () => _fixture.Membership.AddMemberAsync(staff, x, MembershipMemberKind.Group),
                    Throws.TypeOf<LatticeTenantGroupNestingException>(),
                    "the directory refuses a tenant group inside another tenant's group, even under system origin");
            });
        }

        // The operator, through the cluster auth facade.
        using (As(Operator))
        {
            Assert.Multiple(() =>
            {
                Assert.That(
                    () => _fixture.AuthAdmin.AddMemberAsync(ClusterGroup, x, MembershipMemberKind.Group),
                    Throws.TypeOf<LatticeTenantGroupNestingException>());
                Assert.That(
                    () => _fixture.AuthAdmin.AddMemberAsync(staff, x, MembershipMemberKind.Group),
                    Throws.TypeOf<LatticeTenantGroupNestingException>());
                Assert.That(
                    () => _fixture.Directory.AddGroupMemberAsync(b.Value, "staff", x, TenantSubjectKind.ClusterGroup),
                    Throws.TypeOf<TenantAccessConfinementException>()
                        .With.Property(nameof(TenantAccessConfinementException.Rule)).EqualTo(TenantAccessConfinementRule.ForeignTenantGroup),
                    "the tenant directory facade refuses the operator too");
            });
        }

        // A tenant admin of A: no tenant-tier surface reaches outside A, and the
        // cluster facade is operator-only.
        using (As(AdminA))
        {
            Assert.Multiple(() =>
            {
                Assert.That(
                    () => _fixture.Directory.AddGroupMemberAsync(b.Value, "staff", x, TenantSubjectKind.ClusterGroup),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>());
                Assert.That(
                    () => _fixture.AuthAdmin.AddMemberAsync(ClusterGroup, x, MembershipMemberKind.Group),
                    Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            });
        }

        // An admin of both tenants is still confined.
        using (As(Dual))
        {
            Assert.That(
                () => _fixture.Directory.AddGroupMemberAsync(b.Value, "staff", x, TenantSubjectKind.ClusterGroup),
                Throws.TypeOf<TenantAccessConfinementException>()
                    .With.Property(nameof(TenantAccessConfinementException.Rule)).EqualTo(TenantAccessConfinementRule.ForeignTenantGroup));
        }

        using (LatticeSystemOrigin.Enter())
        {
            var parentsOfX = await _fixture.Membership.GroupsOfAsync(x);
            var clusterMembers = await _fixture.Membership.MembersOfAsync(ClusterGroup);
            var staffMembers = await _fixture.Membership.MembersOfAsync(staff);
            Assert.Multiple(() =>
            {
                Assert.That(parentsOfX, Is.Empty, "nothing refused was written: the tenant group joined no group");
                Assert.That(clusterMembers, Does.Not.Contain(x));
                Assert.That(staffMembers, Does.Not.Contain(x));
            });
        }
    }

    [Test]
    public async Task Case06_a_rule_naming_a_tenant_group_cannot_be_scoped_outside_its_tenant_by_anyone_and_an_app_binding_in_another_tenant_naming_it_is_refused()
    {
        const string AdminA = "c06-alice";
        const string AdminB = "c06-bea";
        var a = await _fixture.SeedTenantAsync("c06-a", AdminA);
        var b = await _fixture.SeedTenantAsync("c06-b", AdminB);
        var x = GroupOf(a, "x");
        var subject = LatticeSubjectSelector.Group(x);

        using (As(AdminA))
        {
            await _fixture.Directory.UpsertGroupAsync(a.Value, new TenantGroupDescriptor { Name = "x" });
        }

        var refusedScopes = new (string Name, LatticeScope Scope)[]
        {
            ("another tenant's tree", LatticeScope.Tree(TreeOf(b, "orders"))),
            ("a key of another tenant's tree", LatticeScope.Key(TreeOf(b, "orders"), "k1")),
            ("a prefix of another tenant's tree", LatticeScope.Prefix(TreeOf(b, "orders"), "k")),
            ("another tenant's tenant-wide scope", LatticeScope.TenantWide(b)),
            ("a platform sys- tree", LatticeScope.Tree("sys-audit")),
            ("a sys- tree under its own tenant", LatticeScope.Tree(TreeOf(a, "sys-notes"))),
            ("its own tenant's app tree", LatticeScope.Tree(TreeOf(a, "a/shop/items"))),
            ("another tenant's app tree", LatticeScope.Tree(TreeOf(b, "a/shop/items"))),
            ("Tree:*", LatticeScope.ClusterWide()),
            ("a legacy tree", LatticeScope.Tree("c06-plain")),
        };

        // The operator, through the cluster auth facade.
        using (As(Operator))
        {
            Assert.Multiple(() =>
            {
                foreach (var (name, scope) in refusedScopes)
                {
                    Assert.That(
                        () => _fixture.AuthAdmin.PutRuleAsync(new LatticeAuthorizationRule(
                            "c06-op", subject, scope, LatticeOperation.Read, LatticeEffect.Allow)),
                        Throws.InstanceOf<ArgumentException>(),
                        $"operator, cluster facade: {name}");
                }
            });
        }

        // Anyone writing the policy store under system origin, including the app
        // compiler: an app: rule may target only its own tenant's app trees.
        using (LatticeSystemOrigin.Enter())
        {
            Assert.Multiple(() =>
            {
                foreach (var (name, scope) in refusedScopes)
                {
                    if (scope.TreeId == TreeOf(a, "a/shop/items"))
                    {
                        continue;
                    }

                    Assert.That(
                        () => _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
                            "c06-system", subject, scope, LatticeOperation.Read, LatticeEffect.Allow)),
                        Throws.InstanceOf<ArgumentException>(),
                        $"system origin: {name}");
                    Assert.That(
                        () => _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
                            "app:shop:reader:c06", subject, scope, LatticeOperation.Read, LatticeEffect.Allow)),
                        Throws.InstanceOf<ArgumentException>(),
                        $"app rule, system origin: {name}");
                }

                Assert.That(
                    () => _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
                        "c06-system", subject, LatticeScope.Tree(TreeOf(a, "a/shop/items")), LatticeOperation.Read, LatticeEffect.Allow)),
                    Throws.InstanceOf<ArgumentException>(),
                    "system origin, non-app rule: its own tenant's app tree");
            });
        }

        // The tenant policy facade, for an admin of A, of B, and the operator.
        using (As(AdminA))
        {
            Assert.Multiple(() =>
            {
                Assert.That(
                    () => _fixture.Policy.PutRuleAsync(a.Value, TreeRule("x-app", "x", "a/shop/items", subjectKind: TenantSubjectKind.TenantGroup)),
                    Throws.TypeOf<TenantAccessConfinementException>()
                        .With.Property(nameof(TenantAccessConfinementException.Rule)).EqualTo(TenantAccessConfinementRule.RuleTree));
                Assert.That(
                    () => _fixture.Policy.PutRuleAsync(a.Value, TreeRule("x-sys", "x", "sys-notes", subjectKind: TenantSubjectKind.TenantGroup)),
                    Throws.TypeOf<TenantAccessConfinementException>()
                        .With.Property(nameof(TenantAccessConfinementException.Rule)).EqualTo(TenantAccessConfinementRule.RuleTree));
                Assert.That(
                    () => _fixture.Policy.PutRuleAsync(b.Value, TreeRule("x-on-b", "x", "orders", subjectKind: TenantSubjectKind.TenantGroup)),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>());
            });
        }

        foreach (var caller in new[] { AdminB, Operator })
        {
            using (As(caller))
            {
                Assert.That(
                    () => _fixture.Policy.PutRuleAsync(b.Value, TreeRule("foreign-x", x, "orders", subjectKind: TenantSubjectKind.ClusterGroup)),
                    Throws.TypeOf<TenantAccessConfinementException>()
                        .With.Property(nameof(TenantAccessConfinementException.Rule)).EqualTo(TenantAccessConfinementRule.ForeignTenantGroup),
                    $"{caller}: B's rule naming A's group");
            }
        }

        // A1: an app installed in B may not bind a role to A's group.
        using (As(Operator))
        {
            var request = new AppRegistryInstallRequest
            {
                Tenant = b,
                Identity = new AppIdentity { Slug = AppSlug.Parse("shop"), Version = AppVersion.Parse("1.0.0") },
                Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read),
                RoleBindings = [AppRoleBinding.Create("reader", x)],
            };
            var ex = Assert.ThrowsAsync<ArgumentException>(() => _fixture.Apps.InstallAsync(request));
            Assert.That(ex!.Message, Does.Contain("not a group of the installing tenant"));
            Assert.That(await _fixture.Apps.GetAsync(b, AppSlug.Parse("shop")), Is.Null, "nothing was installed");
        }

        // Positive controls, so the refusals above are the confinement and not a
        // store that refuses every group rule: the in-tenant scopes are admitted.
        using (LatticeSystemOrigin.Enter())
        {
            await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
                "c06-own-tree", subject, LatticeScope.Tree(TreeOf(a, "orders")), LatticeOperation.Read, LatticeEffect.Allow));
            await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
                "app:shop:reader:c06", subject, LatticeScope.Tree(TreeOf(a, "a/shop/items")), LatticeOperation.Read, LatticeEffect.Allow));

            var written = new List<LatticeAuthorizationRule>();
            await foreach (var rule in _fixture.Store.ListRulesAsync())
            {
                if (rule.Subject == subject)
                {
                    written.Add(rule);
                }
            }

            Assert.That(
                written.Select(r => r.Scope.TreeId),
                Is.EquivalentTo(new[] { TreeOf(a, "orders"), TreeOf(a, "a/shop/items") }),
                "only the in-tenant rules naming the group were ever written");

            await _fixture.Store.RemoveRuleAsync(TreeOf(a, "orders"), "c06-own-tree");
            await _fixture.Store.RemoveRuleAsync(TreeOf(a, "a/shop/items"), "app:shop:reader:c06");
        }
    }
}
