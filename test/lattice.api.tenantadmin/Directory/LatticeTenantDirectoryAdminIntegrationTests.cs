using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// End-to-end coverage of the tenant directory facade on a real single-silo cluster
/// with auth, membership and tenancy, delegated tenant access administration enabled:
/// a tenant admin builds a group of a user and a cluster group and puts it in the
/// member set, after which the user can act as the tenant; isolation between
/// tenants; operator reach; a group admin; the caps; the removal cascade; and
/// last-admin protection. Every tenant is distinct per test, so tests are order
/// independent.
/// </summary>
/// <remarks>Owned by the epic coordinator's integration run.</remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTenantDirectoryAdminIntegrationTests
{
    private const string Operator = TenantDirectoryClusterFixture.Operator;

    private readonly TenantDirectoryClusterFixture _fixture = new(delegatedAccessEnabled: true);

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task A_tenant_admin_builds_a_group_and_its_members_can_then_act_as_the_tenant()
    {
        await _fixture.SeedTenantAsync("acme", adminSubjects: "alice");
        using (LatticeSystemOrigin.Enter())
        {
            await _fixture.Membership.UpsertGroupAsync(new MembershipGroup("entra-devs"));
            await _fixture.Membership.AddMemberAsync("entra-devs", "dave");
        }

        using (TenantDirectoryClusterFixture.As("alice"))
        {
            await _fixture.Directory.UpsertGroupAsync("acme", new TenantGroupDescriptor { Name = "eng", DisplayName = "Engineering" });
            await _fixture.Directory.AddGroupMemberAsync("acme", "eng", "bob");
            await _fixture.Directory.AddGroupMemberAsync("acme", "eng", "entra-devs", TenantSubjectKind.ClusterGroup);
        }

        Assert.That(await ActsAsTenantAsync("bob", "acme"), Is.False, "a group member outside the member set is not a tenant member");

        using (TenantDirectoryClusterFixture.As("alice"))
        {
            await _fixture.Directory.AddMemberAsync("acme", "eng", TenantSubjectKind.TenantGroup);

            var members = await _fixture.Directory.ListGroupMembersAsync("acme", "eng");
            Assert.That(members, Is.EqualTo(new[]
            {
                new TenantGroupMember { MemberId = "bob", Kind = TenantSubjectKind.User },
                new TenantGroupMember { MemberId = "entra-devs", Kind = TenantSubjectKind.ClusterGroup },
            }));

            var resolution = await _fixture.Directory.ResolveSubjectAsync("acme", "dave");
            Assert.That(resolution.IsMember, Is.True, "a cluster-group member resolves through the tenant group");
        }

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await WaitUntilActsAsTenantAsync("bob", "acme"), Is.True, "the direct user member can act as the tenant");
            Assert.That(await WaitUntilActsAsTenantAsync("dave", "acme"), Is.True, "a member of the nested cluster group can act as the tenant");
            Assert.That(await ActsAsTenantAsync("mallory", "acme"), Is.False);
        });
    }

    [Test]
    public async Task A_tenant_admin_of_one_tenant_cannot_touch_another()
    {
        await _fixture.SeedTenantAsync("alpha", adminSubjects: "ann");
        await _fixture.SeedTenantAsync("beta", adminSubjects: "ben");

        using (TenantDirectoryClusterFixture.As("ann"))
        {
            Assert.Multiple(() =>
            {
                Assert.That(
                    async () => await _fixture.Directory.UpsertGroupAsync("beta", new TenantGroupDescriptor { Name = "x" }),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>());
                Assert.That(
                    async () => await _fixture.Directory.ListGroupsAsync("beta", new TenantAccessPageRequest()),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>());
                Assert.That(
                    async () => await _fixture.Directory.ListGroupsAsync("no-such-tenant", new TenantAccessPageRequest()),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                    "a missing tenant reads as a denial, never a not-found");
            });
        }
    }

    [Test]
    public async Task A_platform_operator_can_act_on_any_tenant()
    {
        await _fixture.SeedTenantAsync("gamma", adminSubjects: "gail");

        using (TenantDirectoryClusterFixture.As(Operator))
        {
            await _fixture.Directory.UpsertGroupAsync("gamma", new TenantGroupDescriptor { Name = "ops" });
            var page = await _fixture.Directory.ListGroupsAsync("gamma", new TenantAccessPageRequest());

            Assert.That(page.Entries.Select(e => e.Name), Is.EqualTo(new[] { "ops" }));
        }
    }

    [Test]
    public async Task A_group_admin_is_authorized()
    {
        await _fixture.SeedTenantAsync("delta", adminSubjects: "dina");

        using (TenantDirectoryClusterFixture.As("dina"))
        {
            await _fixture.Directory.UpsertGroupAsync("delta", new TenantGroupDescriptor { Name = "admins" });
            await _fixture.Directory.AddGroupMemberAsync("delta", "admins", "erin");
            await _fixture.AccessAdmin.AddAdminSubjectAsync("delta", "t/delta/admins");
        }

        using (TenantDirectoryClusterFixture.As("erin"))
        {
            var created = await _fixture.Directory.UpsertGroupAsync("delta", new TenantGroupDescriptor { Name = "made-by-erin" });
            Assert.That(created.Name, Is.EqualTo("made-by-erin"));
        }

        using (TenantDirectoryClusterFixture.As("frank"))
        {
            Assert.That(
                async () => await _fixture.Directory.ListGroupsAsync("delta", new TenantAccessPageRequest()),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        }
    }

    [Test]
    public async Task The_caps_refuse_additions_beyond_them()
    {
        await _fixture.SeedTenantAsync(
            "capped",
            new TenantQuotas { MaxGroups = 1, MaxMembershipEdges = 1, MaxMemberSubjects = 1 },
            "cap-admin");

        using (TenantDirectoryClusterFixture.As("cap-admin"))
        {
            await _fixture.Directory.UpsertGroupAsync("capped", new TenantGroupDescriptor { Name = "one" });
            await _fixture.Directory.AddGroupMemberAsync("capped", "one", "u1");
            await _fixture.Directory.AddMemberAsync("capped", "u1");

            Assert.Multiple(() =>
            {
                Assert.That(
                    async () => await _fixture.Directory.UpsertGroupAsync("capped", new TenantGroupDescriptor { Name = "two" }),
                    Throws.TypeOf<LatticeQuotaExceededException>().With.Property(nameof(LatticeQuotaExceededException.Dimension)).EqualTo(TenantAccessCaps.GroupsDimension));
                Assert.That(
                    async () => await _fixture.Directory.AddGroupMemberAsync("capped", "one", "u2"),
                    Throws.TypeOf<LatticeQuotaExceededException>().With.Property(nameof(LatticeQuotaExceededException.Dimension)).EqualTo(TenantAccessCaps.MembershipEdgesDimension));
                Assert.That(
                    async () => await _fixture.Directory.AddMemberAsync("capped", "u2"),
                    Throws.TypeOf<LatticeQuotaExceededException>().With.Property(nameof(LatticeQuotaExceededException.Dimension)).EqualTo(TenantAccessCaps.MemberSubjectsDimension));
            });
        }
    }

    [Test]
    public async Task Removing_a_group_cascades_its_edges_entries_and_tenant_rules()
    {
        await _fixture.SeedTenantAsync("eps", adminSubjects: "eve");

        using (TenantDirectoryClusterFixture.As("eve"))
        {
            await _fixture.Directory.UpsertGroupAsync("eps", new TenantGroupDescriptor { Name = "eng" });
            await _fixture.Directory.UpsertGroupAsync("eps", new TenantGroupDescriptor { Name = "all" });
            await _fixture.Directory.AddGroupMemberAsync("eps", "eng", "bob");
            await _fixture.Directory.AddGroupMemberAsync("eps", "all", "eng", TenantSubjectKind.TenantGroup);
            await _fixture.Directory.AddMemberAsync("eps", "eng", TenantSubjectKind.TenantGroup);
            await _fixture.AccessAdmin.AddAdminSubjectAsync("eps", "t/eps/eng");
        }

        var rule = new LatticeAuthorizationRule(
            LatticeTenantRuleIds.For(TenantId.Parse("eps"), "read-orders"),
            LatticeSubjectSelector.Group("t/eps/eng"),
            LatticeScope.Tree("t/eps/orders"),
            LatticeOperation.Read,
            LatticeEffect.Allow);
        using (LatticeSystemOrigin.Enter())
        {
            await _fixture.Policy.PutRuleAsync(rule);
        }

        TenantGroupRemovalResult result;
        using (TenantDirectoryClusterFixture.As("eve"))
        {
            result = await _fixture.Directory.RemoveGroupAsync("eps", "eng");
        }

        var record = await _fixture.Registry.GetAsync(TenantId.Parse("eps"));
        using (LatticeSystemOrigin.Enter())
        {
            await Assert.MultipleAsync(async () =>
            {
                Assert.That(result.Removed, Is.True);
                Assert.That(result.EdgesRemoved, Is.EqualTo(2));
                Assert.That(result.RemovedFromMemberSet, Is.True);
                Assert.That(result.RemovedFromAdminSet, Is.True);
                Assert.That(result.RemovedRuleIds, Is.EqualTo(new[] { "read-orders" }));
                Assert.That(await _fixture.Membership.GetGroupAsync("t/eps/eng"), Is.Null);
                Assert.That(await _fixture.Membership.MembersOfAsync("t/eps/all"), Is.Empty);
                Assert.That(await _fixture.Membership.GroupsOfAsync("bob"), Does.Not.Contain("t/eps/eng"));
                Assert.That(record!.HasMemberSubject("t/eps/eng"), Is.False);
                Assert.That(record.HasAdminSubject("t/eps/eng"), Is.False);
                Assert.That(record.HasAdminSubject("eve"), Is.True);
                Assert.That(await _fixture.Policy.GetRuleAsync("t/eps/orders", rule.RuleId), Is.Null);
            });
        }
    }

    [Test]
    public async Task Removing_the_last_admin_entry_is_refused()
    {
        await _fixture.SeedTenantAsync("solo", adminSubjects: "sam");

        using (TenantDirectoryClusterFixture.As("sam"))
        {
            await _fixture.Directory.UpsertGroupAsync("solo", new TenantGroupDescriptor { Name = "admins" });
            await _fixture.Directory.AddGroupMemberAsync("solo", "admins", "sam");
            await _fixture.AccessAdmin.AddAdminSubjectAsync("solo", "t/solo/admins");
            await _fixture.AccessAdmin.RemoveAdminSubjectAsync("solo", "sam");

            // sam still administers the tenant through the group.
            Assert.That(
                async () => await _fixture.Directory.RemoveGroupAsync("solo", "admins"),
                Throws.TypeOf<TenantLastAdminSubjectException>());
            Assert.That(await _fixture.Directory.GetGroupAsync("solo", "admins"), Is.Not.Null);
        }
    }

    private async Task<bool> ActsAsTenantAsync(string subjectId, string tenantId)
    {
        var groups = await _fixture.GroupsOfAsync(subjectId);
        return _fixture.TenantPolicy.ValidateActiveTenant(subjectId, groups, TenantId.Parse(tenantId)).Allowed;
    }

    /// <summary>
    /// Polls the tenant policy engine until it admits the subject or a deadline
    /// passes: the compiled tenant policy rebuilds asynchronously after a registry
    /// write, so the check is against a deadline rather than a fixed sleep.
    /// </summary>
    private async Task<bool> WaitUntilActsAsTenantAsync(string subjectId, string tenantId)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            if (await ActsAsTenantAsync(subjectId, tenantId))
            {
                return true;
            }

            if (DateTime.UtcNow >= deadline)
            {
                return false;
            }

            await Task.Delay(50);
        }
    }
}
