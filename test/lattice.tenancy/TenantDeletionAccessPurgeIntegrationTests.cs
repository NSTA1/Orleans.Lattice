using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Integration coverage, against a live single-silo cluster, for the access-data
/// purge in the tenant deletion pipeline (epic #4154, D12): deleting a tenant
/// removes its tenant groups, every edge into or out of them, its member set and
/// its tenant-tier rules while leaving cluster groups and other tenants intact; a
/// crash between the rule purge and the group purge is completed by the resumed
/// delete; and the purge runs with the delegated tenant access flag off. Each test
/// uses its own tenant so the shared cluster's state cannot couple them.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class TenantDeletionAccessPurgeIntegrationTests
{
    private TenancyClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new TenancyClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ITenantRegistry Registry => _fixture.Registry;

    private ILatticeMembershipDirectory Directory => _fixture.SiloServices.GetRequiredService<ILatticeMembershipDirectory>();

    private ITenantScopedMembershipStore MembershipStore => _fixture.SiloServices.GetTenantScopedMembershipStore();

    private ILatticeAuthorizationPolicyStore PolicyStore => _fixture.SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    private DelegatedTenantAccessFlag Flag => _fixture.SiloServices.GetRequiredService<DelegatedTenantAccessFlag>();

    private ILattice RawEdges =>
        _fixture.SiloServices.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(MembershipConstants.EdgesTree);

    [Test]
    public async Task Deleting_a_tenant_purges_its_groups_edges_members_and_rules_and_keeps_cluster_groups()
    {
        Flag.Set(true);
        try
        {
            var seeded = await SeedAsync("delall");

            var removed = await Registry.DeleteAsync(seeded.Tenant);

            Assert.That(removed, Is.True);
            await AssertPurgedAsync(seeded);
        }
        finally
        {
            Flag.Set(false);
        }
    }

    [Test]
    public async Task A_crash_between_the_two_purges_is_completed_by_the_resumed_delete()
    {
        var seeded = await SeedAsync("delcrash");

        // The rule purge completes, then the process "crashes" before the group
        // purge: the record and the groups are still there.
        var real = TenantAccessDataPurge.FromServices(_fixture.SiloServices);
        var crashing = new TenantAccessDataPurge(
            real.PurgeRules,
            (_, _) => Task.FromException<TenantMembershipPurgeResult>(new InvalidOperationException("simulated crash")));
        Assert.That(async () => await crashing.PurgeAsync(seeded.Tenant), Throws.InvalidOperationException);

        Assert.Multiple(async () =>
        {
            Assert.That(await TenantRuleIdsAsync(seeded.Name), Is.Empty, "the rule purge ran before the crash");
            Assert.That(await MembershipStore.CountTenantGroupsAsync(seeded.Tenant), Is.EqualTo(2), "the group purge did not");
            Assert.That(await Registry.GetAsync(seeded.Tenant), Is.Not.Null, "the record survives the crash");
        });

        // The operator re-runs the delete.
        var removed = await Registry.DeleteAsync(seeded.Tenant);

        Assert.That(removed, Is.True);
        await AssertPurgedAsync(seeded);
    }

    [Test]
    public async Task With_the_flag_off_the_purge_still_runs()
    {
        Assert.That(Flag.IsEnabled, Is.False, "the fixture runs with delegated tenant access off");
        var seeded = await SeedAsync("deloff");

        var removed = await Registry.DeleteAsync(seeded.Tenant);

        Assert.That(removed, Is.True);
        await AssertPurgedAsync(seeded);
    }

    private sealed record Seeded(string Name, TenantId Tenant, string AdminsGroup, string OpsGroup, string ClusterGroup);

    /// <summary>
    /// Seeds a tenant with a record carrying member subjects, two tenant groups
    /// (one nested in the other), a user member, a cluster group nested into a
    /// tenant group (with its own member), a tenant-wide and a key-scoped tenant
    /// rule, plus a sibling tenant's group and rule and an operator rule that must
    /// all survive the delete.
    /// </summary>
    private async Task<Seeded> SeedAsync(string name)
    {
        var tenant = TenantId.Parse(name);
        var admins = $"t/{name}/admins";
        var ops = $"t/{name}/ops";
        var cluster = $"{name}-entra";
        var sibling = TenantId.Parse($"{name}-sib");

        var record = TenantRecord.Create(tenant, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(10), "test");
        record.AddMemberSubject($"{name}-carol", Clock(11), "test");
        record.AddMemberSubject(ops, Clock(12), "test");
        await Registry.PutAsync(record);

        using (LatticeSystemOrigin.Enter())
        {
            await Directory.UpsertGroupAsync(new MembershipGroup(admins));
            await Directory.UpsertGroupAsync(new MembershipGroup(ops));
            await Directory.UpsertGroupAsync(new MembershipGroup(cluster));
            await Directory.AddMemberAsync(admins, $"{name}-alice");
            await Directory.AddMemberAsync(ops, admins, MembershipMemberKind.Group);
            await Directory.AddMemberAsync(admins, cluster, MembershipMemberKind.Group);
            await Directory.AddMemberAsync(cluster, $"{name}-bob");
            await Directory.AddMemberAsync($"t/{sibling.Value}/keep", $"{name}-alice");

            await PolicyStore.PutRuleAsync(new LatticeAuthorizationRule(
                LatticeTenantRuleIds.For(tenant, "wide"),
                LatticeSubjectSelector.Group(admins),
                LatticeScope.TenantWide(tenant),
                LatticeOperation.Read,
                LatticeEffect.Allow));
            await PolicyStore.PutRuleAsync(new LatticeAuthorizationRule(
                LatticeTenantRuleIds.For(tenant, "orders"),
                LatticeSubjectSelector.User($"{name}-alice"),
                LatticeScope.Key($"t/{name}/orders", "k1"),
                LatticeOperation.Write,
                LatticeEffect.Allow));
            await PolicyStore.PutRuleAsync(new LatticeAuthorizationRule(
                LatticeTenantRuleIds.For(sibling, "keep"),
                LatticeSubjectSelector.User($"{name}-alice"),
                LatticeScope.Tree($"t/{sibling.Value}/orders"),
                LatticeOperation.Read,
                LatticeEffect.Allow));
            await PolicyStore.PutRuleAsync(new LatticeAuthorizationRule(
                $"{name}-operator",
                LatticeSubjectSelector.Group(cluster),
                LatticeScope.Tree($"{name}-cluster-tree"),
                LatticeOperation.Read,
                LatticeEffect.Allow));
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await MembershipStore.CountTenantGroupsAsync(tenant), Is.EqualTo(2));
            Assert.That(await TenantRuleIdsAsync(name), Has.Count.EqualTo(2));
            Assert.That(await EdgeRowsMentioningAsync($"t/{name}/"), Is.Not.Empty);
        });

        return new Seeded(name, tenant, admins, ops, cluster);
    }

    private async Task AssertPurgedAsync(Seeded seeded)
    {
        var sibling = TenantId.Parse($"{seeded.Name}-sib");
        IReadOnlyList<string> allRuleIds;
        using (LatticeSystemOrigin.Enter())
        {
            allRuleIds = await RuleIdsAsync();
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await Registry.GetAsync(seeded.Tenant), Is.Null, "the record, and with it the member set, is gone");
            Assert.That(await MembershipStore.CountTenantGroupsAsync(seeded.Tenant), Is.Zero);
            Assert.That(await Directory.GetGroupAsync(seeded.AdminsGroup), Is.Null);
            Assert.That(await Directory.GetGroupAsync(seeded.OpsGroup), Is.Null);
            Assert.That(await EdgeRowsMentioningAsync($"t/{seeded.Name}/"), Is.Empty, "no edge into or out of a tenant group remains");
            Assert.That(await TenantRuleIdsAsync(seeded.Name), Is.Empty, "no tenant-tier rule remains");

            Assert.That(await Directory.GetGroupAsync(seeded.ClusterGroup), Is.Not.Null, "the cluster group survives");
            Assert.That(
                await Directory.MembersOfAsync(seeded.ClusterGroup),
                Is.EquivalentTo(new[] { $"{seeded.Name}-bob" }),
                "the cluster group keeps its own members");
            Assert.That(
                await Directory.GroupsOfAsync($"{seeded.Name}-alice"),
                Is.EquivalentTo(new[] { $"t/{sibling.Value}/keep" }),
                "another tenant's group is untouched");
            Assert.That(allRuleIds, Does.Contain(LatticeTenantRuleIds.For(sibling, "keep")), "another tenant's rule is untouched");
            Assert.That(allRuleIds, Does.Contain($"{seeded.Name}-operator"), "operator rules are untouched");
        });
    }

    private async Task<List<string>> TenantRuleIdsAsync(string name)
    {
        var prefix = $"tenant:{name}:";
        List<string> all;
        using (LatticeSystemOrigin.Enter())
        {
            all = await RuleIdsAsync();
        }

        return all.FindAll(id => id.StartsWith(prefix, StringComparison.Ordinal));
    }

    private async Task<List<string>> RuleIdsAsync()
    {
        var ids = new List<string>();
        await foreach (var rule in PolicyStore.ListRulesAsync())
        {
            ids.Add(rule.RuleId);
        }

        return ids;
    }

    private async Task<List<string>> EdgeRowsMentioningAsync(string fragment)
    {
        var rows = new List<string>();
        using (LatticeSystemOrigin.Enter())
        {
            await foreach (var key in RawEdges.ScanKeysAsync())
            {
                if (key.Contains(fragment, StringComparison.Ordinal))
                {
                    rows.Add(key);
                }
            }
        }

        return rows;
    }
}
