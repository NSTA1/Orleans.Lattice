using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Integration coverage, against a live single-silo cluster, for the tenant group
/// nesting invariant's permitted edges and for the
/// <see cref="ITenantScopedMembershipStore"/> half of the directory: tenant
/// counts, the ordered paged listing, the cascading group removal, and the tenant
/// purge (both edge directions, legacy cross-tier edges, an interrupted purge
/// completed by a re-run, and idempotency). Each test uses its own tenant so the
/// shared cluster's state cannot couple them.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeMembershipDirectoryTenantScopeIntegrationTests
{
    private const char Sep = '\u001f';

    private MembershipClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new MembershipClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeMembershipDirectory Directory => _fixture.Directory;

    private ITenantScopedMembershipStore Store => _fixture.SiloServices.GetTenantScopedMembershipStore();

    private ILattice RawEdges =>
        _fixture.SiloServices.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(MembershipConstants.EdgesTree);

    private ILattice RawGroups =>
        _fixture.SiloServices.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(MembershipConstants.GroupsTree);

    [Test]
    public async Task Permitted_nesting_edges_are_written_and_resolve()
    {
        await Directory.AddMemberAsync("t/nest/ops", "t/nest/admins", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/nest/admins", "nest-alice");
        await Directory.AddMemberAsync("t/nest/admins", "nest-entra", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("nest-entra", "nest-bob");

        Assert.Multiple(async () =>
        {
            Assert.That(await Directory.GroupsOfAsync("nest-alice"), Is.EquivalentTo(new[] { "t/nest/admins", "t/nest/ops" }));
            Assert.That(
                await Directory.GroupsOfAsync("nest-bob"),
                Is.EquivalentTo(new[] { "nest-entra", "t/nest/admins", "t/nest/ops" }),
                "a cluster group nested in a tenant group carries its members into the tenant group");
        });
    }

    [Test]
    public async Task Refused_nesting_edges_write_nothing()
    {
        await Directory.AddMemberAsync("refuse-cluster", "refuse-alice");

        Assert.That(
            async () => await Directory.AddMemberAsync("refuse-cluster", "t/refuse/admins", MembershipMemberKind.Group),
            Throws.TypeOf<LatticeTenantGroupNestingException>());
        Assert.That(
            async () => await Directory.AddMemberAsync("t/refuse-other/ops", "t/refuse/admins", MembershipMemberKind.Group),
            Throws.TypeOf<LatticeTenantGroupNestingException>());

        Assert.Multiple(async () =>
        {
            Assert.That(await Directory.MembersOfAsync("refuse-cluster"), Is.EquivalentTo(new[] { "refuse-alice" }));
            Assert.That(await Directory.MembersOfAsync("t/refuse-other/ops"), Is.Empty);
            Assert.That(await Directory.GroupsOfAsync("t/refuse/admins"), Is.Empty);
        });
    }

    [Test]
    public async Task Counts_cover_only_the_tenant_scope()
    {
        var tenant = TenantId.Parse("cnt");
        await Directory.UpsertGroupAsync(new MembershipGroup("t/cnt/a"));
        await Directory.UpsertGroupAsync(new MembershipGroup("t/cnt/b"));
        await Directory.UpsertGroupAsync(new MembershipGroup("t/cnt-2/a"));
        await Directory.AddMemberAsync("t/cnt/a", "cnt-alice");
        await Directory.AddMemberAsync("t/cnt/a", "t/cnt/b", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/cnt/b", "cnt-entra", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/cnt-2/a", "cnt-alice");

        Assert.Multiple(async () =>
        {
            Assert.That(await Store.CountTenantGroupsAsync(tenant), Is.EqualTo(2));
            Assert.That(await Store.CountTenantEdgesAsync(tenant), Is.EqualTo(3));
            Assert.That(await Store.CountTenantGroupsAsync(TenantId.Parse("cnt-empty")), Is.Zero);
        });
    }

    [Test]
    public async Task ListTenantGroupsAsync_pages_the_tenant_groups_in_order()
    {
        var tenant = TenantId.Parse("page");
        foreach (var name in new[] { "e", "c", "a", "d", "b" })
        {
            await Directory.UpsertGroupAsync(new MembershipGroup($"t/page/{name}"));
        }

        await Directory.UpsertGroupAsync(new MembershipGroup("t/page-2/a"));
        await Directory.UpsertGroupAsync(new MembershipGroup("t/pagd/z"));

        var seen = new List<string>();
        string? after = null;
        var pages = 0;
        do
        {
            var page = await Store.ListTenantGroupsAsync(tenant, after, pageSize: 2);
            seen.AddRange(page.Groups.Select(g => g.GroupId));
            after = page.ContinuationAfter;
            pages++;
        }
        while (after is not null && pages < 10);

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(new[] { "t/page/a", "t/page/b", "t/page/c", "t/page/d", "t/page/e" }));
            Assert.That(pages, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task ListTenantGroupsAsync_exact_page_reports_no_continuation()
    {
        var tenant = TenantId.Parse("exact");
        await Directory.UpsertGroupAsync(new MembershipGroup("t/exact/a"));
        await Directory.UpsertGroupAsync(new MembershipGroup("t/exact/b"));

        var page = await Store.ListTenantGroupsAsync(tenant, null, pageSize: 2);

        Assert.Multiple(() =>
        {
            Assert.That(page.Groups.Select(g => g.GroupId), Is.EqualTo(new[] { "t/exact/a", "t/exact/b" }));
            Assert.That(page.ContinuationAfter, Is.Null);
        });
    }

    [Test]
    public async Task RemoveGroupCascadeAsync_removes_the_group_and_its_edges_in_both_directions()
    {
        await Directory.UpsertGroupAsync(new MembershipGroup("t/cas/g"));
        await Directory.AddMemberAsync("t/cas/g", "cas-alice");
        await Directory.AddMemberAsync("t/cas/g", "cas-entra", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/cas/parent", "t/cas/g", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/cas/parent", "cas-bob");

        var removed = await Store.RemoveGroupCascadeAsync("t/cas/g");
        var again = await Store.RemoveGroupCascadeAsync("t/cas/g");

        Assert.Multiple(async () =>
        {
            Assert.That(removed, Is.EquivalentTo(new[]
            {
                new MembershipEdge("t/cas/parent", "t/cas/g"),
                new MembershipEdge("t/cas/g", "cas-alice"),
                new MembershipEdge("t/cas/g", "cas-entra"),
            }));
            Assert.That(again, Is.Empty, "the cascade is idempotent");
            Assert.That(await Directory.GetGroupAsync("t/cas/g"), Is.Null);
            Assert.That(await Directory.GroupsOfAsync("cas-alice"), Is.Empty);
            Assert.That(await Directory.GroupsOfAsync("cas-entra"), Is.Empty);
            Assert.That(await Directory.MembersOfAsync("t/cas/parent"), Is.EquivalentTo(new[] { "cas-bob" }));
            Assert.That(await EdgeRowsMentioningAsync("t/cas/g"), Is.Empty);
        });
    }

    [Test]
    public async Task PurgeTenantAsync_removes_groups_and_edges_in_both_directions()
    {
        var tenant = TenantId.Parse("pur");
        await Directory.UpsertGroupAsync(new MembershipGroup("t/pur/a"));
        await Directory.UpsertGroupAsync(new MembershipGroup("t/pur/b"));
        await Directory.UpsertGroupAsync(new MembershipGroup("t/pur-2/keep"));
        await Directory.AddMemberAsync("t/pur/a", "pur-alice");
        await Directory.AddMemberAsync("t/pur/a", "pur-entra", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/pur/b", "t/pur/a", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/pur-2/keep", "pur-alice");

        // A cross-tier edge written before the invariant existed: the tenant
        // group as a member of a cluster group. Only the forward scan finds it.
        await WriteRawEdgeAsync("pur-legacy-cluster", "t/pur/a");

        var result = await Store.PurgeTenantAsync(tenant);
        var rerun = await Store.PurgeTenantAsync(tenant);

        Assert.Multiple(async () =>
        {
            Assert.That(result, Is.EqualTo(new TenantMembershipPurgeResult(GroupsRemoved: 2, EdgesRemoved: 4)));
            Assert.That(rerun, Is.EqualTo(new TenantMembershipPurgeResult(0, 0)), "a completed purge re-runs as a no-op");
            Assert.That(await EdgeRowsMentioningAsync("t/pur/"), Is.Empty);
            Assert.That(await Directory.GroupsOfAsync("pur-alice"), Is.EquivalentTo(new[] { "t/pur-2/keep" }));
            Assert.That(await Directory.GroupsOfAsync("pur-entra"), Is.Empty);
            Assert.That(await Directory.MembersOfAsync("pur-legacy-cluster"), Is.Empty);
            Assert.That(await Directory.GetGroupAsync("t/pur/a"), Is.Null);
            Assert.That(await Directory.GetGroupAsync("t/pur-2/keep"), Is.Not.Null, "another tenant is untouched");
        });
    }

    [Test]
    public async Task PurgeTenantAsync_rerun_completes_an_interrupted_purge()
    {
        var tenant = TenantId.Parse("part");
        await Directory.UpsertGroupAsync(new MembershipGroup("t/part/a"));
        await Directory.UpsertGroupAsync(new MembershipGroup("t/part/b"));
        await Directory.AddMemberAsync("t/part/a", "part-alice");
        await Directory.AddMemberAsync("t/part/a", "part-entra", MembershipMemberKind.Group);
        await Directory.AddMemberAsync("t/part/b", "t/part/a", MembershipMemberKind.Group);
        await WriteRawEdgeAsync("part-legacy-cluster", "t/part/a");

        // The states an interrupted purge leaves: a counterpart row already gone
        // while the row that located it survives (both directions), and a group
        // record already removed.
        using (SystemOriginScope.Enter())
        {
            await RawEdges.DeleteAsync($"f{Sep}part-alice{Sep}t/part/a");
            await RawEdges.DeleteAsync($"r{Sep}part-legacy-cluster{Sep}t/part/a");
            await RawGroups.DeleteAsync("t/part/b");
        }

        var result = await Store.PurgeTenantAsync(tenant);
        var rerun = await Store.PurgeTenantAsync(tenant);

        Assert.Multiple(async () =>
        {
            Assert.That(result, Is.EqualTo(new TenantMembershipPurgeResult(GroupsRemoved: 1, EdgesRemoved: 4)));
            Assert.That(rerun, Is.EqualTo(new TenantMembershipPurgeResult(0, 0)));
            Assert.That(await EdgeRowsMentioningAsync("t/part/"), Is.Empty);
            Assert.That(await Directory.GetGroupAsync("t/part/a"), Is.Null);
        });
    }

    private async Task WriteRawEdgeAsync(string groupId, string memberId)
    {
        var marker = "g"u8.ToArray();
        using (SystemOriginScope.Enter())
        {
            await RawEdges.SetAsync($"f{Sep}{memberId}{Sep}{groupId}", marker);
            await RawEdges.SetAsync($"r{Sep}{groupId}{Sep}{memberId}", marker);
        }
    }

    private async Task<List<string>> EdgeRowsMentioningAsync(string fragment)
    {
        var rows = new List<string>();
        using (SystemOriginScope.Enter())
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
