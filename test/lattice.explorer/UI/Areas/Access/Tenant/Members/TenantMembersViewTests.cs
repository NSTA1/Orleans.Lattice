using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Members;

/// <summary>
/// Issue #4162: the tenant's member set (<c>/t/acme/access/members</c>) - its entries
/// with each one's kind, add through the tenant-aware picker and remove, the
/// default-deny explanation, the administrators listed read-only as implicit members
/// with a link to Tenancy, the <c>MaxMemberSubjects</c> cap, and every loading,
/// empty, error and feature-off state.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantMembersViewTests : TenantAccessPagesTestContext
{
    [Test]
    public async Task The_member_set_is_listed_with_each_entrys_kind()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops");
        await AddMemberAsync("alice", TenantSubjectKind.User);
        await AddMemberAsync("ops", TenantSubjectKind.TenantGroup);
        await AddMemberAsync("entra-sre", TenantSubjectKind.ClusterGroup);

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Is.EqualTo(new[]
            {
                new[] { "alice", "User" },
                new[] { "entra-sre", "Cluster group" },
                new[] { "ops", "This tenant's group" },
            }));
            Assert.That(cut.Find("[data-lt-tenant-view=members] tbody a").GetAttribute("href"), Does.EndWith("t/acme/access/groups/ops"));
            Assert.That(cut.Find("[data-lt-tenant-view=members] .lt-access-count").TextContent, Is.EqualTo("3 members"));
        });
    }

    [Test]
    public void The_page_explains_that_rules_decide_and_the_default_is_deny()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-default-deny]").TextContent,
            Does.Contain("rules decide what it can do").And.Contain("default-deny")));
    }

    [Test]
    public void An_empty_member_set_shows_the_empty_state_and_still_offers_add()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=members]").TextContent, Does.Contain("no members besides its administrators"));
            Assert.That(AccessForms.Button(cut, "Add member").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void While_the_member_set_is_read_a_skeleton_is_shown()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(new TaskCompletionSource<TenantMemberPage>().Task);
        UseDirectory(directory);

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=members]").InnerHtml, Does.Contain("Loading members")));
    }

    [Test]
    public void A_failed_read_shows_the_error_state()
    {
        TenantFacades.AsTenantAdmin();
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        directory.ListMembersAsync(default!, default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantMemberPage>(new InvalidOperationException("down")));
        UseDirectory(directory);

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=members]").TextContent, Does.Contain("Members could not be read"));
            Assert.That(AccessForms.Button(cut, "Try again"), Is.Not.Null);
        });
    }

    [Test]
    public void A_member_is_added_through_the_picker()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member"), Is.True));

        AccessForms.Type(cut, "Member", "alice");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Is.EqualTo(new[] { new[] { "alice", "User" } }));
            Assert.That(TenantFacades.Gate.Calls, Does.Contain(nameof(ILatticeTenantDirectoryAdmin.AddMemberAsync)));
        });
    }

    [Test]
    public void Another_tenants_group_is_refused_by_the_picker()
    {
        TenantFacades.AsTenantAdmin();
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(AccessForms.HasField(cut, "Member kind"), Is.True));

        AccessForms.Choose(cut, "Member kind", "cluster-group");
        AccessForms.Type(cut, "Member", "t/globex/ops");
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Member"), Is.EqualTo(AccessSubjectPicker.ForeignTenantGroupMessage));
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.AddMemberAsync)));
        });
    }

    [Test]
    public async Task A_member_is_removed_after_a_confirmation()
    {
        TenantFacades.AsTenantAdmin();
        await AddMemberAsync("alice", TenantSubjectKind.User);
        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(1)));

        cut.Find("button[aria-label='Remove alice']").Click();
        AccessForms.Type(cut, "Member name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Is.Empty);
            Assert.That(TenantFacades.Gate.Calls, Does.Contain(nameof(ILatticeTenantDirectoryAdmin.RemoveMemberAsync)));
        });
    }

    [Test]
    public void At_the_cap_adding_is_disabled_with_the_reason()
    {
        TenantFacades.AsTenantAdmin();
        TenantFacades.PolicyFake.MemberSubjects = new TenantQuotaDimensionUsage { Usage = 5, Limit = 5 };

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(AccessForms.Button(cut, "Add member").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-cap=member-subjects]").TextContent, Is.EqualTo("5 of 5 members"));
            Assert.That(cut.Find("[data-lt-cap-reason=member-subjects]").TextContent, Does.Contain("cap of 5 members"));
        });
    }

    [Test]
    public void The_administrators_are_listed_read_only_with_a_link_to_tenancy()
    {
        TenantFacades.AsTenantAdmin();
        TenantAccessAdmin.ListAdminSubjectsAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantAdminSubjectReport { TenantId = "acme", Subjects = ["zed@example.com", "t/acme/ops", "ann@example.com", "t/globex/ops"] });

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            var admins = cut.Find("[data-lt-implicit-members]");
            Assert.That(admins.GetAttribute("data-lt-implicit-members"), Is.EqualTo("ready"));
            Assert.That(
                admins.QuerySelectorAll("[data-lt-admin]").Select(item => item.TextContent.Trim()),
                Is.EqualTo(new[] { "ann@example.com", "t/acme/ops", "t/globex/ops", "zed@example.com" }));
            Assert.That(
                admins.QuerySelectorAll("[data-lt-admin] a").Select(link => link.GetAttribute("href")),
                Has.Exactly(1).Items.And.All.EndsWith("t/acme/access/groups/ops"),
                "only this tenant's own group links to its page");
            Assert.That(admins.QuerySelectorAll("button"), Is.Empty, "the administrators are read-only here");
            Assert.That(admins.QuerySelector("a")!.GetAttribute("href"), Does.EndWith(TenancyRoutes.TenantMembers("acme").Format().TrimStart('/')));
        });
    }

    [Test]
    public void Administrators_that_cannot_be_read_are_said_to_be_unlisted()
    {
        TenantFacades.AsTenantAdmin();
        TenantAccessAdmin.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantAdminSubjectReport>(new Orleans.Lattice.LatticeAuthorizationDeniedException("no")));

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            var admins = cut.Find("[data-lt-implicit-members]");
            Assert.That(admins.GetAttribute("data-lt-implicit-members"), Is.EqualTo("unavailable"));
            Assert.That(admins.TextContent, Does.Contain("could not be listed"));
        });
    }

    [Test]
    public void With_the_feature_off_the_member_set_is_not_reachable()
    {
        TenantFacades.AsTenantAdmin();
        TenantFacades.Gate.Enabled = false;

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-access]").GetAttribute("data-lt-tenant-access"), Is.EqualTo("off"));
            Assert.That(cut.FindAll("[data-lt-tenant-view]"), Is.Empty);
            Assert.That(TenantFacades.Gate.Calls, Does.Not.Contain(nameof(ILatticeTenantDirectoryAdmin.ListMembersAsync)));
        });
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void An_operator_and_a_tenant_admin_both_administer_the_member_set(bool operatorCaller)
    {
        if (operatorCaller)
        {
            TenantFacades.AsOperator();
        }
        else
        {
            TenantFacades.AsTenantAdmin();
        }

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Add member").HasAttribute("disabled"), Is.False));
    }

    private async Task AddMemberAsync(string subject, TenantSubjectKind kind)
    {
        var enabled = TenantFacades.Gate.Enabled;
        TenantFacades.Gate.Enabled = true;
        await TenantFacades.DirectoryFake.AddMemberAsync("acme", subject, kind);
        TenantFacades.Gate.Enabled = enabled;
        TenantFacades.Gate.Calls.Clear();
    }

    private static List<string[]> Rows(IRenderedComponent<AccessMembersPage> cut) =>
        [.. cut.FindAll("[data-lt-tenant-view=members] tbody tr")
            .Select(row => row.QuerySelectorAll("th, td").Take(2).Select(cell => cell.TextContent.Trim()).ToArray())];
}
