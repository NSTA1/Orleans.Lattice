using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// A tenant's admin subjects (add, a remove confirmed by typing the subject, the
/// last-subject invariant) and the tenant navigation row with its stylesheet.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyMembersTests : TenancyTestContext
{
    [Test]
    public void Subjects_are_listed_in_order_and_the_last_cannot_be_removed()
    {
        var cut = RenderMembers();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent.Trim()), Is.EqualTo(new[] { FakeTenancyCluster.Caller }));
            Assert.That(cut.Find("tbody button").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find(".lt-tenancy-count").TextContent, Is.EqualTo("1 admin subject"));
            Assert.That(cut.FindAll(".lt-tenancy-note").Select(note => note.TextContent), Has.Some.Contains("keeps at least one admin subject"));
        });
    }

    [Test]
    public void A_subject_is_added_and_can_then_be_removed_after_typing_its_id()
    {
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        TenancyForms.Type(cut, "Subject id", "alice");
        cut.Find("form.lt-tenancy-add").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent.Trim()), Is.EqualTo(new[] { "alice", FakeTenancyCluster.Caller }));
            Assert.That(cut.Find(".lt-tenancy-count").TextContent, Is.EqualTo("2 admin subjects"));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("alice can now administer tenant acme."));
        });

        cut.Find("button[aria-label='Remove alice']").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__name").TextContent, Is.EqualTo("alice")));
        TenancyForms.Type(cut, "Subject name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Admins, Is.EqualTo(new[] { FakeTenancyCluster.Caller }));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("alice can no longer administer tenant acme."));
        });
    }

    [Test]
    [TestCase("", "Enter the subject id.")]
    [TestCase(FakeTenancyCluster.Caller, FakeTenancyCluster.Caller + " is already an admin subject.")]
    public void A_blank_or_existing_subject_is_refused_before_anything_is_written(string subject, string error)
    {
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        TenancyForms.Type(cut, "Subject id", subject);
        cut.Find("form.lt-tenancy-add").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(TenancyForms.ErrorOf(cut, "Subject id"), Is.EqualTo(error));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.AddAdminSubjectAsync)));
        });
    }

    [Test]
    public void The_clusters_refusal_is_shown_beside_the_subject()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.AddAdminSubjectAsync), FakeTenancyCluster.Denied());
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        TenancyForms.Type(cut, "Subject id", "alice");
        cut.Find("form.lt-tenancy-add").Submit();

        cut.WaitUntil(() => Assert.That(TenancyForms.ErrorOf(cut, "Subject id"), Is.EqualTo(TenancyFailure.NotPermittedMessage)));
    }

    [Test]
    public void A_refused_remove_is_a_toast()
    {
        Cluster.Tenants["acme"].Admins.Add("alice");
        Cluster.Fail(nameof(FakeTenancyCluster.RemoveAdminSubjectAsync), new Orleans.Lattice.Api.TenantAdmin.TenantLastAdminSubjectException("acme", "alice"));
        var cut = RenderMembers();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));

        cut.Find("button[aria-label='Remove alice']").Click();
        TenancyForms.Type(cut, "Subject name", "alice");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() => Assert.That(Services.GetToasts().Last().Message, Does.StartWith("A tenant keeps at least one admin subject.")));
    }

    [Test]
    public void A_tenant_with_no_recorded_subject_says_so()
    {
        Cluster.Tenants["acme"].Admins.Clear();

        var cut = RenderMembers();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Does.StartWith("No admin subject is recorded.")));
    }

    [Test]
    public void A_refused_read_is_not_permitted_and_a_failed_one_can_be_retried()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.ListAdminSubjectsAsync), FakeTenancyCluster.Denied());
        var denied = RenderMembers();
        denied.WaitUntil(() => Assert.That(denied.Find(".lt-empty h3").TextContent, Is.EqualTo("Not permitted")));

        Cluster.Fail(nameof(FakeTenancyCluster.ListAdminSubjectsAsync), new TimeoutException());
        var failed = RenderMembers();
        failed.WaitUntil(() => Assert.That(failed.Find(".lt-empty h3").TextContent, Is.EqualTo("Admin subjects could not be read")));
        Cluster.Heal(nameof(FakeTenancyCluster.ListAdminSubjectsAsync));
        TenancyForms.Button(failed, "Try again").Click();
        failed.WaitUntil(() => Assert.That(failed.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void While_subjects_load_a_skeleton_is_shown()
    {
        var hold = Cluster.Hold(nameof(FakeTenancyCluster.ListAdminSubjectsAsync));

        var cut = RenderMembers();

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void Below_the_small_breakpoint_a_subject_opens_a_sheet_with_its_remove_action()
    {
        Cluster.Tenants["acme"].Admins.Add("alice");
        var cut = RenderMembers(LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-compact-row__secondary").Select(line => line.TextContent.Trim()), Is.EqualTo(new[] { "Admin subject", "Admin subject" })));

        cut.FindAll(".lt-table-list__open")[0].Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog .lt-dialog__actions button").TextContent, Is.EqualTo("Remove")));
    }

    [Test]
    [TestCase(false, null, "Overview")]
    [TestCase(false, "grants", "Grants")]
    [TestCase(true, null, "Overview")]
    [TestCase(true, "sharing", "Sharing")]
    public void The_navigation_marks_only_the_current_page(bool own, string? current, string expected)
    {
        var cut = Render<TenancyNav>(parameters => parameters
            .Add(nav => nav.Tenant, "acme")
            .Add(nav => nav.Own, own)
            .Add(nav => nav.Current, current));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-tenancy-nav__link").Where(link => link.GetAttribute("aria-current") == "page").Select(link => link.TextContent), Is.EqualTo(new[] { expected }));
            Assert.That(cut.Find("nav").GetAttribute("aria-label"), Is.EqualTo(own ? "Tenant acme" : "Administration of tenant acme"));
        });
    }

    [Test]
    public void The_navigation_without_a_tenant_draws_no_link_and_links_the_stylesheet()
    {
        var cut = Render<TenancyNav>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-tenancy-nav__link"), Is.Empty);
            Assert.That(cut.Find("link[rel=stylesheet]").GetAttribute("href"), Is.EqualTo(TenancyAssets.Stylesheet));
            Assert.That(File.Exists(Path.Combine(HygieneRepository.FindRepoRoot(), "src", "lattice.explorer", "UI", "wwwroot", "tenancy", "lattice-tenancy.css")), Is.True);
        });
    }

    private IRenderedComponent<TenancyMembers> RenderMembers(LtBreakpoint? band = null) =>
        RenderSection<TenancyMembers>(parameters => parameters.Add(members => members.TenantId, "acme"), band);
}
