using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The review of the installed version is its manage page: no install stepper, and a tree the
/// install already owns never reads as one it cannot own (issue #3984, finding 1).
/// </summary>
public sealed partial class AppReviewPageTests
{
    [Test]
    public void The_installed_versions_page_is_in_manage_mode_without_the_install_stepper()
    {
        var cut = RenderInstalled(AppLifecycleState.Enabled);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-steps"), Is.Empty, "no install stepper on the manage page");
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("v1.0.0 is installed"));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Does.Contain("Edit consent...").And.Contain("Disable"));
            Assert.That(cut.Find("section[aria-labelledby=lt-apps-creates]").TextContent, Does.Contain("owned by this install"));
        });
    }

    [Test]
    public void An_ownership_conflict_on_the_installed_version_never_reads_as_a_refused_install()
    {
        var cut = RenderInstalled(AppLifecycleState.Enabled, conflict: "Tree 'tasks' is owned by another install of app 'task-board' from a different publisher.");

        var creates = cut.Find("section[aria-labelledby=lt-apps-creates]");
        Assert.Multiple(() =>
        {
            Assert.That(creates.TextContent, Does.Not.Contain("install is refused"));
            Assert.That(creates.QuerySelector(".lt-apps-flag")!.TextContent, Is.EqualTo("Ownership not confirmed: activation checks it again"));
            Assert.That(cut.Markup, Does.Not.Contain("cannot be installed here"));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Does.Contain("Edit consent..."), "managing it is not blocked");
        });
    }

    [Test]
    public void Editing_the_consent_of_the_installed_version_shows_its_own_steps()
    {
        var cut = RenderInstalled(AppLifecycleState.Enabled);

        Button(cut, "Edit consent...").Click();

        cut.WaitUntil(() => Assert.That(
            cut.FindAll(".lt-apps-steps__step").Select(step => step.TextContent.Trim()),
            Is.EqualTo(new[] { "Review", "Confirm ceiling", "Record consent", "Enable" })));
    }

    [Test]
    public void A_version_that_is_not_installed_keeps_the_install_stepper_and_refuses_a_tree_it_cannot_own()
    {
        Offer(AppsTestData.TaskBoard() with { Trees = [new AppTreeDescriptor { Name = "tasks", OwnershipConflict = "owned by another app" }] });

        var cut = RenderReady(Review);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-steps__step"), Is.Not.Empty);
            Assert.That(cut.Find("section[aria-labelledby=lt-apps-creates] .lt-apps-flag").TextContent, Is.EqualTo("Cannot be owned: install is refused"));
            Assert.That(cut.Markup, Does.Contain("It cannot be installed here"));
        });
    }
}
