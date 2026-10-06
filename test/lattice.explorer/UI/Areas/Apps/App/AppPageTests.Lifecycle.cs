using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// An app page follows the app's lifecycle within one circuit (issue #3831): a page
/// reached before an install reads the app again when it is installed, every section
/// change reads it again, a read that misses a change made a moment ago is retried
/// while it settles, and an enabled app the caller cannot open yet says so rather than
/// claiming nothing is there.
/// </summary>
public sealed partial class AppPageTests
{
    private const string NothingHere = "Nothing lives at this address";

    [Test]
    public void A_page_reached_before_the_install_reads_the_app_again_when_it_is_installed_in_the_same_circuit()
    {
        var cut = RenderAt("apps/crm/overview");
        Assert.That(cut.Find("h1").TextContent, Is.EqualTo(NothingHere));

        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());
        cut.InvokeAsync(() => Services.GetRequiredService<AppsAccess>().Invalidate(Slug)).GetAwaiter().GetResult();

        cut.WaitForAssertion(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM")));
    }

    [Test]
    public async Task A_page_left_while_a_settling_read_is_on_its_way_ends_quietly()
    {
        // Issue #4011: the read resumes after the page is disposed, finds the app still
        // settling, and must not arm a retry on a disposed cancellation source.
        Services.GetRequiredService<AppsAccess>().Invalidate(Slug);
        Workspace.Gate = new TaskCompletionSource();
        Workspace.GateIgnoresCancellation = true;
        var cut = RenderAt("apps/crm/overview");
        cut.WaitForAssertion(() => Assert.That(Workspace.Described, Has.Count.EqualTo(1)));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(Workspace.Gate.SetResult);

        // The read resumes on the renderer's dispatcher after a hop through the loader;
        // a fault it raised would complete the renderer's unhandled-exception task.
        SpinWait.SpinUntil(() => Renderer.UnhandledException.IsCompleted, TimeSpan.FromSeconds(2));

        Assert.Multiple(() =>
        {
            Assert.That(Renderer.UnhandledException.IsCompleted, Is.False, () => "the circuit would end: " + Renderer.UnhandledException.Result);
            Assert.That(Time.ArmedTimers, Is.Zero, "no retry is armed for a page that is gone");
        });
    }

    [Test]
    public void A_read_that_misses_a_change_made_a_moment_ago_is_retried_while_it_settles()
    {
        Services.GetRequiredService<AppsAccess>().Invalidate(Slug);
        var cut = RenderAt("apps/crm/overview");

        cut.WaitForAssertion(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("The app is starting")));
        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());
        cut.InvokeAsync(() => Time.Advance(TimeSpan.FromSeconds(1))).GetAwaiter().GetResult();

        cut.WaitForAssertion(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM")));
    }

    [Test]
    public void A_change_that_never_arrives_settles_on_not_found_after_a_bounded_number_of_reads()
    {
        Services.GetRequiredService<AppsAccess>().Invalidate(Slug);
        var cut = RenderAt("apps/crm/overview");

        for (var attempt = 0; attempt < Orleans.Lattice.Explorer.UI.Areas.Apps.App.AppPage.SettlingRetries; attempt++)
        {
            cut.WaitForAssertion(() => Assert.That(Time.ArmedTimers, Is.GreaterThan(0)));
            cut.InvokeAsync(() => Time.Advance(TimeSpan.FromSeconds(1))).GetAwaiter().GetResult();
        }

        cut.WaitForAssertion(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo(NothingHere)));
        Assert.That(Workspace.Described, Has.Count.EqualTo(Orleans.Lattice.Explorer.UI.Areas.Apps.App.AppPage.SettlingRetries + 1));
    }

    [Test]
    public void An_enabled_app_the_caller_cannot_open_yet_says_so_instead_of_not_found()
    {
        Control.Administer(Admin(ui: true, state: AppLifecycleState.Enabled), CoveringConsent());

        var cut = RenderAt("apps/crm/window");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM is not open to you yet"));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent), Does.Contain("Retry"));
        });
    }
}
