using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The layout at a standalone address, one its area marks as filling a browser window of
/// its own: no skip links, header, address line or spine, but every gate on the main
/// landmark, the session overlay and the toasts stay, on the same root element.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutStandaloneTests : ShellLayoutTestContext
{
    [Test]
    public void A_standalone_address_renders_the_page_without_any_chrome()
    {
        AddStandaloneArea();
        Navigation.NavigateTo("data/orders/window");

        var cut = RenderLayout();

        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-skip"), Is.Empty);
            Assert.That(cut.FindAll("header.lt-shell-header"), Is.Empty);
            Assert.That(cut.FindAll("nav[aria-label='Address']"), Is.Empty);
            Assert.That(cut.FindAll("#lt-shell-directory"), Is.Empty);
            Assert.That(cut.Find("main#lt-shell-content").GetAttribute("tabindex"), Is.EqualTo("-1"));
            Assert.That(cut.Find(".lt-shell").ClassList, Does.Contain("lt-shell--standalone").And.Contain("lt-viewport"));
        });
    }

    [Test]
    public void Any_other_address_in_the_same_area_keeps_the_chrome()
    {
        AddStandaloneArea();
        Navigation.NavigateTo("data/orders");

        var cut = RenderLayout();

        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("header.lt-shell-header"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("nav[aria-label='Address']"), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-shell").ClassList, Does.Not.Contain("lt-shell--standalone"));
        });
    }

    [Test]
    public void A_standalone_address_in_a_hidden_area_still_renders_not_found()
    {
        AddStandaloneArea(_ => ValueTask.FromResult(AreaAvailability.Hidden));
        Navigation.NavigateTo("data/orders/window");

        var cut = RenderLayout();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("#page-body"), Is.Empty);
            Assert.That(cut.Find("main h1").TextContent, Is.EqualTo("Nothing lives at this address"));
        });
        Assert.That(cut.FindAll("header.lt-shell-header"), Is.Empty, "the gate applies, and the chrome stays away");
    }

    [Test]
    public void A_standalone_address_never_renders_its_page_before_the_areas_availability_is_known()
    {
        var gate = new TaskCompletionSource<AreaAvailability>();
        AddStandaloneArea(_ => new ValueTask<AreaAvailability>(gate.Task));
        Navigation.NavigateTo("data/orders/window");

        var cut = RenderLayout();
        Assert.That(cut.FindAll("#page-body"), Is.Empty);

        gate.SetResult(AreaAvailability.Visible);

        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_standalone_address_keeps_the_session_overlay_at_the_root_but_not_the_header_slots()
    {
        AddSlotProbes();
        AddStandaloneArea();
        Navigation.NavigateTo("data/orders/window");

        var cut = RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-shell > [data-probe='overlay']"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("[data-probe='connection'], [data-probe='identity']"), Is.Empty);
        });
    }

    [Test]
    public void Leaving_a_standalone_address_draws_the_chrome_again_on_the_same_root()
    {
        AddStandaloneArea();
        Navigation.NavigateTo("data/orders/window");
        var cut = RenderLayout();
        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));

        NavigateAndRender(cut, "data/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("header.lt-shell-header"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1));
        });
        Assert.That(Module.Invocations["observeViewport"], Has.Count.EqualTo(1), "the width observer stays on the one root");
    }

    [Test]
    public void A_caller_who_is_not_signed_in_keeps_the_chrome_to_sign_in_and_loses_it_once_signed_in()
    {
        AddStandaloneArea(signedIn: false);
        Navigation.NavigateTo("data/orders/window");

        var cut = RenderLayout();

        cut.WaitUntil(() => Assert.That(cut.FindAll("header.lt-shell-header"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find(".lt-shell").ClassList, Does.Not.Contain("lt-shell--standalone"));

        cut.InvokeAsync(() => AuthSession.SignIn("dana")).GetAwaiter().GetResult();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("header.lt-shell-header"), Is.Empty);
            Assert.That(cut.Find(".lt-shell").ClassList, Does.Contain("lt-shell--standalone"));
        });
    }

    private FakeAuthSession AuthSession => (FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>();

    private void AddStandaloneArea(Func<CancellationToken, ValueTask<AreaAvailability>>? availability = null, bool signedIn = true)
    {
        var area = new FakeArea("data", "Data")
        {
            Standalone = address => address.Path.Count == 2 && address.Path[1] == "window",
        };

        if (availability is not null)
        {
            area.Availability = availability;
        }

        AddArea(area);

        if (signedIn)
        {
            AuthSession.SignIn("dana");
        }
    }
}
