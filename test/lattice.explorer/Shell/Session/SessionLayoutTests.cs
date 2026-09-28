using System.Reflection;
using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Layout;
using Orleans.Lattice.Explorer.Shell.Session;
using Orleans.Lattice.Explorer.Tests.Shell.Layout;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// The session chrome inside the real layout: a session modal closes the
/// layout's compact sheets before it opens, so modals never stack, and the
/// session overlay receives the width band so its dialogs open as sheets.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SessionLayoutTests : ShellLayoutTestContext
{
    private SessionChromeState State => Services.GetRequiredService<SessionChromeState>();

    [Test]
    public async Task Asking_for_sign_in_closes_the_overflow_menu_and_opens_sign_in_as_a_sheet()
    {
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        OpenMenu(cut);

        await cut.InvokeAsync(State.OpenSignIn);

        Assert.Multiple(() =>
        {
            Assert.That(Sheets(cut, "Menu"), Is.Empty, "the overflow menu closes before the session modal opens");
            Assert.That(cut.FindComponents<SignInDialog>(), Has.Count.EqualTo(1));
            Assert.That(Sheets(cut, "Sign in"), Has.Count.EqualTo(1), "sign-in opens as a full-screen sheet when compact");
        });
    }

    [Test]
    public async Task Asking_for_the_connection_settings_closes_the_directory_sheet()
    {
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Directory").Click();

        await cut.InvokeAsync(State.OpenConfiguration);

        Assert.Multiple(() =>
        {
            Assert.That(Sheets(cut, "Directory"), Is.Empty);
            Assert.That(Sheets(cut, "Connect to a cluster"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Re_authentication_raised_off_the_renderer_closes_the_overflow_menu()
    {
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        OpenMenu(cut);
        var auth = (FakeAuthSession)Services.GetRequiredService<Orleans.Lattice.Explorer.Core.Authentication.IExplorerAuthSession>();

        // Core raises the latch on a thread-pool thread, not the renderer's. The
        // layout marshals onto the renderer's dispatcher, which runs work in
        // order, so an empty dispatch after it drains everything it queued.
        await Task.Run(auth.RaiseReauthRequired);
        await cut.InvokeAsync(() => { });

        Assert.Multiple(() =>
        {
            Assert.That(Sheets(cut, "Menu"), Is.Empty);
            Assert.That(cut.FindAll(".lt-dialog--sheet[role=alertdialog]"), Has.Count.EqualTo(1), "re-authentication opens as a sheet");
        });
    }

    [Test]
    public async Task At_medium_the_session_modals_stay_centred()
    {
        var cut = RenderLayout();
        await SetBandAsync(cut, 1);

        await cut.InvokeAsync(State.OpenSignIn);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindComponents<SignInDialog>(), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-dialog--sheet"), Is.Empty);
        });
    }

    [Test]
    public void Removing_the_layout_unsubscribes_it_from_the_session()
    {
        var cut = Render<LayoutHost>(parameters => parameters.Add(host => host.Show, true));
        var subscribed = OverlayOpeningSubscribers();

        cut.Render(parameters => parameters.Add(host => host.Show, false));

        Assert.Multiple(() =>
        {
            Assert.That(subscribed, Is.EqualTo(1));
            Assert.That(OverlayOpeningSubscribers(), Is.Zero);
        });
    }

    private int OverlayOpeningSubscribers()
    {
        var field = typeof(SessionChromeState).GetField(nameof(SessionChromeState.OverlayOpening), BindingFlags.Instance | BindingFlags.NonPublic);
        return (field?.GetValue(State) as Delegate)?.GetInvocationList().Length ?? 0;
    }

    /// <summary>Hosts the layout so a test can remove it, as the renderer does when it disposes a component.</summary>
    public sealed class LayoutHost : ComponentBase
    {
        /// <summary>Whether the layout is rendered.</summary>
        [Parameter]
        public bool Show { get; set; }

        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            if (Show)
            {
                builder.OpenComponent<ShellLayout>(0);
                builder.AddComponentParameter(1, nameof(ShellLayout.Body), PageBody);
                builder.CloseComponent();
            }
        }
    }

    private static void OpenMenu(IRenderedComponent<ShellLayout> cut)
    {
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Menu").Click();
        Assert.That(Sheets(cut, "Menu"), Has.Count.EqualTo(1), "precondition: the overflow menu is open");
    }

    private static IReadOnlyList<AngleSharp.Dom.IElement> Sheets(IRenderedComponent<ShellLayout> cut, string title) =>
        cut.FindAll(".lt-dialog--sheet")
            .Where(sheet => sheet.QuerySelector(".lt-dialog__title")?.TextContent == title)
            .ToArray();
}
