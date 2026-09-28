using AngleSharp.Dom;
using Bunit;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// The header controls' two presentations: inline in the header at medium and
/// expanded widths, and folded into the header's overflow menu below the small
/// breakpoint, selected by the width band the layout cascades.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SessionFoldedPresentationTests : SessionTestContext
{
    private const string Endpoint = "https://cluster.example:443";

    [TestCase(null, false)]
    [TestCase("Compact", true)]
    [TestCase("Medium", false)]
    [TestCase("Expanded", false)]
    public void Only_the_compact_band_folds(string? band, bool folded)
    {
        Assert.That(SessionPresentation.IsFolded(Band(band)), Is.EqualTo(folded));
    }

    [TestCase(null, "Center")]
    [TestCase("Compact", "End")]
    [TestCase("Medium", "Center")]
    [TestCase("Expanded", "Center")]
    public void Session_dialogs_are_full_screen_sheets_only_when_compact(string? band, string placement)
    {
        Assert.That(SessionPresentation.DialogPlacement(Band(band)), Is.EqualTo(Enum.Parse<LtDialogPlacement>(placement)));
    }

    [TestCase(null, false)]
    [TestCase("Compact", true)]
    [TestCase("Medium", false)]
    public void The_connection_gate_opens_as_a_sheet_only_when_compact(string? band, bool sheet)
    {
        var cut = RenderAt<ConnectionDialog>(Band(band));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-dialog--sheet.lt-dialog--end[role=dialog]"), Has.Count.EqualTo(sheet ? 1 : 0));
            Assert.That(cut.FindAll(".lt-dialog-layer--sheet"), Has.Count.EqualTo(sheet ? 1 : 0));
        });
    }

    [TestCase(null, false)]
    [TestCase("Compact", true)]
    [TestCase("Expanded", false)]
    public void Sign_in_opens_as_a_sheet_only_when_compact(string? band, bool sheet)
    {
        var cut = RenderAt<SignInDialog>(Band(band));

        Assert.That(cut.FindAll(".lt-dialog--sheet.lt-dialog--end[role=dialog]"), Has.Count.EqualTo(sheet ? 1 : 0));
    }

    [TestCase(null, false)]
    [TestCase("Compact", true)]
    [TestCase("Medium", false)]
    public void Re_authentication_opens_as_a_sheet_only_when_compact(string? band, bool sheet)
    {
        var cut = RenderAt<ReauthInterstitial>(Band(band));

        Assert.That(cut.FindAll(".lt-dialog--sheet.lt-dialog--end[role=alertdialog]"), Has.Count.EqualTo(sheet ? 1 : 0));
    }

    [Test]
    public void The_overlay_hands_the_width_band_to_the_first_run_gate()
    {
        var cut = RenderAt<SessionOverlay>(LtBreakpoint.Compact);

        Assert.That(cut.FindAll(".lt-dialog--sheet[role=dialog]"), Has.Count.EqualTo(1));
    }

    [TestCase("Medium")]
    [TestCase("Expanded")]
    public void At_medium_and_up_the_identity_menu_is_a_chip_that_opens_a_dialog(string band)
    {
        Auth.SignIn("alice");

        var cut = RenderAt<IdentityMenu>(Band(band));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button").GetAttribute("aria-haspopup"), Is.EqualTo("dialog"));
            Assert.That(cut.FindAll(".lt-dl"), Is.Empty, "the details wait behind the chip");
        });
    }

    [Test]
    public void Folded_the_identity_menu_shows_its_details_and_actions_in_place_with_no_dialog_of_its_own()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        Auth.SignIn("alice");

        var cut = RenderAt<IdentityMenu>(LtBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("section").GetAttribute("aria-label"), Is.EqualTo("Your session"));
            Assert.That(cut.FindAll("[aria-haspopup]"), Is.Empty);
            Assert.That(cut.FindAll("[role=dialog]"), Is.Empty, "a dialog inside the overflow sheet would stack one modal on another");
            Assert.That(cut.FindAll(".lt-dl__term").Select(term => term.TextContent), Is.EqualTo(new[] { "Signed in as", "Sign-in method", "Cluster" }));
            Assert.That(cut.FindAll("a").Single().TextContent, Is.EqualTo("Reset view"));
            Assert.That(cut.Find("form button[type=submit]").TextContent, Is.EqualTo("Sign out"));
        });
    }

    [Test]
    public void Folded_an_anonymous_caller_is_still_offered_sign_in()
    {
        var cut = RenderAt<IdentityMenu>(LtBreakpoint.Compact);

        cut.Find("button").Click();

        Assert.That(State.Overlay, Is.EqualTo(SessionOverlayKind.SignIn));
    }

    [Test]
    public void Folded_an_in_circuit_sign_out_works_in_place()
    {
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });
        Auth.SignIn("alice");
        var cut = RenderAt<IdentityMenu>(LtBreakpoint.Compact);

        cut.FindAll("button").Single(button => button.TextContent == "Sign out").Click();

        Assert.That(Auth.SignOuts, Is.EqualTo(1));
    }

    [Test]
    public void At_medium_and_up_the_connection_indicator_is_inline()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Connected, Endpoint, null));

        var cut = RenderAt<ConnectionIndicator>(LtBreakpoint.Medium);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("section"), Is.Empty);
            Assert.That(cut.Find("code").TextContent, Is.EqualTo(Endpoint));
        });
    }

    [Test]
    public void Folded_the_connection_indicator_stacks_its_state_endpoint_and_actions()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "Unavailable"));

        var cut = RenderAt<ConnectionIndicator>(LtBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("section").GetAttribute("aria-label"), Is.EqualTo("Connection"));
            Assert.That(cut.FindAll(".lt-dl__term").Select(term => term.TextContent), Is.EqualTo(new[] { "Connection", "Endpoint" }));
            Assert.That(cut.Find("[role=status] .lt-pill__text").TextContent, Is.EqualTo("Disconnected"));
            Assert.That(cut.Find(".lt-dl__value--mono").TextContent, Is.EqualTo(Endpoint));
            Assert.That(
                cut.FindAll(".lt-dialog__actions button").Select(button => button.TextContent),
                Is.EqualTo(new[] { "Reconnect", "Connection settings" }));
        });
    }

    [Test]
    public void Folded_the_connection_actions_still_act()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "Unauthenticated", RequiresAuthentication: true));
        var cut = RenderAt<ConnectionIndicator>(LtBreakpoint.Compact);

        cut.FindAll("button").Single(button => button.TextContent == "Sign in").Click();

        Assert.That(State.Overlay, Is.EqualTo(SessionOverlayKind.SignIn));
    }

    [Test]
    public void Every_session_control_in_every_presentation_is_a_design_system_control()
    {
        // The touch-target and density tokens reach a control only through the
        // primitives' classes, so no session control may escape them.
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "Unauthenticated", RequiresAuthentication: true));
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });

        var controls = new List<IElement>();
        foreach (LtBreakpoint? breakpoint in new LtBreakpoint?[] { null, LtBreakpoint.Compact })
        {
            controls.AddRange(Controls(RenderAt<ConnectionIndicator>(breakpoint).Nodes));
            controls.AddRange(Controls(RenderAt<IdentityMenu>(breakpoint).Nodes));
        }

        Auth.SignIn("alice");
        var menu = RenderAt<IdentityMenu>(LtBreakpoint.Medium);
        menu.Find("button").Click();
        controls.AddRange(Controls(menu.Nodes));
        controls.AddRange(Controls(RenderAt<IdentityMenu>(LtBreakpoint.Compact).Nodes));
        controls.AddRange(Controls(Render<ConnectionDialog>(parameters => parameters.Add(p => p.AllowCancel, true)).Nodes));
        controls.AddRange(Controls(Render<SignInDialog>().Nodes));
        controls.AddRange(Controls(Render<ReauthInterstitial>().Nodes));

        var escaped = controls
            .Where(control => !control.ClassList.Any(DesignControlClasses.Contains))
            .Select(control => control.OuterHtml)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(controls, Has.Count.GreaterThan(15), "the scan must reach the session controls");
            Assert.That(escaped, Is.Empty, "every session control must be an Lt primitive or carry lt-btn / lt-input");
        });
    }

    private static readonly HashSet<string> DesignControlClasses = new(StringComparer.Ordinal)
    {
        "lt-btn",
        "lt-input",
        "lt-check__box",
    };

    private static IEnumerable<IElement> Controls(INodeList nodes) =>
        nodes.OfType<IElement>()
            .SelectMany(element => new[] { element }.Concat(element.QuerySelectorAll("*")))
            .Where(element => element.TagName is "BUTTON" or "A" or "SELECT" or "TEXTAREA"
                || (element.TagName == "INPUT" && element.GetAttribute("type") != "hidden"));

    // The width band is internal, so the cases carry its name.
    private static LtBreakpoint? Band(string? name) => name is null ? null : Enum.Parse<LtBreakpoint>(name);

    private IRenderedComponent<TComponent> RenderAt<TComponent>(LtBreakpoint? breakpoint)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        breakpoint is { } band
            ? Render<TComponent>(parameters => parameters.AddCascadingValue(LtBreakpointCascade.Name, band))
            : Render<TComponent>();
}
