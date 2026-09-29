using Bunit;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Session;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The connection indicator: the endpoint and its state in words and a health
/// role, Reconnect when down, Sign in after an authentication refusal, and the
/// way into the connection settings.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ConnectionIndicatorTests : SessionTestContext
{
    private const string Endpoint = "https://cluster.example:443";

    [Test]
    public void An_unconfigured_explorer_reads_not_configured_and_offers_the_settings()
    {
        var cut = Render<ConnectionIndicator>();

        Assert.Multiple(() =>
        {
            Assert.That(PillText(cut), Is.EqualTo("Not configured"));
            Assert.That(cut.Find(".lt-pill").GetAttribute("data-lt-state"), Is.EqualTo("unknown"));
            Assert.That(cut.FindAll("code"), Is.Empty);
            Assert.That(Buttons(cut), Is.EqualTo(new[] { "Connection settings" }));
        });
    }

    [TestCase(LatticeConnectionState.Connected, false, "healthy", "Connected")]
    [TestCase(LatticeConnectionState.Connecting, false, "unknown", "Connecting")]
    [TestCase(LatticeConnectionState.Reconnecting, false, "lagging", "Reconnecting")]
    [TestCase(LatticeConnectionState.Faulted, false, "failed", "Disconnected")]
    [TestCase(LatticeConnectionState.Faulted, true, "stalled", "Sign-in required")]
    public void Each_state_is_named_in_words_beside_its_role(LatticeConnectionState state, bool requiresAuthentication, string role, string text)
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(state, Endpoint, "message", requiresAuthentication));

        var cut = Render<ConnectionIndicator>();

        Assert.Multiple(() =>
        {
            Assert.That(PillText(cut), Is.EqualTo(text));
            Assert.That(cut.Find(".lt-pill").GetAttribute("data-lt-state"), Is.EqualTo(role));
            Assert.That(cut.Find(".lt-pill").ParentElement!.GetAttribute("role"), Is.EqualTo("status"));
            Assert.That(cut.Find("code").TextContent, Is.EqualTo(Endpoint));
        });
    }

    [Test]
    public void A_status_change_re_renders_the_indicator()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        var cut = Render<ConnectionIndicator>();

        cut.InvokeAsync(() => StateConnection.Move(new LatticeConnectionStatus(LatticeConnectionState.Connected, Endpoint, null))).GetAwaiter().GetResult();

        Assert.That(PillText(cut), Is.EqualTo("Connected"));
    }

    [Test]
    public void A_healthy_connection_offers_no_reconnect()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Connected, Endpoint, null));

        var cut = Render<ConnectionIndicator>();

        Assert.That(Buttons(cut), Has.No.Member("Reconnect"));
    }

    [Test]
    public void A_faulted_connection_offers_reconnect_which_rebuilds_the_channel()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "Unavailable"));
        var cut = Render<ConnectionIndicator>();

        cut.FindAll("button").Single(button => button.TextContent == "Reconnect").Click();

        StateConnection.Connection.Received(1).ReconnectAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void An_authentication_refusal_offers_sign_in_which_opens_the_session_overlay()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "Unauthenticated", RequiresAuthentication: true));
        var cut = Render<ConnectionIndicator>();

        cut.FindAll("button").Single(button => button.TextContent == "Sign in").Click();

        Assert.That(State.Overlay, Is.EqualTo(SessionOverlayKind.SignIn));
    }

    [Test]
    public void A_signed_in_caller_refused_by_the_endpoint_is_not_offered_sign_in_again()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        Auth.SignIn("alice");
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "PermissionDenied", RequiresAuthentication: true));

        var cut = Render<ConnectionIndicator>();

        Assert.That(Buttons(cut), Has.No.Member("Sign in"));
    }

    [Test]
    public void Connection_settings_opens_the_settings_dialog()
    {
        var cut = Render<ConnectionIndicator>();

        cut.FindAll("button").Single(button => button.TextContent == "Connection settings").Click();

        Assert.That(State.Overlay, Is.EqualTo(SessionOverlayKind.Configuration));
    }

    [Test]
    public void Falling_into_a_fault_posts_the_endpoints_explanation_once()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Connected, Endpoint, null));
        var cut = Render<ConnectionIndicator>();
        var toasts = Services.GetRequiredService<LtToastService>();

        var faulted = new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "Connection refused.");
        cut.InvokeAsync(() => StateConnection.Move(faulted)).GetAwaiter().GetResult();
        cut.InvokeAsync(() => StateConnection.Move(faulted)).GetAwaiter().GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(toasts.Toasts, Has.Count.EqualTo(1));
            Assert.That(toasts.Toasts[0].Message, Is.EqualTo($"Disconnected from {Endpoint}: Connection refused."));
            Assert.That(toasts.Toasts[0].Tone, Is.EqualTo(LtToastTone.Danger));
        });
    }

    [Test]
    public void Disposing_the_indicator_unsubscribes_it()
    {
        var cut = Render<ConnectionIndicator>();
        var before = Explorer.ConfigurationSubscribers;

        cut.Instance.Dispose();

        Assert.That(Explorer.ConfigurationSubscribers, Is.EqualTo(before - 1));
    }

    private static string PillText(IRenderedComponent<ConnectionIndicator> cut) => cut.Find(".lt-pill__text").TextContent;

    private static string[] Buttons(IRenderedComponent<ConnectionIndicator> cut) =>
        cut.FindAll("button").Select(button => button.TextContent).ToArray();
}
