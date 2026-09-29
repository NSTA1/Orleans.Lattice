using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The connection status to health-role mapping and the connection test's
/// classification: every state has words as well as a role.
/// </summary>
[TestFixture]
public sealed class SessionConnectionPresentationTests
{
    [Test]
    public void For_rejects_a_missing_status()
    {
        Assert.That(() => SessionConnectionPresentation.For(null!, isConfigured: true), Throws.ArgumentNullException);
    }

    [TestCase(LatticeConnectionState.Connected, false, true, LtStateRole.Healthy, "Connected")]
    [TestCase(LatticeConnectionState.Reconnecting, false, true, LtStateRole.Lagging, "Reconnecting")]
    [TestCase(LatticeConnectionState.Connecting, false, true, LtStateRole.Unknown, "Connecting")]
    [TestCase(LatticeConnectionState.Faulted, true, true, LtStateRole.Stalled, "Sign-in required")]
    [TestCase(LatticeConnectionState.Faulted, false, true, LtStateRole.Failed, "Disconnected")]
    [TestCase(LatticeConnectionState.Disconnected, false, false, LtStateRole.Unknown, "Not configured")]
    [TestCase(LatticeConnectionState.Disconnected, false, true, LtStateRole.Unknown, "Disconnected")]
    public void Each_state_maps_to_a_role_and_words(
        LatticeConnectionState state, bool requiresAuthentication, bool isConfigured, LtStateRole role, string text)
    {
        var status = new LatticeConnectionStatus(state, "https://x", null, requiresAuthentication);

        Assert.That(SessionConnectionPresentation.For(status, isConfigured), Is.EqualTo(new SessionConnectionPresentation(role, text)));
    }

    [Test]
    public void A_connected_probe_is_reachable()
    {
        var result = LatticeConnectionTester.Classify(new LatticeConnectionStatus(LatticeConnectionState.Connected, "https://x", null));

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(ConnectionTestOutcome.Reachable));
            Assert.That(result.Role, Is.EqualTo(LtStateRole.Healthy));
            Assert.That(result.Text, Is.EqualTo("Reachable"));
            Assert.That(result.Message, Is.Null);
        });
    }

    [Test]
    public void An_authentication_refusal_is_reachable_and_needs_a_sign_in()
    {
        var result = LatticeConnectionTester.Classify(
            new LatticeConnectionStatus(LatticeConnectionState.Faulted, "https://x", "Unauthenticated", RequiresAuthentication: true));

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(ConnectionTestOutcome.SignInRequired));
            Assert.That(result.Role, Is.EqualTo(LtStateRole.Stalled));
            Assert.That(result.Text, Is.EqualTo("Reachable - sign-in required"));
            Assert.That(result.Message, Is.EqualTo("Unauthenticated"));
        });
    }

    [TestCase(LatticeConnectionState.Faulted)]
    [TestCase(LatticeConnectionState.Reconnecting)]
    [TestCase(LatticeConnectionState.Disconnected)]
    public void Anything_else_is_unreachable_with_the_endpoints_explanation(LatticeConnectionState state)
    {
        var result = LatticeConnectionTester.Classify(new LatticeConnectionStatus(state, "https://x", "Connection refused."));

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(ConnectionTestOutcome.Unreachable));
            Assert.That(result.Role, Is.EqualTo(LtStateRole.Failed));
            Assert.That(result.Text, Is.EqualTo("Unreachable"));
            Assert.That(result.Message, Is.EqualTo("Connection refused."));
        });
    }

    [Test]
    public void Classify_and_test_reject_missing_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeConnectionTester.Classify(null!), Throws.ArgumentNullException);
            Assert.ThrowsAsync<ArgumentNullException>(() => new LatticeConnectionTester().TestAsync(null!));
        });
    }
}
