using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

[TestFixture]
public class LatticeConnectionStatusTests
{
    [Test]
    public void Disconnected_HasExpectedDefaults()
    {
        var status = LatticeConnectionStatus.Disconnected;

        Assert.That(status.State, Is.EqualTo(LatticeConnectionState.Disconnected));
        Assert.That(status.Endpoint, Is.Null);
        Assert.That(status.IsDisconnected, Is.True);
        Assert.That(status.IsUsable, Is.False);
    }

    [TestCase(LatticeConnectionState.Connected, true, false)]
    [TestCase(LatticeConnectionState.Reconnecting, true, false)]
    [TestCase(LatticeConnectionState.Faulted, false, true)]
    [TestCase(LatticeConnectionState.Disconnected, false, true)]
    [TestCase(LatticeConnectionState.Connecting, false, false)]
    public void UsabilityFlags_ReflectState(LatticeConnectionState state, bool usable, bool disconnected)
    {
        var status = new LatticeConnectionStatus(state, "http://host", "msg");

        Assert.That(status.IsUsable, Is.EqualTo(usable));
        Assert.That(status.IsDisconnected, Is.EqualTo(disconnected));
    }
}

[TestFixture]
public class LatticeCallAuthenticationTests
{
    [Test]
    public void HasHeaders_FalseWhenNullOrEmpty()
    {
        Assert.That(new LatticeCallAuthentication().HasHeaders, Is.False);
        Assert.That(new LatticeCallAuthentication { Headers = new Dictionary<string, string>() }.HasHeaders, Is.False);
    }

    [Test]
    public void HasHeaders_TrueWhenPopulated()
    {
        var auth = new LatticeCallAuthentication
        {
            Headers = new Dictionary<string, string> { ["authorization"] = "Bearer t" },
        };

        Assert.That(auth.HasHeaders, Is.True);
    }
}
