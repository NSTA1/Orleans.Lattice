using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The production <see cref="ShellGrpcChannelFactory"/> is a pass-through to the
/// Core channel factory, so the Shell builds its channels exactly as the state
/// connection does.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Builds one GrpcChannel in-process with no listener, call or cluster; measured at 110 ms for 2 tests, so it stays in the fast dev loop.")]
public sealed class ShellGrpcChannelFactoryTests
{
    [Test]
    public void It_builds_a_channel_to_the_configured_endpoint()
    {
        var factory = new ShellGrpcChannelFactory();

        using var channel = factory.CreateChannel(new LatticeConnectionSettings { Address = "http://localhost:1", AllowUnencryptedHttp2 = true });

        Assert.That(channel.Target, Is.EqualTo("localhost:1"));
    }

    [Test]
    public void It_rejects_missing_settings()
    {
        Assert.That(() => new ShellGrpcChannelFactory().CreateChannel(null!), Throws.ArgumentNullException);
    }
}
