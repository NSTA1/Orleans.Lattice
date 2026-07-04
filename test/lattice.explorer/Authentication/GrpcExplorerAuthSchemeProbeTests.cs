using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Guards on the production gRPC auth-scheme probe. The probe's happy path opens
/// a real channel (covered by end-to-end tests); these focus on the argument
/// contract and disposability without any network dependency.
/// </summary>
[TestFixture]
public class GrpcExplorerAuthSchemeProbeTests
{
    [Test]
    public void ProbeAsync_nullAddress_throws()
    {
        using var probe = new GrpcExplorerAuthSchemeProbe();
        Assert.That(async () => await probe.ProbeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ProbeAsync_whitespaceAddress_throws()
    {
        using var probe = new GrpcExplorerAuthSchemeProbe();
        Assert.That(async () => await probe.ProbeAsync("   "), Throws.ArgumentException);
    }

    [Test]
    public void Dispose_isIdempotent()
    {
        var probe = new GrpcExplorerAuthSchemeProbe();
        Assert.That(() =>
        {
            probe.Dispose();
            probe.Dispose();
        }, Throws.Nothing);
    }
}
