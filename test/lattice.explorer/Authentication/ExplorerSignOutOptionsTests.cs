using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Tests the defaults of <see cref="ExplorerSignOutOptions"/>, the configuration
/// seam the core UI reads to drive a federated sign-out.
/// </summary>
[TestFixture]
public class ExplorerSignOutOptionsTests
{
    [Test]
    public void Defaults_areSafe()
    {
        var options = new ExplorerSignOutOptions();

        Assert.That(options.FederatedSignOutPath, Is.Null);
    }

    [Test]
    public void FederatedSignOutPath_roundTrips()
    {
        var options = new ExplorerSignOutOptions
        {
            FederatedSignOutPath = "/explorer-entra/signout",
        };

        Assert.That(options.FederatedSignOutPath, Is.EqualTo("/explorer-entra/signout"));
    }
}
