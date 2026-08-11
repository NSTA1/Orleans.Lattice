namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRemoteGroupEndpointSource"/>, which
/// projects each facade group's served endpoint from the configured remote
/// options so <c>lattice_capabilities</c> can report where each group is reached.
/// Proves per-group projection, a null endpoint for an unconfigured group, and
/// the null-options guard.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRemoteGroupEndpointSourceTests
{
    [Test]
    public void Constructor_null_options_throws()
        => Assert.That(() => new LatticeApiMcpRemoteGroupEndpointSource(null!), Throws.ArgumentNullException);

    [Test]
    public void EndpointFor_projects_each_configured_group()
    {
        var source = new LatticeApiMcpRemoteGroupEndpointSource(RemoteTestSupport.Options(o =>
        {
            o.State = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://state:5001" };
            o.Data = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://data:5002" };
            o.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://auth:5003" };
            o.Backup = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://backup:5004" };
            o.Replication = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://replication:5005" };
            o.TreeAdmin = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://treeadmin:5006" };
        }));

        Assert.Multiple(() =>
        {
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.State), Is.EqualTo("https://state:5001"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Data), Is.EqualTo("https://data:5002"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Auth), Is.EqualTo("https://auth:5003"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Backup), Is.EqualTo("https://backup:5004"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Replication), Is.EqualTo("https://replication:5005"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.TreeAdmin), Is.EqualTo("https://treeadmin:5006"));
        });
    }

    [Test]
    public void EndpointFor_unconfigured_group_is_null()
    {
        var source = new LatticeApiMcpRemoteGroupEndpointSource(RemoteTestSupport.Options(o =>
            o.State = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://state:5001" }));

        Assert.Multiple(() =>
        {
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.State), Is.EqualTo("https://state:5001"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Data), Is.Null);
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Auth), Is.Null);
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Backup), Is.Null);
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Replication), Is.Null);
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.TreeAdmin), Is.Null);
        });
    }
}
