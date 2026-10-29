using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>Route writer growth and malformed query recovery through the public URL contract.</summary>
[TestFixture]
public sealed class ExplorerRoutePathBoundaryTests
{
    [Test]
    public void Format_multiple_buffer_growths_preserves_every_segment_and_query_pair()
    {
        var id = new string('i', 1024) + "/orders";
        var tenant = new string('t', 8192) + " corp";
        var value = new string('v', 32768) + "&value";
        var route = ExplorerRoute.Root.WithArea("tenants")
            .WithSelection("detail", id).WithSurface("data")
            .WithAllTenants(true).WithTenant(tenant)
            .WithParameter("alpha", value).WithParameter("zeta", "last");
        var expected = "/area/tenants/detail/" + Uri.EscapeDataString(id)
            + "/data?all-tenants=true&tenant=" + Uri.EscapeDataString(tenant)
            + "&alpha=" + Uri.EscapeDataString(value) + "&zeta=last";

        var address = ExplorerRoutePath.Format(route);
        var parsed = ExplorerRoutePath.Parse(address);

        Assert.Multiple(() =>
        {
            Assert.That(address, Is.EqualTo(expected));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Canonical));
            Assert.That(parsed.Route, Is.EqualTo(route));
            Assert.That(ExplorerRoutePath.Format(ExplorerRoute.Home), Is.EqualTo("/explore"));
        });
    }

    [TestCase("=ignored")]
    [TestCase("=bad%FF")]
    [TestCase("=")]
    public void Parse_empty_query_key_reports_malformed_but_preserves_valid_neighbors(string invalidPair)
    {
        var parsed = ExplorerRoutePath.Parse("/explore/trees/orders?tenant=acme&" + invalidPair + "&tab=history");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Malformed));
            Assert.That(parsed.IsUnderstood, Is.False);
            Assert.That(parsed.ShouldCanonicalize, Is.True);
            Assert.That(parsed.Route.Id, Is.EqualTo("orders"));
            Assert.That(parsed.Route.Tenant, Is.EqualTo("acme"));
            Assert.That(parsed.Route.Parameters.Count, Is.EqualTo(1));
            Assert.That(parsed.Route.Parameters.GetValueOrEmpty("tab"), Is.EqualTo("history"));
            Assert.That(ExplorerRoutePath.Format(parsed.Route), Is.EqualTo("/explore/trees/orders?tenant=acme&tab=history"));
        });
    }
}
