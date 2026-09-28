using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeAppCatalog"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellAppCatalogTransportTests : ShellTransportAdapterContractTests<ILatticeAppCatalog>
{
    private const string Service = "/orleans.lattice.api.apps.catalog/";

    internal override IEnumerable<ShellTransportCall<ILatticeAppCatalog>> Calls() =>
    [
        new("ListSourcesAsync", Service + "ListSources", (f, ct) => f.ListSourcesAsync(ct)),
        new("ListAvailableAsync", Service + "ListAvailable", (f, ct) => f.ListAvailableAsync(new AvailableAppQuery { Text = "crm" }, ct)),
        new("DescribeFromSourceAsync", Service + "DescribeFromSource", (f, ct) => f.DescribeFromSourceAsync("in-image", "crm", "1.0.0", ct)),
        new("GetIconAsync", Service + "GetIcon", (f, ct) => f.GetIconAsync("in-image", "crm", null, ct)),
        new("GetCapabilitiesAsync", Service + "GetCapabilities", (f, ct) => f.GetCapabilitiesAsync(ct)),
    ];

    [Test]
    public void An_unknown_source_or_app_maps_to_key_not_found()
    {
        using var circuit = new ShellTransportCircuit();
        var catalog = circuit.Resolve<ILatticeAppCatalog>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.NotFound, "The requested app or version was not found.");

        Assert.That(
            () => catalog.DescribeFromSourceAsync("in-image", "crm"),
            Throws.InstanceOf<KeyNotFoundException>().With.Message.EqualTo("The requested app or version was not found."));
    }

    [Test]
    public void An_unserved_catalogue_maps_to_not_supported()
    {
        using var circuit = new ShellTransportCircuit();
        var catalog = circuit.Resolve<ILatticeAppCatalog>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.Unimplemented);

        Assert.That(() => catalog.GetCapabilitiesAsync(), Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var catalog = circuit.Resolve<ILatticeAppCatalog>();

        Assert.Multiple(() =>
        {
            Assert.That(() => catalog.ListAvailableAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => catalog.DescribeFromSourceAsync(string.Empty, "crm"), Throws.ArgumentException);
            Assert.That(() => catalog.DescribeFromSourceAsync("in-image", string.Empty), Throws.ArgumentException);
            Assert.That(() => catalog.GetIconAsync(string.Empty, "crm"), Throws.ArgumentException);
            Assert.That(() => catalog.GetIconAsync("in-image", string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
