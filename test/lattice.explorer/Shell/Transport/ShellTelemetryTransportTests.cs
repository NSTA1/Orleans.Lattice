using Grpc.Core;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeTelemetry"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTelemetryTransportTests : ShellTransportAdapterContractTests<ILatticeTelemetry>
{
    private const string Service = "/orleans.lattice.api.telemetry/";

    private static readonly TelemetryQueryRequest Request = new() { QueryId = "shard-reads" };

    internal override IEnumerable<ShellTransportCall<ILatticeTelemetry>> Calls() =>
    [
        new("GetCatalogAsync", Service + "GetCatalog", (f, ct) => f.GetCatalogAsync(ct)),
        new("QueryAsync", Service + "Query", (f, ct) => f.QueryAsync(Request, ct)),
    ];

    [Test]
    public void An_unknown_query_maps_to_query_not_found_with_its_id()
    {
        using var circuit = new ShellTransportCircuit();
        var telemetry = circuit.Resolve<ILatticeTelemetry>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "no such query");

        var ex = Assert.ThrowsAsync<TelemetryQueryNotFoundException>(() => telemetry.QueryAsync(Request));

        Assert.That(ex!.QueryId, Is.EqualTo("shard-reads"));
    }

    [Test]
    public void A_backend_that_could_not_answer_maps_to_a_backend_fault_with_the_query_id()
    {
        using var circuit = new ShellTransportCircuit();
        var telemetry = circuit.Resolve<ILatticeTelemetry>();
        circuit.Peer.AnswerWith(StatusCode.Unavailable, "retry with backoff");

        var ex = Assert.ThrowsAsync<TelemetryBackendException>(() => telemetry.QueryAsync(Request));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.QueryId, Is.EqualTo("shard-reads"));
            Assert.That(ex.InnerException, Is.InstanceOf<RpcException>());
        });
    }

    [Test]
    public void The_catalog_takes_the_shared_fault_table()
    {
        using var circuit = new ShellTransportCircuit();
        var telemetry = circuit.Resolve<ILatticeTelemetry>();

        circuit.Peer.AnswerWith(StatusCode.NotFound);
        Assert.That(() => telemetry.GetCatalogAsync(), Throws.InstanceOf<KeyNotFoundException>());

        circuit.Peer.AnswerWith(StatusCode.Unavailable);
        Assert.That(() => telemetry.GetCatalogAsync(), Throws.InstanceOf<Orleans.Lattice.Explorer.Shell.Transport.ShellTransportException>());
    }

    [Test]
    public void A_query_outside_its_bounds_maps_to_argument_out_of_range()
    {
        using var circuit = new ShellTransportCircuit();
        var telemetry = circuit.Resolve<ILatticeTelemetry>();
        circuit.Peer.AnswerWith(StatusCode.OutOfRange, "window too wide");

        Assert.That(() => telemetry.QueryAsync(Request), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void A_null_request_is_rejected_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var telemetry = circuit.Resolve<ILatticeTelemetry>();

        Assert.Multiple(() =>
        {
            Assert.That(() => telemetry.QueryAsync(null!), Throws.ArgumentNullException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
