using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeDataApi"/> transport adapter (#5367): every member,
/// the typed-CRDT ones included, reaches its RPC, maps a denial, carries the circuit
/// credential and is one instance per circuit.
/// </summary>
[TestFixture]
public sealed class ShellDataTransportTests : ShellTransportAdapterContractTests<ILatticeDataApi>
{
    private const string Service = "/orleans.lattice.api.data/";

    private static readonly byte[] Value = [1, 2, 3];

    internal override IEnumerable<ShellTransportCall<ILatticeDataApi>> Calls() =>
    [
        new("SetAsync", Service + "Set", (f, ct) => f.SetAsync("orders", "k", Value, ct)),
        new("DeleteAsync", Service + "Delete", (f, ct) => f.DeleteAsync("orders", "k", ct)),
        new("DeleteRangeAsync", Service + "DeleteRange", (f, ct) => f.DeleteRangeAsync(new DataRangeDeleteRequest { TreeId = "orders", StartInclusive = "a", EndExclusive = "b" }, ct)),
        new("SetManyAtomicAsync", Service + "SetManyAtomic", (f, ct) => f.SetManyAtomicAsync("orders", new DataAtomicBatch(), "op", ct)),
        new("SetManyAtomicCrossTreeAsync", Service + "SetManyAtomicCrossTree", (f, ct) => f.SetManyAtomicCrossTreeAsync([], "op", ct)),
        new("SetManyAsync", Service + "SetMany", (f, ct) => f.SetManyAsync("orders", [], ct)),
        new("GetAsync", Service + "Get", (f, ct) => f.GetAsync("orders", "k", ct)),
        new("ReadRangeAsync", Service + "ReadRange", (f, ct) => f.ReadRangeAsync(new DataRangeRequest { TreeId = "orders" }, ct)),
        new("ProbeCapabilitiesAsync", Service + "ProbeCapabilities", (f, ct) => f.ProbeCapabilitiesAsync("orders", "k", ct)),
        new("CounterIncrementAsync", Service + "CrdtWrite", (f, ct) => f.CounterIncrementAsync("orders", "k", "r", 1, ct)),
        new("CounterDecrementAsync", Service + "CrdtWrite", (f, ct) => f.CounterDecrementAsync("orders", "k", "r", 1, ct)),
        new("CounterGetAsync", Service + "CrdtRead", (f, ct) => f.CounterGetAsync("orders", "k", ct)),
        new("SetAddAsync", Service + "CrdtWrite", (f, ct) => f.SetAddAsync("orders", "k", Value, "r", ct)),
        new("SetRemoveAsync", Service + "CrdtWrite", (f, ct) => f.SetRemoveAsync("orders", "k", Value, ct)),
        new("SetGetAsync", Service + "CrdtRead", (f, ct) => f.SetGetAsync("orders", "k", ct)),
        new("OrFlagEnableAsync", Service + "CrdtWrite", (f, ct) => f.OrFlagEnableAsync("orders", "k", "r", ct)),
        new("OrFlagDisableAsync", Service + "CrdtWrite", (f, ct) => f.OrFlagDisableAsync("orders", "k", ct)),
        new("OrFlagGetAsync", Service + "CrdtRead", (f, ct) => f.OrFlagGetAsync("orders", "k", ct)),
        new("RwFlagEnableAsync", Service + "CrdtWrite", (f, ct) => f.RwFlagEnableAsync("orders", "k", "r", ct)),
        new("RwFlagDisableAsync", Service + "CrdtWrite", (f, ct) => f.RwFlagDisableAsync("orders", "k", "r", ct)),
        new("RwFlagGetAsync", Service + "CrdtRead", (f, ct) => f.RwFlagGetAsync("orders", "k", ct)),
        new("GCounterIncrementAsync", Service + "CrdtWrite", (f, ct) => f.GCounterIncrementAsync("orders", "k", "r", 1, ct)),
        new("GCounterGetAsync", Service + "CrdtRead", (f, ct) => f.GCounterGetAsync("orders", "k", ct)),
        new("GSetAddAsync", Service + "CrdtWrite", (f, ct) => f.GSetAddAsync("orders", "k", Value, ct)),
        new("GSetGetAsync", Service + "CrdtRead", (f, ct) => f.GSetGetAsync("orders", "k", ct)),
        new("RwSetAddAsync", Service + "CrdtWrite", (f, ct) => f.RwSetAddAsync("orders", "k", Value, "r", ct)),
        new("RwSetRemoveAsync", Service + "CrdtWrite", (f, ct) => f.RwSetRemoveAsync("orders", "k", Value, "r", ct)),
        new("RwSetGetAsync", Service + "CrdtRead", (f, ct) => f.RwSetGetAsync("orders", "k", ct)),
        new("VersionVectorTickAsync", Service + "CrdtWrite", (f, ct) => f.VersionVectorTickAsync("orders", "k", "r", ct)),
        new("VersionVectorGetAsync", Service + "CrdtRead", (f, ct) => f.VersionVectorGetAsync("orders", "k", ct)),
        new("RegisterSetAsync", Service + "CrdtWrite", (f, ct) => f.RegisterSetAsync("orders", "k", "r", Value, ct)),
        new("RegisterGetAsync", Service + "CrdtRead", (f, ct) => f.RegisterGetAsync("orders", "k", ct)),
        new("MaxRegisterSetAsync", Service + "CrdtWrite", (f, ct) => f.MaxRegisterSetAsync("orders", "k", Value, ct)),
        new("MaxRegisterGetAsync", Service + "CrdtRead", (f, ct) => f.MaxRegisterGetAsync("orders", "k", ct)),
        new("MinRegisterSetAsync", Service + "CrdtWrite", (f, ct) => f.MinRegisterSetAsync("orders", "k", Value, ct)),
        new("MinRegisterGetAsync", Service + "CrdtRead", (f, ct) => f.MinRegisterGetAsync("orders", "k", ct)),
        new("SequenceInsertAtAsync", Service + "CrdtWrite", (f, ct) => f.SequenceInsertAtAsync("orders", "k", 0, "r", Value, ct)),
        new("SequenceRemoveAtAsync", Service + "CrdtWrite", (f, ct) => f.SequenceRemoveAtAsync("orders", "k", 0, ct)),
        new("SequenceGetAsync", Service + "CrdtRead", (f, ct) => f.SequenceGetAsync("orders", "k", ct)),
        new("MapSetAsync", Service + "CrdtWrite", (f, ct) => f.MapSetAsync("orders", "k", "field", "r", Value, ct)),
        new("MapRemoveAsync", Service + "CrdtWrite", (f, ct) => f.MapRemoveAsync("orders", "k", "field", ct)),
        new("MapGetAsync", Service + "CrdtRead", (f, ct) => f.MapGetAsync("orders", "k", ct)),
    ];

    [Test]
    public void The_contract_covers_every_member_of_the_facade()
    {
        var declared = typeof(ILatticeDataApi).GetMethods().Select(method => method.Name).ToHashSet(StringComparer.Ordinal);

        Assert.That(Calls().Select(call => call.Member).ToHashSet(StringComparer.Ordinal), Is.EquivalentTo(declared));
    }

    [Test]
    public async Task A_probe_carries_its_tree_and_key_and_returns_the_clusters_answer()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeDataApi>();
        circuit.Peer.Respond(Service + "ProbeCapabilities", new LatticeDataCapabilities { TreeId = "orders", Key = "k", CanWrite = true });

        var report = await facade.ProbeCapabilitiesAsync("orders", "k");
        var request = circuit.Peer.Decode<Orleans.Lattice.Api.Data.Grpc.DataProbeRequest>(circuit.Peer.Requests.Single());

        Assert.Multiple(() =>
        {
            Assert.That(report, Is.EqualTo(new LatticeDataCapabilities { TreeId = "orders", Key = "k", CanWrite = true }));
            Assert.That(request, Is.EqualTo(new Orleans.Lattice.Api.Data.Grpc.DataProbeRequest { TreeId = "orders", Key = "k" }));
        });
    }

    [Test]
    public void A_schema_refusal_arrives_as_a_value_validation_fault()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeDataApi>();
        circuit.Peer.AnswerWith(
            Grpc.Core.StatusCode.InvalidArgument,
            "The value was rejected by the tree's schema policy.",
            new Dictionary<string, string>
            {
                ["lattice-validation-code"] = "schema-policy",
                ["lattice-validation-reason"] = "total must be at least 0",
            });

        var fault = Assert.ThrowsAsync<ShellValueValidationException>(() => facade.SetAsync("orders", "k", Value));

        Assert.That(((ILatticeValueValidationFault)fault!).Reason, Is.EqualTo("total must be at least 0"));
    }

    [Test]
    public void Writes_validate_their_arguments_without_a_call()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeDataApi>();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.SetAsync(string.Empty, "k", Value), Throws.ArgumentException);
            Assert.That(() => facade.SetAsync("orders", null!, Value), Throws.ArgumentNullException);
            Assert.That(() => facade.SetAsync("orders", "k", null!), Throws.ArgumentNullException);
            Assert.That(() => facade.DeleteAsync("orders", null!), Throws.ArgumentNullException);
            Assert.That(() => facade.ProbeCapabilitiesAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
