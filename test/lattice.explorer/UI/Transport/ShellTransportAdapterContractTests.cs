using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The contract every Shell transport adapter is held to, run once per adapter
/// against the in-memory <see cref="ShellTransportPeer"/>: every facade member
/// reaches its own RPC and returns on success, carries the circuit's credential,
/// and maps a transport denial to the facade's
/// <see cref="LatticeAuthorizationDeniedException"/>.
/// </summary>
/// <typeparam name="TFacade">The facade interface the adapter implements.</typeparam>
public abstract class ShellTransportAdapterContractTests<TFacade>
    where TFacade : class
{
    /// <summary>Every member of the facade the adapter serves, with the RPC it must reach.</summary>
    /// <returns>The calls.</returns>
    internal abstract IEnumerable<ShellTransportCall<TFacade>> Calls();

    /// <summary>Answers the RPCs whose default success message the typed client cannot map.</summary>
    /// <param name="peer">The peer to script.</param>
    internal virtual void ScriptSuccess(ShellTransportPeer peer)
    {
    }

    [Test]
    public async Task Every_member_reaches_its_rpc_and_returns_on_success()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<TFacade>();
        ScriptSuccess(circuit.Peer);
        circuit.Peer.AnswerWithSuccess();

        foreach (var call in Calls())
        {
            var before = circuit.Peer.Requests.Count;

            await call.Invoke(facade, CancellationToken.None);

            var requests = circuit.Peer.Requests;
            Assert.Multiple(() =>
            {
                Assert.That(requests, Has.Count.EqualTo(before + 1), $"{call.Member} must make exactly one call");
                Assert.That(requests[^1].Path, Is.EqualTo(call.Rpc), $"{call.Member} must reach {call.Rpc}");
            });
        }
    }

    [Test]
    public void Every_member_maps_a_denial_to_the_facade_denial()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<TFacade>();
        circuit.Peer.AnswerWith(StatusCode.PermissionDenied, "not an administrator");

        Assert.Multiple(() =>
        {
            foreach (var call in Calls())
            {
                var denial = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                    () => call.Invoke(facade, CancellationToken.None),
                    call.Member);
                Assert.That(denial?.Message, Is.EqualTo("not an administrator"), call.Member);
                Assert.That(denial?.InnerException, Is.InstanceOf<RpcException>(), call.Member);
            }
        });
    }

    [Test]
    public async Task Every_member_carries_the_circuit_credential()
    {
        using var circuit = new ShellTransportCircuit();
        circuit.Authentication = LatticeCallAuthentication.Basic("operator", "secret");
        var facade = circuit.Resolve<TFacade>();
        ScriptSuccess(circuit.Peer);
        circuit.Peer.AnswerWithSuccess();

        foreach (var call in Calls())
        {
            await call.Invoke(facade, CancellationToken.None);
        }

        var expected = LatticeCallAuthentication.Basic("operator", "secret").Headers![LatticeCallAuthentication.AuthorizationHeaderName];
        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests, Is.Not.Empty);
            Assert.That(circuit.Peer.Requests.Select(request => request.Authorization), Is.All.EqualTo(expected));
            Assert.That(circuit.ChannelFactory.Created, Has.Count.EqualTo(1), "every call in a circuit shares one channel");
        });
    }

    [Test]
    public void The_adapter_is_one_instance_per_circuit()
    {
        using var first = new ShellTransportCircuit();
        using var second = new ShellTransportCircuit();

        Assert.Multiple(() =>
        {
            Assert.That(first.Resolve<TFacade>(), Is.SameAs(first.Resolve<TFacade>()));
            Assert.That(first.Resolve<TFacade>(), Is.Not.SameAs(second.Resolve<TFacade>()));
        });
    }
}
