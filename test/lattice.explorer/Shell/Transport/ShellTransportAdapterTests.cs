using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Transport;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>
/// The shared adapter base: it builds the typed client once over the circuit's
/// forwarding invoker, and refuses a missing channel or client factory.
/// </summary>
[TestFixture]
public sealed class ShellTransportAdapterTests
{
    [Test]
    public void The_constructor_rejects_a_missing_channel_or_factory()
    {
        using var circuit = new ShellTransportCircuit();
        var channel = circuit.Services.GetRequiredService<ShellTransportChannel>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new ProbeAdapter(null!, static (_, _) => new object()), Throws.ArgumentNullException);
            Assert.That(() => new ProbeAdapter(channel, null!), Throws.ArgumentNullException);
            Assert.That(() => new ShellAuthAdminTransport(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_client_is_built_once_over_the_circuit_invoker_and_serializer()
    {
        using var circuit = new ShellTransportCircuit();
        var channel = circuit.Services.GetRequiredService<ShellTransportChannel>();
        var builds = new List<(CallInvoker Invoker, IServiceProvider Serializer)>();

        var adapter = new ProbeAdapter(channel, (invoker, serializer) =>
        {
            builds.Add((invoker, serializer));
            return new object();
        });

        Assert.Multiple(() =>
        {
            Assert.That(builds, Has.Count.EqualTo(1));
            Assert.That(builds[0].Invoker, Is.SameAs(channel.Invoker));
            Assert.That(builds[0].Serializer, Is.SameAs(channel.SerializerServices));
            Assert.That(adapter.Built, Is.Not.Null);
            Assert.That(circuit.ChannelFactory.Created, Is.Empty, "building a client must not open a channel");
        });
    }

    private sealed class ProbeAdapter(ShellTransportChannel channel, Func<CallInvoker, IServiceProvider, object> create)
        : ShellTransportAdapter<object>(channel, create)
    {
        public object Built => Client;
    }
}
