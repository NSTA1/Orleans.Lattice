using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Shell.Transport;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>
/// The circuit's <see cref="ShellTransportChannel"/>: it reads the endpoint and
/// sign-in from the circuit's own sessions, attaches the credential through the
/// Core connection plumbing, reuses one channel until the configuration or the
/// sign-in changes, and refuses every call once the circuit ends.
/// </summary>
[TestFixture]
public sealed class ShellTransportChannelTests
{
    private const string CurrentTenant = "/orleans.lattice.api.tenantadmin/GetCurrentTenant";

    [Test]
    public void The_constructor_rejects_every_missing_dependency()
    {
        var session = Substitute.For<IExplorerSession>();
        var auth = Substitute.For<IExplorerAuthSession>();
        var factory = new ShellGrpcChannelFactory();
        using var serializer = new ShellTransportSerializer();

        Assert.Multiple(() =>
        {
            Assert.That(() => new ShellTransportChannel(null!, auth, factory, serializer), Throws.ArgumentNullException);
            Assert.That(() => new ShellTransportChannel(session, null!, factory, serializer), Throws.ArgumentNullException);
            Assert.That(() => new ShellTransportChannel(session, auth, null!, serializer), Throws.ArgumentNullException);
            Assert.That(() => new ShellTransportChannel(session, auth, factory, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_invoker_is_one_forwarding_invoker_for_the_circuit()
    {
        using var circuit = new ShellTransportCircuit();
        var channel = circuit.Services.GetRequiredService<ShellTransportChannel>();

        Assert.Multiple(() =>
        {
            Assert.That(channel.Invoker, Is.InstanceOf<ShellCircuitCallInvoker>());
            Assert.That(channel.Invoker, Is.SameAs(channel.Invoker));
            Assert.That(channel.SerializerServices, Is.SameAs(circuit.Services.GetRequiredService<ShellTransportSerializer>().Services));
        });
    }

    [Test]
    public void A_call_before_an_endpoint_is_configured_fails_without_building_a_channel()
    {
        using var circuit = new ShellTransportCircuit { Configuration = null };
        var self = circuit.Resolve<ILatticeTenantSelfService>();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => self.GetCurrentTenantAsync(),
                Throws.InvalidOperationException.With.Message.EqualTo(ShellTransportChannel.NotConfiguredMessage));
            Assert.That(circuit.ChannelFactory.Created, Is.Empty);
        });
    }

    [Test]
    public async Task Calls_share_one_channel_until_something_changes()
    {
        using var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();
        await self.GetCurrentTenantAsync();
        await self.ListAccessibleTenantsAsync();

        Assert.That(circuit.ChannelFactory.Created, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_signed_out_circuit_sends_no_credential()
    {
        using var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();

        Assert.That(circuit.Peer.Requests.Single().Authorization, Is.Null);
    }

    [Test]
    public async Task A_sign_in_rebuilds_the_channel_with_the_new_credential()
    {
        using var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();
        await self.GetCurrentTenantAsync();

        circuit.Authentication = LatticeCallAuthentication.Basic("alice", "pw");
        await self.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(circuit.ChannelFactory.Created, Has.Count.EqualTo(2));
            Assert.That(circuit.Peer.Requests[0].Authorization, Is.Null);
            Assert.That(circuit.Peer.Requests[1].Authorization, Does.StartWith("Basic "));
            Assert.That(circuit.ChannelFactory.Settings[1].Authentication, Is.SameAs(circuit.Authentication));
        });
    }

    [Test]
    public async Task A_token_provider_is_asked_for_a_fresh_header_on_every_call()
    {
        using var circuit = new ShellTransportCircuit();
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("Bearer one"), new ValueTask<string?>("Bearer two"));
        circuit.Authentication = LatticeCallAuthentication.Bearer(provider);
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();
        await self.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests.Select(request => request.Authorization), Is.EqualTo(new[] { "Bearer one", "Bearer two" }));
            Assert.That(circuit.ChannelFactory.Created, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_new_configuration_rebuilds_the_channel_with_its_transport_headers()
    {
        using var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();
        await self.GetCurrentTenantAsync();

        circuit.Configuration = ShellTransportCircuit.PlaintextConfiguration(
            new Dictionary<string, string> { ["x-azure-fdid"] = "origin-1" });
        await self.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(circuit.ChannelFactory.Created, Has.Count.EqualTo(2));
            Assert.That(circuit.Peer.Requests[0].Headers.ContainsKey("x-azure-fdid"), Is.False);
            Assert.That(circuit.Peer.Requests[1].Headers["x-azure-fdid"], Is.EqualTo("origin-1"));
            Assert.That(circuit.Peer.Requests[1].Path, Is.EqualTo(CurrentTenant));
        });
    }

    [Test]
    public async Task A_static_credential_is_refused_over_plaintext_that_was_not_opted_in()
    {
        using var circuit = new ShellTransportCircuit
        {
            Configuration = new ExplorerConfiguration { Endpoint = ShellTransportCircuit.Endpoint },
            Authentication = LatticeCallAuthentication.Basic("alice", "pw"),
        };
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        Assert.That(() => self.GetCurrentTenantAsync(), Throws.InvalidOperationException.With.Message.Contains("not https"));

        circuit.Configuration = ShellTransportCircuit.PlaintextConfiguration();
        await self.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests, Has.Count.EqualTo(1), "the refused call never reached the peer");
            Assert.That(circuit.Peer.Requests[0].Authorization, Does.StartWith("Basic "));
        });
    }

    [Test]
    public void Once_the_circuit_ends_every_call_is_refused()
    {
        var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        var channel = circuit.Services.GetRequiredService<ShellTransportChannel>();

        channel.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(() => self.GetCurrentTenantAsync(), Throws.InstanceOf<ObjectDisposedException>());
            Assert.That(() => channel.Dispose(), Throws.Nothing);
        });
        circuit.Dispose();
    }

    [Test]
    public void Every_call_shape_resolves_the_circuit_channel()
    {
        using var circuit = new ShellTransportCircuit { Configuration = null };
        var invoker = circuit.Services.GetRequiredService<ShellTransportChannel>().Invoker;
        var method = new Method<string, string>(MethodType.Unary, "svc", "M", Marshallers.StringMarshaller, Marshallers.StringMarshaller);

        Assert.Multiple(() =>
        {
            Assert.That(() => invoker.BlockingUnaryCall(method, null, default, "x"), Throws.InvalidOperationException);
            Assert.That(() => invoker.AsyncUnaryCall(method, null, default, "x"), Throws.InvalidOperationException);
            Assert.That(() => invoker.AsyncServerStreamingCall(method, null, default, "x"), Throws.InvalidOperationException);
            Assert.That(() => invoker.AsyncClientStreamingCall(method, null, default), Throws.InvalidOperationException);
            Assert.That(() => invoker.AsyncDuplexStreamingCall(method, null, default), Throws.InvalidOperationException);
        });
    }
}
