using System.Net;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Gateway-level adversarial coverage for issue #5346: can an external Orleans
/// client that announces a <c>hosted-{silo}</c> client id keep the reserved
/// capability keys that <see cref="LatticeCapabilityStrippingCallFilter"/> strips
/// from ordinary clients?
/// </summary>
/// <remarks>
/// <para>
/// <b>Verdict: exploitable. This fixture records a KNOWN GAP.</b> It asserts the
/// current behaviour so it stays green in CI, and it must be inverted when hosted
/// provenance is authenticated.
/// </para>
/// <para>
/// The gap exists because Orleans (10.4.2) does not authenticate the client id.
/// An external client generates its id locally, in the internal
/// <c>LocalClientDetails</c> singleton (<c>ClientGrainId.Create()</c>). It sends that
/// id in the connection preamble and stamps it as <c>SendingGrain</c> on every
/// message. <c>GatewayInboundConnection</c> checks only two things: the preamble id
/// parses as a client id, and the cluster id matches. It never checks a message's
/// <c>SendingGrain</c> against the preamble.
/// </para>
/// <para>
/// The client process is the attacker's, so the attacker can replace that id (here
/// by reflection; a raw-protocol client works equally well) with
/// <c>hosted-{liveSiloAddress}</c>. Live silo addresses, generation included, are
/// visible to any client through the gateway list and membership.
/// </para>
/// <para>
/// The filter's live-membership check rejects garbage and dead addresses. That
/// check is a plausibility test, not authentication: given a live address, the
/// forged caller is treated as the in-silo hosted client. The filter retains every
/// reserved key the caller seeded and stamps the internal-origin marker, so a
/// direct call to a physical shard grain is admitted.
/// </para>
/// <para>
/// The trust boundary is therefore the network: the Orleans gateway port admits
/// trusted infrastructure only, and untrusted callers use the gRPC and HTTP facades
/// (see <c>.github/instructions/security.instructions.md</c>). Authenticating hosted
/// provenance with a signed cluster trust token is tracked as follow-up work.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class HostedClientProvenanceGapIntegrationTests
{
    private InternalOriginGuardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new InternalOriginGuardClusterFixture { UseTcpTransport = true };
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TearDown]
    public void TearDown() => RequestContext.Clear();

    private const int MaxAttempts = 24;

    private static byte[] Val(string key) => Encoding.UTF8.GetBytes(key);

    [Test]
    public async Task KnownGap_external_client_forging_live_hosted_id_keeps_reserved_capability_and_reaches_leaf()
    {
        // Any live silo's hosted id works; the secondary is used so the forged id
        // differs from the hosted client of the gateway the attacker connects to.
        var otherSilo = _cluster.Silos.First(s => !s.SiloAddress.Equals(_cluster.Primary.SiloAddress));
        var forgedId = "hosted-" + otherSilo.SiloAddress.ToParsableString();
        using var host = await StartExternalClientAsync(forgedId);
        var client = host.Services.GetRequiredService<IClusterClient>();

        // Admission is proven by reaching the leaf's argument validation: the leaf
        // rejects an LWW-register CRDT delta with ArgumentException, which only an
        // admitted call can reach. A stripped caller is refused earlier with
        // LatticeAuthorizationDeniedException (see the refusal tests below).
        // The response is addressed to the forged id, so when the shard activates on
        // the forged silo that silo's genuine hosted client claims it and the caller
        // times out. A timeout proves nothing either way, so each attempt uses a
        // fresh grain key (fresh placement) until one response returns. Placement is
        // roughly even across the two silos, so the cap must make an all-timeout run
        // negligible: six attempts failed about one run in 64.
        Exception? observed = null;
        try
        {
            for (var attempt = 0; attempt < MaxAttempts && observed is null; attempt++)
            {
                var shard = client.GetGrain<IShardRootGrain>($"hosted-provenance-gap-{attempt}/0");
                RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);
                try
                {
                    await shard.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, Val("v"));
                    Assert.Fail("The leaf accepted an LWW-register CRDT delta.");
                }
                catch (TimeoutException)
                {
                }
                catch (Exception ex)
                {
                    observed = ex;
                }
                finally
                {
                    RequestContext.Clear();
                }
            }
        }
        finally
        {
            await host.StopAsync();
        }

        // KNOWN GAP (#5346): the forged external call is admitted as in-silo
        // infrastructure. Invert this assertion once provenance is authenticated.
        Assert.That(observed, Is.InstanceOf<ArgumentException>(), observed?.ToString() ?? "every attempt timed out");
    }

    [Test]
    public async Task External_client_forging_dead_hosted_id_is_stripped_and_refused()
    {
        var deadSilo = SiloAddress.New(IPAddress.Loopback, 1, 1);
        using var host = await StartExternalClientAsync("hosted-" + deadSilo.ToParsableString());
        var client = host.Services.GetRequiredService<IClusterClient>();
        var shard = client.GetGrain<IShardRootGrain>("hosted-provenance-dead/0");
        RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.SetAsync("k", Val("k")));
        await host.StopAsync();
    }

    [Test]
    public async Task External_client_with_custom_ordinary_id_is_stripped_and_refused()
    {
        using var host = await StartExternalClientAsync("plain-" + Guid.NewGuid().ToString("N"));
        var client = host.Services.GetRequiredService<IClusterClient>();
        var shard = client.GetGrain<IShardRootGrain>("hosted-provenance-plain/0");
        RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.SetAsync("k", Val("k")));
        await host.StopAsync();
    }

    [Test]
    public void External_client_with_ordinary_id_is_stripped_and_refused()
    {
        var shard = _cluster.Client.GetGrain<IShardRootGrain>("hosted-provenance-control/0");
        RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.SetAsync("k", Val("k")));
    }

    // Builds an ordinary external Orleans client against the primary gateway, then
    // overwrites the client id Orleans generated locally before the client starts.
    // Everything sent on the wire (preamble and every SendingGrain) is read lazily
    // from that singleton, so this is exactly what a hostile client process can do.
    private async Task<IHost> StartExternalClientAsync(string clientIdKey)
    {
        var gateway = _cluster.Primary.GatewayAddress.Endpoint;
        var host = Host.CreateDefaultBuilder()
            .UseOrleansClient(client =>
            {
                client.UseStaticClustering(gateway);
                client.Configure<ClientMessagingOptions>(o => o.ResponseTimeout = TimeSpan.FromSeconds(10));
                client.Configure<ClusterOptions>(o =>
                {
                    o.ClusterId = _cluster.Options.ClusterId;
                    o.ServiceId = _cluster.Options.ServiceId;
                });
            })
            .Build();

        var detailsType = typeof(IClusterClient).Assembly.GetType("Orleans.LocalClientDetails", throwOnError: true)!;
        var details = host.Services.GetRequiredService(detailsType);
        var idField = detailsType.GetField("<ClientId>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("LocalClientDetails.ClientId backing field not found; Orleans internals changed.");
        // ClientGrainId is internal to Orleans; build it through its public-in-assembly factory.
        var create = idField.FieldType.GetMethod("Create", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic, [typeof(string)])
            ?? throw new InvalidOperationException("ClientGrainId.Create(string) not found; Orleans internals changed.");
        idField.SetValue(details, create.Invoke(null, [clientIdKey]));

        try
        {
            await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(30));
        }
        catch (TimeoutException ex)
        {
            throw new InvalidOperationException("External client StartAsync timed out for id " + clientIdKey, ex);
        }

        return host;
    }
}
