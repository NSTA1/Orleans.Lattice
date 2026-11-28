using System.Collections.Concurrent;
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
        GapRecorder.Reset();
        _fixture = new InternalOriginGuardClusterFixture
        {
            UseTcpTransport = true,
            ConfigureBuilder = b => b.AddSiloBuilderConfigurator<GapRecorder.Configurator>(),
        };
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TearDown]
    public void TearDown() => RequestContext.Clear();

    private static byte[] Val(string key) => Encoding.UTF8.GetBytes(key);

    [Test]
    public async Task KnownGap_external_client_forging_live_hosted_id_keeps_reserved_capability_and_passes_internal_origin_guard()
    {
        // Any live silo's hosted id works; the secondary is used so the forged id
        // differs from the hosted client of the gateway the attacker connects to.
        var otherSilo = _cluster.Silos.First(s => !s.SiloAddress.Equals(_cluster.Primary.SiloAddress));
        var forgedId = "hosted-" + otherSilo.SiloAddress.ToParsableString();
        using var host = await StartExternalClientAsync(forgedId);
        var client = host.Services.GetRequiredService<IClusterClient>();

        // The outcome is observed on the silo, by a recording filter that runs after
        // the capability-stripping filter (GapRecorder), not from the client's reply.
        // The reply is addressed to the forged id, so Orleans may route it to that
        // silo's genuine hosted client and the attacker never sees it; the call has
        // been admitted and has run regardless, so the client outcome is ignored.
        // The internal-origin guard is the first statement of
        // ShardRootGrain.ApplyCrdtDeltaAsync, so a stripped caller is refused with
        // LatticeAuthorizationDeniedException (see the refusal tests below). Any
        // other outcome - the leaf's ArgumentException for an LWW-register delta,
        // or a first-touch activation-readiness timeout on a loaded runner - is
        // only reachable after the guard admitted the call. A missing observation
        // is retried on a fresh key.
        GapRecorder.Observation? conclusive = null;
        try
        {
            for (var attempt = 0; attempt < 3 && conclusive is null; attempt++)
            {
                var key = $"{GapRecorder.KeyPrefix}{attempt}/0";
                var shard = client.GetGrain<IShardRootGrain>(key);
                RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);
                try
                {
                    await shard.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, Val("v"))
                        .WaitAsync(TimeSpan.FromSeconds(15));
                }
                catch (Exception)
                {
                    // Client-side outcome is not evidence; see above.
                }
                finally
                {
                    RequestContext.Clear();
                }

                var observation = await GapRecorder.WaitForAsync(key, TimeSpan.FromSeconds(30));
                TestContext.Progress.WriteLine($"attempt {attempt}: {observation?.ToString() ?? "no observation"}");
                conclusive = observation;
            }
        }
        finally
        {
            await StopQuietlyAsync(host);
        }

        Assert.That(conclusive, Is.Not.Null, "no conclusive silo-side observation: " + GapRecorder.Describe());

        // KNOWN GAP (#5346): the forged external call is treated as in-silo
        // infrastructure - its seeded reserved capability survives the filter and
        // the call passes the internal-origin guard on the physical shard grain.
        // Invert these once provenance is authenticated.
        Assert.Multiple(() =>
        {
            Assert.That(conclusive!.SourceKey, Is.EqualTo(forgedId));
            Assert.That(conclusive.SystemOriginRetained, Is.True, "reserved capability was stripped");
            Assert.That(conclusive.Fault, Is.Not.InstanceOf<LatticeAuthorizationDeniedException>(), "internal-origin guard refused the call");
        });
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

    // Silo-side observer for the known-gap test. Registered after the fixture's
    // capability-stripping filter, so it sees the RequestContext the grain body
    // will see, and it records the call's outcome on the silo, independent of
    // whether the reply ever reaches the (forged) caller.
    internal sealed class GapRecorder : IIncomingGrainCallFilter
    {
        public const string KeyPrefix = "hosted-provenance-gap-";

        private static readonly ConcurrentDictionary<string, Observation> Observations = new();

        public sealed record Observation(string? SourceKey, bool SystemOriginRetained, Exception? Fault);

        public static void Reset() => Observations.Clear();

        public static string Describe() => Observations.IsEmpty
            ? "(none)"
            : string.Join(" | ", Observations.Select(o => $"{o.Key}: {o.Value.Fault?.GetType().Name ?? "ok"}"));

        public static async Task<Observation?> WaitForAsync(string key, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (Observations.TryGetValue(key, out var observation))
                {
                    return observation;
                }

                await Task.Delay(50);
            }

            return null;
        }

        public async Task Invoke(IIncomingGrainCallContext context)
        {
            var key = context.TargetId.Key.ToString();
            if (context.Grain is not IShardRootGrain || key is null || !key.StartsWith(KeyPrefix, StringComparison.Ordinal))
            {
                await context.Invoke();
                return;
            }

            var sourceKey = context.SourceId is { } source && source.IsClient() ? source.Key.ToString() : null;
            var retained = RequestContext.Get(LatticeEventConstants.AccessGateSystemOriginRequestContextKey) is not null;
            try
            {
                await context.Invoke();
                Observations[key] = new Observation(sourceKey, retained, null);
            }
            catch (Exception ex)
            {
                Observations[key] = new Observation(sourceKey, retained, ex);
                throw;
            }
        }

        public sealed class Configurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder) => siloBuilder.AddIncomingGrainCallFilter<GapRecorder>();
        }
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

    // A forged client's shutdown can stall on replies routed elsewhere; never let
    // it hold the fixture.
    private static async Task StopQuietlyAsync(IHost host)
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(15));
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine("external client stop did not complete cleanly: " + ex.GetType().Name);
        }
    }
}
