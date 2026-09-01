using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;
using System.Buffers;
using System.Collections.Concurrent;
using System.Net.Http;

namespace Orleans.Lattice.Replication.Grpc.Tests.Chaos;

/// <summary>
/// Chaos coverage of the gRPC push transport under transport-layer
/// fault injection. A real
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc(Microsoft.AspNetCore.Routing.IEndpointRouteBuilder)"/>
/// receiver is hosted on an ASP.NET Core <see cref="TestServer"/>; the
/// sender's channel is dialed through a fault-injecting
/// <see cref="DelegatingHandler"/> that throws <see cref="HttpRequestException"/>
/// at a configurable per-call probability. Drives a series of
/// <see cref="ReplicationBatch"/> sends through the production
/// <see cref="GrpcPushTransport"/> and asserts that after a bounded
/// caller-side retry budget every entry the sender attempted is
/// observed at the receiver - re-deliveries the chaos induces are
/// absorbed by the receiver's HWM dedupe and surface as no-ops, not as
/// lost entries.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class GrpcTransportChaosTests
{
    private const string SenderClusterId = "site-sender";
    private const string ReceiverClusterId = "site-receiver";
    private const string TreeName = "grpc-chaos-tree";

    private IHost _host = null!;
    private FaultInjectingHandler _faultHandler = null!;
    private ConcurrentDictionary<string, byte> _appliedKeys = null!;
    private IReplicationBatchEncoder _encoder = null!;
    private Serializer<ReplicationAck> _ackSerializer = null!;

    [SetUp]
    public async Task SetUp()
    {
        _appliedKeys = new ConcurrentDictionary<string, byte>();

        // In-test IReplicationApplier that records each observed entry
        // by key. Set-add is idempotent, so re-deliveries the chaos
        // induces collapse to a single recorded entry per key - this is
        // the receiver-side HWM dedupe behaviour the chaos test is
        // designed to validate at the wire boundary.
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var batch = callInfo.Arg<IReadOnlyList<WalRecord>>();
                var max = HybridLogicalClock.Zero;
                foreach (var e in batch)
                {
                    _appliedKeys[e.Key] = 0;
                    if (e.Timestamp.CompareTo(max) > 0) max = e.Timestamp;
                }
                return Task.FromResult(new ApplyResult { Applied = batch.Count > 0, HighWaterMark = max });
            });
        applier.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var entry = callInfo.Arg<WalRecord>();
                _appliedKeys[entry.Key] = 0;
                return Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = entry.Timestamp });
            });

        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddSingleton<IReplicationApplier>(applier);
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new SerializerBackedBatchEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddRouting();
                    services.AddSingleton(Substitute.For<IGrainFactory>());
                    services.AddEnrollAllReplicationContext();
                    services.AddLatticeReplicationGrpc();
                    services.Configure<LatticeReplicationSecurityOptions>(o => o.RequireAuthentication = false);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeReplicationGrpc());
                });
            });

        _host = await hostBuilder.StartAsync();
        _encoder = _host.Services.GetRequiredService<IReplicationBatchEncoder>();
        _ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();

        var inner = _host.GetTestServer().CreateHandler();
        _faultHandler = new FaultInjectingHandler(inner);
    }

    [TearDown]
    public async Task TearDown()
    {
        _faultHandler.Dispose();
        if (_host is not null)
        {
            await _host.StopAsync();
            _host.Dispose();
        }
    }

    [Test]
    public async Task Sender_retry_loop_under_15pct_channel_faults_delivers_every_entry()
    {
        await RunChaosShipAsync(faultProbability: 0.15);
    }

    [Test]
    public async Task Sender_retry_loop_under_30pct_channel_faults_delivers_every_entry()
    {
        await RunChaosShipAsync(faultProbability: 0.30);
    }

    private async Task RunChaosShipAsync(double faultProbability)
    {
        _faultHandler.FaultProbability = faultProbability;

        var transport = BuildTransport();

        var distinctKeys = new HashSet<string>(StringComparer.Ordinal);
        var faultsSeenBefore = _faultHandler.FaultsInjected;

        // Ship 10 batches of 8 entries each. Each batch is retried on
        // exception or negative ack, bounded to MaxAttempts so a
        // runaway channel (e.g. faultProbability = 1.0) surfaces as
        // an assertion failure rather than an infinite loop.
        for (var batchIdx = 0; batchIdx < 10; batchIdx++)
        {
            var batchEntries = new List<WalRecord>(8);
            for (var i = 0; i < 8; i++)
            {
                var key = $"k-{batchIdx:D2}-{i}";
                distinctKeys.Add(key);
                batchEntries.Add(new WalRecord
                {
                    TreeId = TreeName,
                    Op = MutationKind.Set,
                    Key = key,
                    Value = System.Text.Encoding.UTF8.GetBytes($"v-{batchIdx}-{i}"),
                    Timestamp = new HybridLogicalClock { WallClockTicks = batchIdx * 100 + i + 1, Counter = 0 },
                    OriginClusterId = SenderClusterId,
                    Mode = LatticeMergeMode.LwwRegister,
                });
            }

            var batch = new ReplicationBatch
            {
                TargetClusterId = ReceiverClusterId,
                TreeName = TreeName,
                OriginClusterId = SenderClusterId,
                Envelope = new ReplicationBatchEnvelope
                {
                    WireVersion = 1,
                    TreeName = TreeName,
                    OriginClusterId = SenderClusterId,
                    Entries = batchEntries,
                },
                Payload = ReadOnlyMemory<byte>.Empty,
            };

            const int MaxAttempts = 40;
            ReplicationAck ack = default;
            var gotAck = false;
            for (var attempt = 0; attempt < MaxAttempts; attempt++)
            {
                try
                {
                    ack = await transport.SendAsync(batch, CancellationToken.None);
                    gotAck = true;
                    if (ack.Accepted) break;
                }
                catch
                {
                    // Channel / transport fault - retry within budget.
                }
            }

            Assert.That(gotAck, Is.True, $"Batch {batchIdx}: transport never returned an ack within {MaxAttempts} attempts.");
            Assert.That(ack.Accepted, Is.True, $"Batch {batchIdx}: ack negative after {MaxAttempts} attempts.");
        }

        var faultsInjected = _faultHandler.FaultsInjected - faultsSeenBefore;
        Assert.That(faultsInjected, Is.GreaterThan(0),
            $"Test is vacuous - no faults were injected at p={faultProbability}.");

        // Every distinct key shipped must have been applied at the
        // receiver. The chaos test does NOT assert on apply-call count
        // (re-deliveries are expected); it asserts on the set of
        // applied keys, which is the convergence invariant.
        var missing = distinctKeys.Except(_appliedKeys.Keys).ToArray();
        Assert.That(missing, Is.Empty,
            $"Receiver missed {missing.Length} keys after {faultsInjected} channel faults: " +
            string.Join(",", missing.Take(20)));
    }

    private GrpcPushTransport BuildTransport()
    {
        var server = _host.GetTestServer();

        var grpcOpts = new GrpcPushTransportOptions
        {
            AllowPlaintextEndpoints = true,
            ConfigureChannel = (_, channelOptions) =>
            {
                channelOptions.HttpHandler = _faultHandler;
            },
        };
        grpcOpts.PeerEndpoints[ReceiverClusterId] = server.BaseAddress;

        var grpcMonitor = Substitute.For<IOptionsMonitor<GrpcPushTransportOptions>>();
        grpcMonitor.CurrentValue.Returns(grpcOpts);
        grpcMonitor.Get(Arg.Any<string>()).Returns(grpcOpts);

        var lro = new LatticeReplicationOptions { ClusterId = SenderClusterId };
        var lroMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        lroMonitor.CurrentValue.Returns(lro);
        lroMonitor.Get(Arg.Any<string>()).Returns(lro);

        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));
        secrets.GetAcceptedSecretsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeReplicationAcceptedSecrets>(LatticeReplicationAcceptedSecrets.Empty));
        secrets.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));

        var method = GrpcTestFactories.CreateMethod(_encoder, _ackSerializer);

        return new GrpcPushTransport(
            method, _encoder, grpcMonitor, secrets, lroMonitor);
    }

    /// <summary>
    /// <see cref="IReplicationBatchEncoder"/> backed by the Orleans
    /// serializer; mirrors the test-internal encoder
    /// <c>GrpcPushTransportIntegrationTests</c> uses.
    /// </summary>
    private sealed class SerializerBackedBatchEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public SerializerBackedBatchEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    /// <summary>
    /// <see cref="DelegatingHandler"/> that throws
    /// <see cref="HttpRequestException"/> at a configurable per-call
    /// probability, modelling a flapping network channel. Counts
    /// faults so the test can pin "test was non-vacuous".
    /// </summary>
    private sealed class FaultInjectingHandler : DelegatingHandler
    {
        private readonly Random _rng = new(98765);
        private int _faults;

        public FaultInjectingHandler(HttpMessageHandler inner) : base(inner) { }

        public double FaultProbability { get; set; }
        public int FaultsInjected => Volatile.Read(ref _faults);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            double roll;
            lock (_rng) { roll = _rng.NextDouble(); }
            if (roll < FaultProbability)
            {
                Interlocked.Increment(ref _faults);
                throw new HttpRequestException("simulated chaos channel fault");
            }
            return base.SendAsync(request, cancellationToken);
        }
    }
}
