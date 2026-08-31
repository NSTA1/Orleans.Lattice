using System.Buffers;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests.PublicApiContract;

/// <summary>
/// Shared fixture for the gRPC package public-API contract suite. The
/// surface this package contributes is a single unified helper pair:
/// <see cref="LatticeReplicationGrpcOptions"/>,
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc"/>, and
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc"/>.
/// The fixture wires a real ASP.NET Core
/// <see cref="TestServer"/>-hosted receiver and exposes a builder for
/// sender-side service providers that target it, so every contract
/// partial walks the same composition path the production registration
/// produces.
/// </summary>
/// <remarks>
/// <para>
/// The fixture deliberately does <b>not</b> stand up an Orleans cluster.
/// Grain-level wiring is owned by the replication-core contract suite
/// (<c>PublicReplicationApiContractTests</c>); this suite's job is to
/// pin the gRPC package's own DI seams, transport options, channel
/// hardening, and wire round-trip.
/// </para>
/// <para>
/// Receiver-side auth is disabled by default so the contract assertions
/// can focus on the transport / options / wire shape. The auth surface
/// is exhaustively covered elsewhere under
/// <c>test/lattice.replication.grpc/Security</c>.
/// </para>
/// </remarks>
internal sealed class GrpcPublicApiContractFixture
{
    /// <summary>Cluster id stamped on every outbound batch.</summary>
    public const string SenderClusterId = "site-sender";

    /// <summary>Cluster id used to address the receiver in <see cref="LatticeReplicationGrpcOptions.Peers"/>.</summary>
    public const string ReceiverClusterId = "site-receiver";

    /// <summary>The receiver-side ASP.NET Core host.</summary>
    public IHost ReceiverHost { get; private set; } = null!;

    /// <summary>The receiver's <see cref="TestServer"/> handle.</summary>
    public TestServer ReceiverServer { get; private set; } = null!;

    /// <summary>
    /// Substitute applier installed in the receiver's DI graph; tests
    /// can configure its behaviour and assert on the per-batch calls it
    /// receives.
    /// </summary>
    public IReplicationApplier ReceiverApplier { get; private set; } = null!;

    /// <summary>Captured base address the receiver listens on.</summary>
    public Uri ReceiverBaseAddress => ReceiverServer.BaseAddress;

    /// <summary>
    /// Stands up the receiver-side host and configures a default
    /// <see cref="IReplicationApplier"/> that accepts every batch and
    /// reports the pointwise-maximum <see cref="HybridLogicalClock"/> as
    /// the high-water-mark.
    /// </summary>
    public async Task InitializeAsync()
    {
        ReceiverApplier = Substitute.For<IReplicationApplier>();
        ConfigureAcceptAllApplier(ReceiverApplier);

        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddRouting();
                    services.AddSingleton<IReplicationApplier>(ReceiverApplier);
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new EnvelopeSerializerEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddSingleton(Substitute.For<IGrainFactory>());
                    services.AddEnrollAllReplicationContext();
                    services.AddLatticeReplicationGrpc();
                    services.Configure<LatticeReplicationSecurityOptions>(o =>
                        o.RequireAuthentication = false);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapLatticeReplicationGrpc());
                });
            });

        ReceiverHost = await hostBuilder.StartAsync();
        ReceiverServer = ReceiverHost.GetTestServer();
    }

    /// <summary>Tears down the receiver host.</summary>
    public async Task DisposeAsync()
    {
        if (ReceiverHost is not null)
        {
            await ReceiverHost.StopAsync();
            ReceiverHost.Dispose();
        }
    }

    /// <summary>
    /// Resets the substitute applier to the default accept-all
    /// behaviour. Tests that override the applier's stub call this
    /// from their tear-down so the next test starts from a clean slate.
    /// </summary>
    public void ResetApplier()
    {
        ReceiverApplier.ClearReceivedCalls();
        ConfigureAcceptAllApplier(ReceiverApplier);
    }

    /// <summary>
    /// Builds a sender-side <see cref="IServiceProvider"/> wired with
    /// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc"/>
    /// pointing at the receiver via <see cref="ReceiverBaseAddress"/>.
    /// The supplied <paramref name="configure"/> delegate runs after the
    /// fixture's defaults so tests can override <c>RequireHttps</c>,
    /// <c>AllowPlaintextEndpoints</c>, <c>LocalClusterId</c>, or the
    /// <c>ConfigureChannel</c> callback as needed.
    /// </summary>
    public ServiceProvider BuildSenderServices(Action<LatticeReplicationGrpcOptions>? configure = null)
    {
        var handler = ReceiverServer.CreateHandler();
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddLogging();
        services.AddSingleton<IReplicationBatchEncoder>(sp =>
            new EnvelopeSerializerEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
        services.Configure<LatticeReplicationOptions>(o =>
        {
            o.ClusterId = SenderClusterId;
        });
        services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers[ReceiverClusterId] = ReceiverBaseAddress;
            opts.LocalClusterId = SenderClusterId;
            // TestServer is plaintext (http://); the package's hardened
            // default refuses non-https unless the host opts in.
            opts.AllowPlaintextEndpoints = true;
            // Inject the TestServer handler so the channel speaks to
            // the in-process receiver rather than dialing a real socket.
            opts.ConfigureChannel = (_, channelOptions) =>
            {
                channelOptions.HttpHandler = handler;
            };
            configure?.Invoke(opts);
        });
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Convenience helper that builds a minimal <see cref="ReplicationBatch"/>
    /// suitable for round-tripping through the gRPC transport. Tests
    /// supply just the entries they care about.
    /// </summary>
    public static ReplicationBatch BuildBatch(
        IReadOnlyList<WalRecord> entries,
        string treeName = "contract-tree",
        string targetClusterId = ReceiverClusterId,
        string originClusterId = SenderClusterId)
    {
        ArgumentNullException.ThrowIfNull(entries);

        return new ReplicationBatch
        {
            TargetClusterId = targetClusterId,
            TreeName = treeName,
            OriginClusterId = originClusterId,
            // The transport decodes the payload bytes into a typed
            // envelope on send; we hand it an empty payload because the
            // gRPC transport recognises an empty payload as a
            // heartbeat and constructs an empty envelope on its own.
            // Tests that need non-empty entries encode them via the
            // shared encoder below.
            Payload = ReadOnlyMemory<byte>.Empty,
        };
    }

    /// <summary>
    /// Encodes a populated <see cref="ReplicationBatchEnvelope"/> through
    /// the shared <see cref="IReplicationBatchEncoder"/> from the sender
    /// provider so payload bytes match the production wire shape.
    /// </summary>
    public static ReadOnlyMemory<byte> EncodeEnvelope(
        IServiceProvider senderServices,
        ReplicationBatchEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(senderServices);

        var encoder = senderServices.GetRequiredService<IReplicationBatchEncoder>();
        var buffer = new ArrayBufferWriter<byte>();
        encoder.Encode(envelope, buffer);
        return buffer.WrittenMemory;
    }

    private static void ConfigureAcceptAllApplier(IReplicationApplier applier)
    {
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var batch = callInfo.Arg<IReadOnlyList<WalRecord>>();
                var max = HybridLogicalClock.Zero;
                var applied = false;
                foreach (var entry in batch)
                {
                    applied = true;
                    if (entry.Timestamp.CompareTo(max) > 0)
                    {
                        max = entry.Timestamp;
                    }
                }
                return Task.FromResult(new ApplyResult { Applied = applied, HighWaterMark = max });
            });
        applier.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var entry = callInfo.Arg<WalRecord>();
                return Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = entry.Timestamp });
            });
    }

    private sealed class EnvelopeSerializerEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _serializer;

        public EnvelopeSerializerEncoder(Serializer<ReplicationBatchEnvelope> serializer)
        {
            _serializer = serializer;
        }

        public string ContentType => "application/x-orleans-lattice-replog+binary";

        public int CurrentWireVersion => 1;

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
            => _serializer.Serialize(envelope, writer);

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
            => _serializer.Deserialize(payload.Span);
    }
}
