using System.Buffers;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Lattice.Replication.Tests;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Exercises the client-side <see cref="GrpcRemoteSnapshotTransport"/>
/// RPC bodies end to end against the real sender-side
/// <see cref="LatticeRemoteSnapshotGrpcService"/> hosted on an in-memory
/// <see cref="TestServer"/>. Unlike the contract suite (which routes a
/// hand-rolled transport clone through the test channel), this fixture
/// drives the production transport itself by injecting the server's
/// HTTP handler through
/// <see cref="GrpcRemoteSnapshotTransportOptions.ConfigureChannel"/>, so
/// <c>ResolvePeerChannel</c>, <c>GetMetadataAsync</c>, and the
/// <c>RequestSnapshotAsync</c> stream drain are all covered.
/// </summary>
[TestFixture]
[Category("Integration")]
public class GrpcRemoteSnapshotTransportClientIntegrationTests
{
    private const string Source = "site-a";
    private const string Tree = "tree";

    private IHost _host = null!;
    private System.Net.Http.HttpMessageHandler _handler = null!;
    private Uri _baseAddress = null!;
    private StubSenderSnapshotProvider _sender = null!;
    private GrpcRemoteSnapshotTransport _transport = null!;

    private sealed class StubEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public StubEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _sender = new StubSenderSnapshotProvider();

        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddSingleton<ISnapshotProvider>(_sender);
                    services.AddSingleton<IReplicationApplier>(Substitute.For<IReplicationApplier>());
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new StubEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddRouting();
                    services.AddSingleton(Substitute.For<IGrainFactory>());
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
        var server = _host.GetTestServer();
        _baseAddress = server.BaseAddress;
        _handler = server.CreateHandler();
        _transport = CreateTransport();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _transport?.Dispose();
        if (_host is not null)
        {
            await _host.StopAsync();
            _host.Dispose();
        }
    }

    private GrpcRemoteSnapshotTransport CreateTransport()
    {
        var methods = _host.Services.GetRequiredService<LatticeRemoteSnapshotGrpcMethods>();

        var options = new GrpcRemoteSnapshotTransportOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = "local",
            ConfigureChannel = (_, channelOptions) => channelOptions.HttpHandler = _handler,
        };
        options.SenderEndpoints[Source] = _baseAddress;

        var monitor = Substitute.For<IOptionsMonitor<GrpcRemoteSnapshotTransportOptions>>();
        monitor.CurrentValue.Returns(options);

        var replicationMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationMonitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "local" });

        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));

        return new GrpcRemoteSnapshotTransport(methods, monitor, secrets, replicationMonitor);
    }

    private static async IAsyncEnumerable<SnapshotEntry> AsAsync(IEnumerable<SnapshotEntry> entries)
    {
        foreach (var e in entries)
        {
            yield return e;
        }
        await Task.CompletedTask;
    }

    [Test]
    public async Task GetMetadataAsync_returns_metadata_over_the_wire()
    {
        var asOf = new HybridLogicalClock { WallClockTicks = 500, Counter = 3 };
        _sender.Stage(Tree, new SnapshotStream(Tree, asOf, new VersionVector(), AsAsync(Array.Empty<SnapshotEntry>())));

        var metadata = await _transport.GetMetadataAsync(Tree, Source, HybridLogicalClock.Zero, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(metadata.TreeName, Is.EqualTo(Tree));
            Assert.That(metadata.SourceClusterId, Is.EqualTo(Source));
            Assert.That(metadata.AsOfHlc, Is.EqualTo(asOf));
        });
    }

    [Test]
    public async Task RequestSnapshotAsync_drains_each_entry_over_the_wire()
    {
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 } },
            new SnapshotEntry { Key = "b", Value = new byte[] { 2 }, Timestamp = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 } },
            new SnapshotEntry { Key = "c", Value = new byte[] { 3 }, Timestamp = new HybridLogicalClock { WallClockTicks = 3, Counter = 0 } },
        };
        _sender.Stage(Tree, new SnapshotStream(Tree, new HybridLogicalClock { WallClockTicks = 3, Counter = 0 }, new VersionVector(), AsAsync(entries)));

        var drained = new List<string>();
        await foreach (var entry in _transport.RequestSnapshotAsync(Tree, Source, HybridLogicalClock.Zero, CancellationToken.None))
        {
            drained.Add(entry.Key);
        }

        Assert.That(drained, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task RequestSnapshotAsync_yields_nothing_for_an_empty_stream_over_the_wire()
    {
        _sender.Stage(Tree, new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), AsAsync(Array.Empty<SnapshotEntry>())));

        var count = 0;
        await foreach (var _ in _transport.RequestSnapshotAsync(Tree, Source, HybridLogicalClock.Zero, CancellationToken.None))
        {
            count++;
        }

        Assert.That(count, Is.EqualTo(0));
    }
}
