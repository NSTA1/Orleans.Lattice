using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Abstract contract test fixture for
/// <see cref="IRemoteSnapshotTransport"/> implementations. Derived
/// classes override <see cref="CreateTransportAsync"/> to plug a
/// concrete transport in front of a sender-side
/// <see cref="ISnapshotProvider"/>, then run the inherited acceptance
/// suite which pins the cross-cluster bootstrap-transport contract:
/// metadata-then-stream is consistent under concurrent sender writes
/// (the snapshot is a point-in-time view at the returned
/// <see cref="RemoteSnapshotMetadata.AsOfHlc"/>, not a moving target).
/// <para>
/// Implementations supply a sender-side <see cref="ISnapshotProvider"/>
/// stub via <see cref="TransportFixture"/>. The contract suite drives
/// the transport's metadata RPC and stream RPC against that stub and
/// asserts (a) every snapshot entry yielded by the provider arrives
/// through the stream, (b) the returned metadata's
/// <see cref="RemoteSnapshotMetadata.TreeName"/> /
/// <see cref="RemoteSnapshotMetadata.SourceClusterId"/> /
/// <see cref="RemoteSnapshotMetadata.AsOfHlc"/> /
/// <see cref="RemoteSnapshotMetadata.CausalStableFrontier"/> match the
/// sender's <see cref="SnapshotStream"/>, (c) entries committed on the
/// sender after the metadata is captured do not leak into the stream,
/// and (d) argument-validation invariants from
/// <see cref="IRemoteSnapshotTransport"/>'s contract hold.
/// </para>
/// </summary>
public abstract class RemoteSnapshotTransportContractTests
{
    /// <summary>
    /// Wires together a concrete <see cref="IRemoteSnapshotTransport"/>
    /// implementation, the sender-side
    /// <see cref="ISnapshotProvider"/> stub it draws from, and an
    /// asynchronous disposal hook for any per-test infrastructure
    /// (servers, channels, background tasks) the implementation owns.
    /// </summary>
    /// <param name="Transport">The client-side transport handle.</param>
    /// <param name="Sender">
    /// The sender-side provider the transport routes to. Tests stage
    /// the expected snapshot on this instance before invoking the
    /// transport.
    /// </param>
    /// <param name="DisposeAsync">
    /// Tear-down hook invoked by the contract suite once a test
    /// completes. Implementations that own background sockets, gRPC
    /// channels, or hosted services dispose them here.
    /// </param>
    protected sealed record TransportFixture(
        IRemoteSnapshotTransport Transport,
        StubSenderSnapshotProvider Sender,
        Func<ValueTask> DisposeAsync);

    /// <summary>
    /// Constructs a fresh transport bound to a fresh sender-side
    /// <see cref="ISnapshotProvider"/> stub. Called once per test so
    /// each test starts from a clean transport state and per-test
    /// teardown can run.
    /// </summary>
    protected abstract Task<TransportFixture> CreateTransportAsync();

    private TransportFixture _fixture = null!;

    /// <summary>The transport instance under test for the current run.</summary>
    protected IRemoteSnapshotTransport Transport => _fixture.Transport;

    /// <summary>The sender-side stub the transport draws from for the current run.</summary>
    protected StubSenderSnapshotProvider Sender => _fixture.Sender;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = await CreateTransportAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync().ConfigureAwait(false);
        }
    }

    private const string Tree = "contract-tree";
    private const string Source = "site-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static SnapshotEntry Entry(string key, byte[] value, HybridLogicalClock ts)
        => new() { Key = key, Value = value, Timestamp = ts };

    [Test]
    public async Task GetMetadataAsync_returns_tree_and_source_unchanged()
    {
        var frontier = new VersionVector();
        frontier.Tick("site-a");
        Sender.Stage(Tree, new SnapshotStream(Tree, Hlc(500), frontier, EmptyEntries()));

        var metadata = await Transport.GetMetadataAsync(Tree, Source, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(metadata.TreeName, Is.EqualTo(Tree));
            Assert.That(metadata.SourceClusterId, Is.EqualTo(Source));
            Assert.That(metadata.AsOfHlc, Is.EqualTo(Hlc(500)));
            Assert.That(metadata.CausalStableFrontier, Is.Not.Null);
        });
    }

    [Test]
    public async Task RequestSnapshotAsync_streams_every_staged_entry()
    {
        var asOf = Hlc(100);
        var frontier = new VersionVector();
        var entries = new[]
        {
            Entry("a", new byte[] { 1 }, Hlc(10)),
            Entry("b", new byte[] { 2 }, Hlc(20)),
            Entry("c", new byte[] { 3 }, Hlc(30)),
        };
        Sender.Stage(Tree, new SnapshotStream(Tree, asOf, frontier, AsAsync(entries)));

        var collected = new List<SnapshotEntry>();
        await foreach (var entry in Transport.RequestSnapshotAsync(Tree, Source, asOf))
        {
            collected.Add(entry);
        }

        Assert.That(collected, Has.Count.EqualTo(3));
        var byKey = collected.ToDictionary(e => e.Key, e => e);
        Assert.That(byKey["a"].Value, Is.EqualTo(new byte[] { 1 }));
        Assert.That(byKey["b"].Value, Is.EqualTo(new byte[] { 2 }));
        Assert.That(byKey["c"].Value, Is.EqualTo(new byte[] { 3 }));
        Assert.That(byKey["a"].Timestamp, Is.EqualTo(Hlc(10)));
        Assert.That(byKey["b"].Timestamp, Is.EqualTo(Hlc(20)));
        Assert.That(byKey["c"].Timestamp, Is.EqualTo(Hlc(30)));
    }

    [Test]
    public async Task RequestSnapshotAsync_yields_empty_stream_when_sender_has_no_entries()
    {
        var asOf = Hlc(100);
        Sender.Stage(Tree, new SnapshotStream(Tree, asOf, new VersionVector(), EmptyEntries()));

        var collected = new List<SnapshotEntry>();
        await foreach (var entry in Transport.RequestSnapshotAsync(Tree, Source, asOf))
        {
            collected.Add(entry);
        }

        Assert.That(collected, Is.Empty);
    }

    [Test]
    public async Task Metadata_then_stream_remains_consistent_under_concurrent_sender_writes()
    {
        // The contract: a snapshot is a point-in-time view at the metadata's
        // AsOfHlc. Entries committed on the sender AFTER the metadata is
        // captured must not leak into a concurrently-running stream call.
        // This is the core acceptance criterion of the remote-snapshot
        // transport contract.
        var asOf = Hlc(100);
        var frontier = new VersionVector();
        var initial = new[]
        {
            Entry("x1", new byte[] { 1 }, Hlc(10)),
            Entry("x2", new byte[] { 2 }, Hlc(20)),
            Entry("x3", new byte[] { 3 }, Hlc(30)),
        };
        Sender.Stage(Tree, new SnapshotStream(Tree, asOf, frontier, AsAsync(initial)));

        var metadata = await Transport.GetMetadataAsync(Tree, Source, asOf);

        // Mutate the sender after metadata is captured. A correct transport
        // pinned the stream to the metadata's view; entries staged after the
        // metadata-capture point must not appear.
        Sender.AppendPostMetadata(Tree, Entry("x4-LATE", new byte[] { 4 }, Hlc(200)));

        var collected = new List<SnapshotEntry>();
        await foreach (var entry in Transport.RequestSnapshotAsync(Tree, Source, metadata.AsOfHlc))
        {
            collected.Add(entry);
        }

        Assert.That(collected.Select(e => e.Key), Is.EquivalentTo(new[] { "x1", "x2", "x3" }));
        Assert.That(collected.Any(e => e.Key == "x4-LATE"), Is.False,
            "Entries committed on the sender after the metadata cut-point must not leak into the stream.");
    }

    [Test]
    public void GetMetadataAsync_throws_when_tree_name_is_null_or_whitespace()
    {
        Assert.That(
            async () => await Transport.GetMetadataAsync(null!, Source, HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(
            async () => await Transport.GetMetadataAsync("   ", Source, HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetMetadataAsync_throws_when_source_cluster_id_is_null_or_whitespace()
    {
        Assert.That(
            async () => await Transport.GetMetadataAsync(Tree, null!, HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(
            async () => await Transport.GetMetadataAsync(Tree, "   ", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RequestSnapshotAsync_throws_when_tree_name_is_null_or_whitespace()
    {
        Sender.Stage(Tree, new SnapshotStream(Tree, Hlc(1), new VersionVector(), EmptyEntries()));

        async Task Drain(string? tree)
        {
            await foreach (var _ in Transport.RequestSnapshotAsync(tree!, Source, HybridLogicalClock.Zero))
            {
            }
        }

        Assert.That(async () => await Drain(null), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await Drain("   "), Throws.InstanceOf<ArgumentException>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task RequestSnapshotAsync_throws_when_source_cluster_id_is_null_or_whitespace()
    {
        Sender.Stage(Tree, new SnapshotStream(Tree, Hlc(1), new VersionVector(), EmptyEntries()));

        async Task Drain(string? source)
        {
            await foreach (var _ in Transport.RequestSnapshotAsync(Tree, source!, HybridLogicalClock.Zero))
            {
            }
        }

        Assert.That(async () => await Drain(null), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await Drain("   "), Throws.InstanceOf<ArgumentException>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task RequestSnapshotAsync_observes_cancellation_during_stream()
    {
        var asOf = Hlc(100);
        var frontier = new VersionVector();
        Sender.Stage(Tree, new SnapshotStream(Tree, asOf, frontier, NeverCompletingStream()));

        using var cts = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            await foreach (var _ in Transport.RequestSnapshotAsync(Tree, Source, asOf, cts.Token)
                .WithCancellation(cts.Token).ConfigureAwait(false))
            {
            }
        });

        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        Assert.That(async () => await pump, Throws.InstanceOf<OperationCanceledException>());
    }

    private static async IAsyncEnumerable<SnapshotEntry> EmptyEntries()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<SnapshotEntry> AsAsync(IEnumerable<SnapshotEntry> entries)
    {
        foreach (var entry in entries)
        {
            await Task.Yield();
            yield return entry;
        }
    }

    private static async IAsyncEnumerable<SnapshotEntry> NeverCompletingStream(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(20, cancellationToken).ConfigureAwait(false);
        }
        cancellationToken.ThrowIfCancellationRequested();
        yield break;
    }
}

/// <summary>
/// Minimal sender-side <see cref="ISnapshotProvider"/> stub used by
/// <see cref="RemoteSnapshotTransportContractTests"/> derived suites
/// to stage the snapshot a transport is expected to round-trip. Not a
/// product type; tests construct one per fixture and stage entries
/// directly.
/// </summary>
public sealed class StubSenderSnapshotProvider : ISnapshotProvider
{
    private readonly Dictionary<string, SnapshotStream> _staged = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<SnapshotEntry>> _postMetadataEntries = new(StringComparer.Ordinal);

    /// <summary>
    /// Stages the <paramref name="stream"/> the next
    /// <see cref="ExportAsync"/> call for
    /// <paramref name="treeName"/> should return.
    /// </summary>
    public void Stage(string treeName, SnapshotStream stream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentNullException.ThrowIfNull(stream);
        _staged[treeName] = stream;
        _postMetadataEntries[treeName] = new List<SnapshotEntry>();
    }

    /// <summary>
    /// Records a sender-side write committed after the metadata cut-point.
    /// Recorded but deliberately NOT folded into the staged snapshot stream:
    /// the consistency test asserts that the late entry does not leak.
    /// </summary>
    public void AppendPostMetadata(string treeName, SnapshotEntry entry)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        if (!_postMetadataEntries.TryGetValue(treeName, out var list))
        {
            list = new List<SnapshotEntry>();
            _postMetadataEntries[treeName] = list;
        }
        list.Add(entry);
    }

    /// <inheritdoc />
    public Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();
        if (!_staged.TryGetValue(treeName, out var stream))
        {
            throw new InvalidOperationException($"No snapshot staged for '{treeName}'.");
        }
        return Task.FromResult(stream);
    }
}
