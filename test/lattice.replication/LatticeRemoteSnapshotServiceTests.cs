using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="LatticeRemoteSnapshotService"/>. The
/// service is the sender-side handler that translates inbound
/// <see cref="IRemoteSnapshotTransport"/> calls into invocations of
/// the local <see cref="ISnapshotProvider"/>. The contract-level
/// metadata-stream consistency is exercised by the sibling
/// <see cref="LatticeRemoteSnapshotServiceContractTests"/> fixture;
/// this fixture focuses on argument validation, delegation, and
/// cancellation.
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotServiceTests
{
    private const string Tree = "rsts-tree";
    private const string Source = "site-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static SnapshotEntry Entry(string key, byte[] value, HybridLogicalClock ts)
        => new() { Key = key, Value = value, Timestamp = ts };

    private static LatticeRemoteSnapshotService CreateService(ISnapshotProvider provider)
        => new(provider, NullLogger<LatticeRemoteSnapshotService>.Instance);

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

    private static async IAsyncEnumerable<SnapshotEntry> NeverCompleting(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(20, cancellationToken).ConfigureAwait(false);
        }
        cancellationToken.ThrowIfCancellationRequested();
        yield break;
    }

    [Test]
    public void Constructor_throws_when_provider_is_null()
    {
        Assert.That(
            () => new LatticeRemoteSnapshotService(null!, NullLogger<LatticeRemoteSnapshotService>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_logger_is_null()
    {
        Assert.That(
            () => new LatticeRemoteSnapshotService(Substitute.For<ISnapshotProvider>(), null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task GetMetadataAsync_returns_provider_cut_point_and_carries_routing_inputs()
    {
        var frontier = new VersionVector();
        frontier.Tick("site-a");
        var stream = new SnapshotStream(Tree, Hlc(500), frontier, EmptyEntries());

        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));

        var service = CreateService(provider);

        var metadata = await service.GetMetadataAsync(Tree, Source, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(metadata.TreeName, Is.EqualTo(Tree));
            Assert.That(metadata.SourceClusterId, Is.EqualTo(Source));
            Assert.That(metadata.AsOfHlc, Is.EqualTo(Hlc(500)));
            Assert.That(metadata.CausalStableFrontier, Is.SameAs(frontier));
        });
    }

    [Test]
    public async Task GetMetadataAsync_forwards_from_as_of_to_provider()
    {
        var frontier = new VersionVector();
        var stream = new SnapshotStream(Tree, Hlc(750), frontier, EmptyEntries());

        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));

        var service = CreateService(provider);
        var hint = Hlc(750);

        _ = await service.GetMetadataAsync(Tree, Source, hint);

        await provider.Received(1).ExportAsync(Tree, hint, Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetMetadataAsync_throws_when_tree_name_is_null_or_whitespace()
    {
        var service = CreateService(Substitute.For<ISnapshotProvider>());

        Assert.That(
            async () => await service.GetMetadataAsync(null!, Source, HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(
            async () => await service.GetMetadataAsync("   ", Source, HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetMetadataAsync_throws_when_source_cluster_id_is_null_or_whitespace()
    {
        var service = CreateService(Substitute.For<ISnapshotProvider>());

        Assert.That(
            async () => await service.GetMetadataAsync(Tree, null!, HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(
            async () => await service.GetMetadataAsync(Tree, "   ", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetMetadataAsync_throws_when_cancellation_already_requested()
    {
        var service = CreateService(Substitute.For<ISnapshotProvider>());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await service.GetMetadataAsync(Tree, Source, HybridLogicalClock.Zero, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task RequestSnapshotAsync_streams_provider_entries_verbatim()
    {
        var frontier = new VersionVector();
        var staged = new[]
        {
            Entry("a", new byte[] { 1 }, Hlc(10)),
            Entry("b", new byte[] { 2 }, Hlc(20)),
            Entry("c", new byte[] { 3 }, Hlc(30)),
        };
        var stream = new SnapshotStream(Tree, Hlc(100), frontier, AsAsync(staged));

        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));

        var service = CreateService(provider);

        var collected = new List<SnapshotEntry>();
        await foreach (var entry in service.RequestSnapshotAsync(Tree, Source, Hlc(100)))
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
    public async Task RequestSnapshotAsync_yields_empty_stream_when_provider_emits_no_entries()
    {
        var stream = new SnapshotStream(Tree, Hlc(100), new VersionVector(), EmptyEntries());
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));

        var service = CreateService(provider);

        var count = 0;
        await foreach (var _ in service.RequestSnapshotAsync(Tree, Source, Hlc(100)))
        {
            count++;
        }

        Assert.That(count, Is.Zero);
    }

    [Test]
    public async Task RequestSnapshotAsync_forwards_from_as_of_to_provider()
    {
        var stream = new SnapshotStream(Tree, Hlc(750), new VersionVector(), EmptyEntries());
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));

        var service = CreateService(provider);
        var hint = Hlc(750);

        await foreach (var _ in service.RequestSnapshotAsync(Tree, Source, hint))
        {
        }

        await provider.Received(1).ExportAsync(Tree, hint, Arg.Any<CancellationToken>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_tree_name_is_null_or_whitespace()
    {
        var service = CreateService(Substitute.For<ISnapshotProvider>());

        async Task Drain(string? tree)
        {
            await foreach (var _ in service.RequestSnapshotAsync(tree!, Source, HybridLogicalClock.Zero))
            {
            }
        }

        Assert.That(async () => await Drain(null), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await Drain("   "), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_source_cluster_id_is_null_or_whitespace()
    {
        var service = CreateService(Substitute.For<ISnapshotProvider>());

        async Task Drain(string? source)
        {
            await foreach (var _ in service.RequestSnapshotAsync(Tree, source!, HybridLogicalClock.Zero))
            {
            }
        }

        Assert.That(async () => await Drain(null), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await Drain("   "), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_cancellation_already_requested()
    {
        var service = CreateService(Substitute.For<ISnapshotProvider>());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        async Task Drain()
        {
            await foreach (var _ in service.RequestSnapshotAsync(Tree, Source, HybridLogicalClock.Zero, cts.Token))
            {
            }
        }

        Assert.That(async () => await Drain(), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task RequestSnapshotAsync_observes_cancellation_during_stream()
    {
        var stream = new SnapshotStream(Tree, Hlc(100), new VersionVector(), NeverCompleting());
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));

        var service = CreateService(provider);

        using var cts = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            await foreach (var _ in service.RequestSnapshotAsync(Tree, Source, Hlc(100), cts.Token)
                .WithCancellation(cts.Token).ConfigureAwait(false))
            {
            }
        });

        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        Assert.That(async () => await pump, Throws.InstanceOf<OperationCanceledException>());
    }
}
