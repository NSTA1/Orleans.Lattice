using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="RemoteSnapshotProvider"/>. The adapter
/// is the receiver-side implementation of <see cref="ISnapshotProvider"/>
/// that translates the local bootstrap state machine's
/// <c>ExportAsync</c> call into a paired metadata-then-stream call
/// against an <see cref="IRemoteSnapshotTransport"/> binding. The
/// fixture focuses on argument validation, delegation to the transport,
/// the legacy-overload guard, and cancellation.
/// </summary>
[TestFixture]
public class RemoteSnapshotProviderTests
{
    private const string Tree = "rsp-tree";
    private const string Source = "site-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static SnapshotEntry Entry(string key, byte[] value, HybridLogicalClock ts)
        => new() { Key = key, Value = value, Timestamp = ts };

    private static RemoteSnapshotProvider CreateProvider(IRemoteSnapshotTransport transport)
        => new(transport, NullLogger<RemoteSnapshotProvider>.Instance);

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
    public void Constructor_throws_when_transport_is_null()
    {
        Assert.That(
            () => new RemoteSnapshotProvider(null!, NullLogger<RemoteSnapshotProvider>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_logger_is_null()
    {
        Assert.That(
            () => new RemoteSnapshotProvider(Substitute.For<IRemoteSnapshotTransport>(), null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ExportAsync_two_arg_overload_throws_invalid_operation()
    {
        var transport = Substitute.For<IRemoteSnapshotTransport>();
        var provider = CreateProvider(transport);

        Assert.That(
            async () => await provider.ExportAsync(Tree, HybridLogicalClock.Zero),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task ExportAsync_returns_metadata_cut_point_from_transport()
    {
        var frontier = new VersionVector();
        frontier.Tick("site-a");
        var metadata = new RemoteSnapshotMetadata
        {
            TreeName = Tree,
            SourceClusterId = Source,
            AsOfHlc = Hlc(500),
            CausalStableFrontier = frontier,
        };

        var transport = Substitute.For<IRemoteSnapshotTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(metadata));
        transport.RequestSnapshotAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => EmptyEntries());

        var provider = CreateProvider(transport);
        var stream = await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(stream.TreeName, Is.EqualTo(Tree));
            Assert.That(stream.AsOfHlc, Is.EqualTo(Hlc(500)));
            Assert.That(stream.CausalStableFrontier, Is.SameAs(frontier));
        });
    }

    [Test]
    public async Task ExportAsync_forwards_from_as_of_to_transport()
    {
        var frontier = new VersionVector();
        var metadata = new RemoteSnapshotMetadata
        {
            TreeName = Tree,
            SourceClusterId = Source,
            AsOfHlc = Hlc(750),
            CausalStableFrontier = frontier,
        };

        var transport = Substitute.For<IRemoteSnapshotTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(metadata));
        transport.RequestSnapshotAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => EmptyEntries());

        var provider = CreateProvider(transport);
        var hint = Hlc(750);

        _ = await provider.ExportAsync(Tree, Source, hint);

        await transport.Received(1).GetMetadataAsync(Tree, Source, hint, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExportAsync_streams_entries_through_transport()
    {
        var frontier = new VersionVector();
        var metadata = new RemoteSnapshotMetadata
        {
            TreeName = Tree,
            SourceClusterId = Source,
            AsOfHlc = Hlc(900),
            CausalStableFrontier = frontier,
        };

        var entries = new[]
        {
            Entry("a", [0x01], Hlc(100)),
            Entry("b", [0x02], Hlc(200)),
            Entry("c", [0x03], Hlc(300)),
        };

        var transport = Substitute.For<IRemoteSnapshotTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(metadata));
        transport.RequestSnapshotAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => AsAsync(entries));

        var provider = CreateProvider(transport);
        var stream = await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero);

        var collected = new List<SnapshotEntry>();
        await foreach (var entry in stream.Entries)
        {
            collected.Add(entry);
        }

        Assert.That(collected, Has.Count.EqualTo(3));
        Assert.Multiple(() =>
        {
            Assert.That(collected[0].Key, Is.EqualTo("a"));
            Assert.That(collected[1].Key, Is.EqualTo("b"));
            Assert.That(collected[2].Key, Is.EqualTo("c"));
        });
    }

    [Test]
    public void ExportAsync_throws_when_tree_name_is_null_or_whitespace()
    {
        var provider = CreateProvider(Substitute.For<IRemoteSnapshotTransport>());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await provider.ExportAsync(null!, Source, HybridLogicalClock.Zero),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                async () => await provider.ExportAsync(string.Empty, Source, HybridLogicalClock.Zero),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                async () => await provider.ExportAsync("   ", Source, HybridLogicalClock.Zero),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void ExportAsync_throws_when_source_cluster_id_is_null_or_whitespace()
    {
        var provider = CreateProvider(Substitute.For<IRemoteSnapshotTransport>());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await provider.ExportAsync(Tree, null!, HybridLogicalClock.Zero),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                async () => await provider.ExportAsync(Tree, string.Empty, HybridLogicalClock.Zero),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                async () => await provider.ExportAsync(Tree, "   ", HybridLogicalClock.Zero),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void ExportAsync_propagates_cancellation_before_calling_transport()
    {
        var transport = Substitute.For<IRemoteSnapshotTransport>();
        var provider = CreateProvider(transport);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ExportAsync_propagates_transport_metadata_failure()
    {
        var transport = Substitute.For<IRemoteSnapshotTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<Task<RemoteSnapshotMetadata>>(_ => throw new InvalidOperationException("boom"));

        var provider = CreateProvider(transport);

        Assert.That(
            async () => await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("boom"));
    }

    [Test]
    public async Task ExportAsync_stream_propagates_cancellation_during_drain()
    {
        var frontier = new VersionVector();
        var metadata = new RemoteSnapshotMetadata
        {
            TreeName = Tree,
            SourceClusterId = Source,
            AsOfHlc = Hlc(1),
            CausalStableFrontier = frontier,
        };

        var transport = Substitute.For<IRemoteSnapshotTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(metadata));
        transport.RequestSnapshotAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call => NeverCompleting(call.Arg<CancellationToken>()));

        var provider = CreateProvider(transport);
        var stream = await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        Assert.That(
            async () =>
            {
                await foreach (var _ in stream.Entries.WithCancellation(cts.Token))
                {
                }
            },
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Default_interface_overload_delegates_to_two_arg_overload_with_validated_source_id()
    {
        // A concrete implementation that only overrides the two-arg
        // overload (mirroring the v1 LatticeSnapshotProvider shape)
        // must still receive a call through the default interface
        // implementation when the bootstrap coordinator invokes the
        // three-arg overload - the additive overload is non-breaking
        // for existing intra-cluster providers.
        var frontier = new VersionVector();
        var stream = new SnapshotStream(Tree, Hlc(42), frontier, EmptyEntries());
        var provider = new LegacyTwoArgProvider(stream);

        // Cast to the interface so we resolve the default interface
        // method, not the concrete class method.
        ISnapshotProvider asInterface = provider;
        var result = await asInterface.ExportAsync(Tree, Source, Hlc(42));

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(stream));
            Assert.That(provider.LastTreeName, Is.EqualTo(Tree));
            Assert.That(provider.LastAsOfHlc, Is.EqualTo(Hlc(42)));
        });

        // And the default impl still validates sourceClusterId before
        // delegating, so an intra-cluster provider inherits the guard
        // without writing it locally.
        Assert.That(
            async () => await asInterface.ExportAsync(Tree, null!, Hlc(42)),
            Throws.InstanceOf<ArgumentException>());
    }

    private sealed class LegacyTwoArgProvider(SnapshotStream stream) : ISnapshotProvider
    {
        public string? LastTreeName { get; private set; }
        public HybridLogicalClock LastAsOfHlc { get; private set; }

        public Task<SnapshotStream> ExportAsync(
            string treeName,
            HybridLogicalClock asOfHlc,
            CancellationToken cancellationToken = default)
        {
            LastTreeName = treeName;
            LastAsOfHlc = asOfHlc;
            return Task.FromResult(stream);
        }
    }
}
