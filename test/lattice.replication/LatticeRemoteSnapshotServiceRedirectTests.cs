using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5537: an export reads the one physical copy the tree resolved to when
/// it opened. When a resize or restore moves the alias during the export and
/// fences that copy, the provider throws <see cref="StaleTreeRoutingException"/>.
/// The sender must end the export with the transient
/// <see cref="LatticeSnapshotExportDeferredException"/> (gRPC <c>Unavailable</c>)
/// so the receiver's bootstrap retries with a fresh export, never fail it as a
/// permanent server fault.
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotServiceRedirectTests
{
    private const string Tree = "rsrt-tree";
    private const string Source = "site-a";

    private static readonly HybridLogicalClock Ts = new() { WallClockTicks = 10 };

    private static StaleTreeRoutingException Redirect() =>
        new(Tree, $"{Tree}-bkprestore-1", $"{Tree}/resized/2");

    private static LatticeRemoteSnapshotService CreateService(ISnapshotProvider provider) =>
        new(provider,
            new StubReplicationContext(Source, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

    private static async IAsyncEnumerable<SnapshotEntry> RedirectedAfterFirst(
        Exception fault, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        yield return new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = Ts };
        cancellationToken.ThrowIfCancellationRequested();
        throw fault;
    }

    private static ISnapshotProvider ProviderStreaming(Exception fault)
    {
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), RedirectedAfterFirst(fault))));
        return provider;
    }

    private static IEnumerable<Exception> RedirectShapes()
    {
        yield return Redirect();
        yield return new AggregateException(Redirect());
        yield return new InvalidOperationException("wrapped", Redirect());
    }

    [TestCaseSource(nameof(RedirectShapes))]
    public async Task RequestSnapshotItemsAsync_defers_transiently_when_the_tree_is_redirected_mid_stream(Exception fault)
    {
        var service = CreateService(ProviderStreaming(fault));
        var received = new List<string>();

        var thrown = Assert.ThrowsAsync<LatticeSnapshotExportDeferredException>(async () =>
        {
            await foreach (var item in service.RequestSnapshotItemsAsync(Tree, Source, HybridLogicalClock.Zero))
            {
                received.Add(item.Entry.Key);
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(received, Is.EqualTo(new[] { "k1" }), "entries read before the redirect still stream");
            Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(thrown!), Is.True,
                "the receiver's bootstrap must retry the export");
            Assert.That(thrown!.Message, Does.Contain(Tree));
        });
    }

    [Test]
    public void RequestSnapshotAsync_defers_transiently_when_the_tree_is_redirected_mid_stream()
    {
        var service = CreateService(ProviderStreaming(Redirect()));

        Assert.ThrowsAsync<LatticeSnapshotExportDeferredException>(async () =>
        {
            await foreach (var _ in service.RequestSnapshotAsync(Tree, Source, HybridLogicalClock.Zero))
            {
            }
        });
    }

    [Test]
    public void GetMetadataAsync_defers_transiently_when_the_tree_is_redirected_while_the_export_opens()
    {
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<Task<SnapshotStream>>(_ => throw Redirect());
        var service = CreateService(provider);

        Assert.ThrowsAsync<LatticeSnapshotExportDeferredException>(
            () => service.GetMetadataAsync(Tree, Source, HybridLogicalClock.Zero));
    }

    [Test]
    public void RequestSnapshotItemsAsync_still_surfaces_an_unrelated_fault_unchanged()
    {
        var service = CreateService(ProviderStreaming(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in service.RequestSnapshotItemsAsync(Tree, Source, HybridLogicalClock.Zero))
            {
            }
        });
    }
}
