using NSubstitute;

namespace Orleans.Lattice.Api.State.Tests;

public sealed partial class LatticeStateApiEdgeCaseTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void ScanEntriesAsync_still_classifies_invalid_and_expired_continuations(bool expired)
    {
        Exception fault = expired
            ? new LatticeCursorSnapshotExpiredException("pin expired")
            : new InvalidOperationException("unknown cursor");
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>("tree").Returns(tree);
        tree.NextEntriesAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<LatticeCursorEntriesPage>(fault));
        var query = CreateQuery(grainFactory: factory);

        var actual = Assert.ThrowsAsync<ArgumentException>(
            async () => await query.ScanEntriesAsync(new EntryScanRequest { TreeId = "tree", ContinuationToken = "cursor" }));
        Assert.That(actual!.InnerException, Is.SameAs(fault));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task CancelScanAsync_still_accepts_invalid_and_expired_continuations(bool expired)
    {
        Exception fault = expired
            ? new LatticeCursorSnapshotExpiredException("pin expired")
            : new InvalidOperationException("unknown cursor");
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>("tree").Returns(tree);
        tree.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromException(fault));
        var query = CreateQuery(grainFactory: factory);

        await query.CancelScanAsync("tree", "cursor");

        await tree.Received(1).CloseCursorAsync("cursor", Arg.Any<CancellationToken>());
    }

    [Test]
    public void ScanEntriesAsync_preserves_domain_refusals_instead_of_blaming_the_token()
    {
        var fault = new LeafProjectionStaleException("projection unavailable");
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>("tree").Returns(tree);
        tree.NextEntriesAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<LatticeCursorEntriesPage>(fault));
        var query = CreateQuery(grainFactory: factory);

        var actual = Assert.ThrowsAsync<LeafProjectionStaleException>(
            async () => await query.ScanEntriesAsync(new EntryScanRequest { TreeId = "tree", ContinuationToken = "cursor" }));
        Assert.That(actual, Is.SameAs(fault));
    }

    [Test]
    public void CancelScanAsync_preserves_domain_refusals_instead_of_reporting_success()
    {
        var fault = new LeafProjectionStaleException("projection unavailable");
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>("tree").Returns(tree);
        tree.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(fault));
        var query = CreateQuery(grainFactory: factory);

        var actual = Assert.ThrowsAsync<LeafProjectionStaleException>(async () => await query.CancelScanAsync("tree", "cursor"));
        Assert.That(actual, Is.SameAs(fault));
    }
}
