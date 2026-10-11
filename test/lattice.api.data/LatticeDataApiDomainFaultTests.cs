using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Data.Tests;

[TestFixture]
public sealed class LatticeDataApiDomainFaultTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void ReadRangeAsync_preserves_domain_refusals_instead_of_blaming_the_token(bool fresh)
    {
        var fault = new LeafProjectionStaleException("projection unavailable");
        var (api, tree) = Create(fault);

        var actual = Assert.ThrowsAsync<LeafProjectionStaleException>(
            async () => await api.ReadRangeAsync(new DataRangeRequest
            {
                TreeId = "tree",
                ContinuationToken = fresh ? null : "cursor",
            }));
        Assert.That(actual, Is.SameAs(fault));
        tree.Received(1).NextEntriesAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ReadRangeAsync_still_classifies_an_expired_snapshot_as_an_invalid_continuation()
    {
        var (api, _) = Create(new LatticeCursorSnapshotExpiredException("pin expired"));

        Assert.That(
            async () => await api.ReadRangeAsync(new DataRangeRequest { TreeId = "tree", ContinuationToken = "cursor" }),
            Throws.TypeOf<ArgumentException>().With.InnerException.TypeOf<LatticeCursorSnapshotExpiredException>());
    }

    [Test]
    public void ReadRangeAsync_still_reports_an_unknown_continuation_as_an_argument_error()
    {
        var (api, _) = Create(new InvalidOperationException("cursor is not open"));

        Assert.That(
            async () => await api.ReadRangeAsync(new DataRangeRequest { TreeId = "tree", ContinuationToken = "cursor" }),
            Throws.TypeOf<ArgumentException>().With.InnerException.TypeOf<InvalidOperationException>());
    }

    private static (LatticeDataApi Api, ILattice Tree) Create(Exception fault)
    {
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);
        tree.OpenEntryCursorAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns("cursor");
        tree.NextEntriesAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<LatticeCursorEntriesPage>(fault));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>("tree").Returns(tree);
        return (new LatticeDataApi(factory, Options.Create(new LatticeApiDataOptions()), new NullTenantContextResolver()), tree);
    }
}
