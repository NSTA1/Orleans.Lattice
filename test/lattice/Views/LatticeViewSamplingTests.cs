using NSubstitute;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class LatticeViewSamplingTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task SampleStatsAsync_uses_one_status_call_and_counts_only_visible_rows(bool aggregation)
    {
        using var cancellation = new CancellationTokenSource();
        var token = cancellation.Token;
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetStatusAsync(token).Returns(new ViewMaintainerStatus(7, "view-sample#g2"));
        var tree = Substitute.For<ILattice>();
        tree.CountAsync(token).Returns(_ =>
        {
            Assert.That(ViewReadContext.IsAuthorised, Is.True);
            return 11;
        });
        tree.CountAsync(AggregationRowCodec.FirstNonReservedKey, null, token).Returns(_ =>
        {
            Assert.That(ViewReadContext.IsAuthorised, Is.True);
            return 3;
        });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>("view-sample#g2").Returns(tree);
        var view = new LatticeView("sample", factory, maintainer, TimeSpan.FromSeconds(1), aggregation);

        var sample = await view.SampleStatsAsync(token);

        Assert.That(sample, Is.EqualTo((7L, aggregation ? 3 : 11)));
        await maintainer.Received(1).GetStatusAsync(token);
        await maintainer.DidNotReceiveWithAnyArgs().GetLagAsync(default);
        await maintainer.DidNotReceiveWithAnyArgs().GetActiveTreeIdAsync(default);
        if (aggregation)
            await tree.DidNotReceive().CountAsync(token);
        else
            await tree.DidNotReceiveWithAnyArgs().CountAsync(null, null, default);
        Assert.That(ViewReadContext.IsAuthorised, Is.False);
    }

    [Test]
    public void SampleStatsAsync_failed_status_does_not_count()
    {
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetStatusAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ViewMaintainerStatus>(new InvalidOperationException("unavailable")));
        var factory = Substitute.For<IGrainFactory>();
        var view = new LatticeView("sample", factory, maintainer, TimeSpan.FromSeconds(1));

        Assert.That(async () => await view.SampleStatsAsync(default),
            Throws.TypeOf<InvalidOperationException>());
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILattice>(default!);
    }
}
