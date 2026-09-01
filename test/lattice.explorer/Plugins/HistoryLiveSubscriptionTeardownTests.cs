using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Plugins.History;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The revision timeline releases its live subscription cleanly when the change
/// feed ends on its own (issue #1855).
/// </summary>
/// <remarks>
/// <para>
/// The follow loop owns the linked token source it was handed and disposes it in
/// a <c>finally</c>. It used to leave the surface's field pointing at that
/// disposed source, so the next teardown - a key change, or the view being
/// disposed when the operator switches surface or selection - cancelled a source
/// that no longer existed. <c>Cancel</c> is the one member of a token source
/// that is not safe to call after <c>Dispose</c>, so this threw
/// <see cref="ObjectDisposedException"/> out of <c>IDisposable.Dispose</c>, and
/// a component that throws from disposal faults the renderer rather than the
/// component.
/// </para>
/// <para>
/// It needed a feed that ends by itself to reproduce, which is exactly what a
/// server-side stream close or an empty change feed does in production, so
/// nothing about it is a test artefact. The reproduction is deterministic: the
/// stream is already finished, so no clock, delay or race decides the outcome.
/// </para>
/// </remarks>
[TestFixture]
public sealed class HistoryLiveSubscriptionTeardownTests
{
    [Test]
    public void A_live_feed_that_ends_on_its_own_does_not_fault_the_views_disposal()
    {
        var surface = SurfaceWithAnAlreadyFinishedFeed();

        // Rendering runs the surface through its whole lifetime, disposal
        // included: the renderer disposes every component it built, and a throw
        // from there surfaces here.
        Assert.DoesNotThrowAsync(async () =>
            await SelectionViewRenderHarness.RenderAsync<HistoryTab, IHistorySurface>(
                surface,
                SelectionViewRenderHarness.Tree()));
    }

    [Test]
    public async Task A_live_feed_that_ends_on_its_own_still_renders_the_timeline()
    {
        var surface = SurfaceWithAnAlreadyFinishedFeed();

        var html = await SelectionViewRenderHarness.RenderAsync<HistoryTab, IHistorySurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        // The subscription ending is not a failure, so the surface must not
        // report one: it simply has no revisions for this key.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("No revisions for this key"));
            Assert.That(html, Does.Not.Contain("Live updates stopped"));
        });
    }

    private static IHistorySurface SurfaceWithAnAlreadyFinishedFeed()
    {
        var surface = Substitute.For<IHistorySurface>();
        surface.InspectedKey(Arg.Any<string>()).Returns("orders/1");
        surface
            .LoadAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new HistoryPage
            {
                Status = StateQueryStatus.Found,
                Revisions = [],
            }));

        surface
            .FollowAsync(Arg.Any<string>(), Arg.Any<HistoryLiveTail>(), Arg.Any<CancellationToken>())
            .Returns(_ => FinishedFeed());

        return surface;
    }

#pragma warning disable CS1998 // a stream that is already finished has nothing to await
    private static async IAsyncEnumerable<HistoryRevisionRow> FinishedFeed()
    {
        yield break;
    }
#pragma warning restore CS1998
}
