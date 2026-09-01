using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Plugins.Data;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The entry inspector releases its live subscription cleanly when the change
/// feed ends on its own.
/// </summary>
/// <remarks>
/// <para>
/// This is the sibling of the defect fixed in the revision timeline under
/// issue #1855. That fix carried a warning that any surface with the same
/// fire-and-forget shape needed the same treatment; a sweep found exactly one,
/// here, and this fixture pins it.
/// </para>
/// <para>
/// <c>FollowLoopAsync</c> owns the linked token source it is handed and disposes
/// it in a <c>finally</c>. It used to leave <c>_liveCts</c> pointing at that
/// disposed source, so the next <c>StopFollowing</c> - which runs from the
/// component's own <c>Dispose</c> - called <c>Cancel</c> on it. <c>Cancel</c> is
/// the one member of a token source that is not safe after <c>Dispose</c>, so it
/// threw <see cref="ObjectDisposedException"/> out of <c>IDisposable.Dispose</c>,
/// and a component that throws from disposal faults the renderer rather than the
/// component. The neighbouring refresh-debounce loop in the same file already
/// released its field correctly, which is what made this an oversight rather
/// than a deliberate asymmetry.
/// </para>
/// <para>
/// Reproducing it needs a feed that ends by itself, which is what a server-side
/// stream close or an empty change feed does in production - so nothing here is
/// a test artefact. It is deterministic: the stream is already finished, so no
/// clock, delay or race decides the outcome.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DataLiveSubscriptionTeardownTests
{
    private const string InspectedKey = "orders/1";

    [Test]
    public void A_live_entry_feed_that_ends_on_its_own_does_not_fault_the_views_disposal()
    {
        var surface = SurfaceWithAnAlreadyFinishedFeed();

        // Rendering runs the surface through its whole lifetime, disposal
        // included: the renderer disposes every component it built, and a throw
        // from there surfaces here.
        Assert.DoesNotThrowAsync(async () =>
            await SelectionViewRenderHarness.RenderAsync<DataTab, IDataSurface>(
                surface,
                SelectionViewRenderHarness.Tree()));
    }

    [Test]
    public async Task A_live_entry_feed_that_ends_on_its_own_is_not_reported_as_a_failure()
    {
        // The subscription ending is not an error. Surfacing one would send the
        // reader to diagnose a healthy stream that simply closed.
        var surface = SurfaceWithAnAlreadyFinishedFeed();

        var html = await SelectionViewRenderHarness.RenderAsync<DataTab, IDataSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.That(html, Does.Not.Contain("Live updates stopped"));
    }

    private static IDataSurface SurfaceWithAnAlreadyFinishedFeed()
    {
        var reader = Substitute.For<IDataReader>();
        reader
            .ScanAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string?>(),
                Arg.Any<TagFilter?>(),
                Arg.Any<string?>(),
                Arg.Any<EntryScanMode>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DataPage { Entries = [] }));

        var surface = Substitute.For<IDataSurface>();
        surface.CreatePager().Returns(_ => new DataPager(reader));
        surface.RetainedLoaded.Returns(true);
        surface.GetRetainedView(Arg.Any<string>())
            .Returns(new DataRetainedView(string.Empty, DataPaging.DefaultPageSize, EntryScanMode.Live, null));
        surface.ObserveConnection(Arg.Any<Action<LatticeConnectionState>>())
            .Returns(Substitute.For<IDisposable>());
        surface.ListTagIndexesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<TagIndexRef>>([]));

        // The three facts that put the view on the live path: it supports live
        // follow, it restores an inspected key on mount (which is what starts the
        // follow loop), and that key resolves to an entry.
        surface.SupportsLiveFollow.Returns(true);
        surface.GetInspectedKey(Arg.Any<string>()).Returns(InspectedKey);
        surface.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DataEntry?>(null));

        surface
            .FollowEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => FinishedFeed());

        return surface;
    }

#pragma warning disable CS1998 // a stream that is already finished has nothing to await
    private static async IAsyncEnumerable<EntryChangeSignal> FinishedFeed()
    {
        yield break;
    }
#pragma warning restore CS1998
}
