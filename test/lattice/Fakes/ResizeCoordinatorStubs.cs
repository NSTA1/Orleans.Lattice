using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Stubs the resize coordinator for unit tests that drive a shard migration
/// through a substitute <see cref="IGrainFactory"/>. A migration reads
/// <see cref="ITreeResizeGrain.IsIdleAsync"/> before and after it opens its
/// source shard's record (issue #4452), and an unconfigured substitute answers
/// <see langword="false"/> - a resize in flight - which would refuse every
/// migration the test means to run.
/// </summary>
internal static class ResizeCoordinatorStubs
{
    /// <summary>
    /// Makes every tree's resize coordinator report idle and returns the shared
    /// substitute, so a test can flip it to in flight.
    /// </summary>
    internal static ITreeResizeGrain StubResizeIdle(this IGrainFactory grainFactory)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsIdleAsync().Returns(Task.FromResult(true));
        grainFactory.GetGrain<ITreeResizeGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(resize);
        return resize;
    }
}
