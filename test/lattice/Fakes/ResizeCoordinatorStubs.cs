using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Stubs the resize coordinator for unit tests that drive a shard migration
/// through a substitute <see cref="IGrainFactory"/>. A migration reads
/// <see cref="ITreeResizeGrain.HoldsShardMigrationsAsync"/> before and after it
/// opens its source shard's record (issue #4452); the stub answers that no
/// resize holds migrations, and returns the shared substitute so a test can
/// make it hold them.
/// </summary>
internal static class ResizeCoordinatorStubs
{
    /// <summary>
    /// Makes every tree's resize coordinator report idle and holding no shard
    /// migration, and returns the shared substitute.
    /// </summary>
    internal static ITreeResizeGrain StubResizeIdle(this IGrainFactory grainFactory)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsIdleAsync().Returns(Task.FromResult(true));
        resize.HoldsShardMigrationsAsync().Returns(Task.FromResult(false));
        grainFactory.GetGrain<ITreeResizeGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(resize);
        return resize;
    }
}
