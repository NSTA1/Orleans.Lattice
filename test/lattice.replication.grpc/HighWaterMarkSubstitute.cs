using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// A substituted high-water-mark grain whose identity-recording advance
/// (issue #4586) delegates to its own <c>TryAdvanceAsync</c>, so a test that
/// stubs or asserts the advance keeps doing so unchanged.
/// </summary>
internal static class HighWaterMarkSubstitute
{
    public static IReplicationHighWaterMarkGrain Create()
    {
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwm.AdvanceAppliedAsync(
                Arg.Any<string>(),
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<IReadOnlyList<HybridLogicalClock>>(),
                Arg.Any<bool>(),
                Arg.Any<CancellationToken>())
            .Returns(call => (bool)call[3]
                ? hwm.TryAdvanceAsync((string)call[0], (HybridLogicalClock)call[1], (CancellationToken)call[4])
                : Task.FromResult(false));
        return hwm;
    }
}
