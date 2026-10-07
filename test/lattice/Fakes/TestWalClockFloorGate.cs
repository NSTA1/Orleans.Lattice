using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Settable <see cref="IWalClockFloorGate"/> for unit tests that construct a
/// <see cref="WalShardGrain"/> directly. Closed by default, which is the
/// pre-#4586 behaviour: the partition never advances a floor.
/// </summary>
internal sealed class TestWalClockFloorGate : IWalClockFloorGate
{
    /// <inheritdoc />
    public bool IsOpen { get; set; }
}
