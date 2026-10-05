namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Implementation of <see cref="IWalTrimWatermarkSupportGrain"/>: the capability
/// marker a silo hosts when its WAL providers persist the trim watermark
/// (issue #4621). It holds no state and is never activated by the library.
/// </summary>
internal sealed class WalTrimWatermarkSupportGrain : Grain, IWalTrimWatermarkSupportGrain
{
    /// <inheritdoc />
    public Task<bool> IsSupportedAsync() => Task.FromResult(true);
}
