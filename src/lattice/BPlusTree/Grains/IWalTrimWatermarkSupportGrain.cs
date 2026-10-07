namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Capability marker (issue #4621): a silo hosts this grain when it runs a build
/// whose WAL providers persist the trim watermark before a trim deletes anything.
/// A silo that predates it trims without moving the watermark, so a reader that
/// trusted the watermark while such a silo is in the cluster would read that
/// silo's trims as holes and skip them silently. Readers therefore trust the
/// watermark only once every silo in the cluster manifest hosts this interface;
/// see <see cref="WalTrimWatermarkSupport"/>. It is never activated.
/// </summary>
[Alias(TypeAliases.IWalTrimWatermarkSupportGrain)]
internal interface IWalTrimWatermarkSupportGrain : IGrainWithIntegerKey
{
    /// <summary>Returns <see langword="true"/>; exists so the interface has a method.</summary>
    Task<bool> IsSupportedAsync();
}
