namespace Orleans.Lattice.Testing;

/// <summary>
/// A page returned by a provider's bytes-shaped read, decoded by the probe into
/// <see cref="WalContractEntry"/> values so
/// <see cref="WalStorageProviderContractTestsBase"/> can compare it with the
/// entry-shaped read.
/// </summary>
/// <param name="Entries">The decoded entries, in the order the provider returned them.</param>
/// <param name="HighestOffsetInclusive">The page's reported highest offset.</param>
public sealed record WalContractEncodedPage(IReadOnlyList<WalContractEntry> Entries, long HighestOffsetInclusive);
