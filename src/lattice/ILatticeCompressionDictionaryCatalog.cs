namespace Orleans.Lattice;

/// <summary>
/// Optional companion to <see cref="ILatticeCompressionDictionaryProvider"/>
/// that enumerates the shared compression-dictionary ids the provider can
/// currently resolve. A provider implements this when its dictionary set is
/// enumerable so a receiver can advertise its dictionary capability to peers
/// (via <c>ReplicationAck.AdvertisedDictionaryIds</c>), letting an opted-in
/// sender gate dictionary compression on whether the target peer can decode
/// the chosen dictionary. Providers whose dictionary set is not enumerable
/// (or that do not participate in advertisement) simply do not implement this
/// interface; consumers treat the absence of a catalog as "no advertised
/// capability".
/// </summary>
public interface ILatticeCompressionDictionaryCatalog
{
    /// <summary>
    /// The stable ids of every shared compression dictionary this provider
    /// can currently resolve via
    /// <see cref="ILatticeCompressionDictionaryProvider.TryGetDictionary(uint, out ReadOnlyMemory{byte})"/>.
    /// Never includes the reserved id <c>0</c> ("no dictionary"). May be
    /// empty when the provider holds no dictionaries. Implementations must
    /// return a stable, immutable snapshot that is safe to enumerate
    /// concurrently.
    /// </summary>
    IReadOnlyCollection<uint> AvailableDictionaryIds { get; }
}
