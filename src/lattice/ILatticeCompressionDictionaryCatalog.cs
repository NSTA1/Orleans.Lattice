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

/// <summary>
/// Optional companion to <see cref="ILatticeCompressionDictionaryProvider"/>
/// that installs shared compression-dictionary bytes pulled from a peer under
/// a stable numeric id. A provider implements this when its dictionary set can
/// grow at runtime - for example the auto-training provider, which converges a
/// pair of clusters onto a shared dictionary by pulling the bytes behind a
/// peer-advertised id it does not yet hold and installing them locally.
/// <para>
/// Installation is content-addressed and idempotent: installing the same bytes
/// under an id the provider already resolves to those bytes is a no-op that
/// succeeds, while installing <em>different</em> bytes under an id already in
/// use is rejected (returns <see langword="false"/>) so a pulled payload can
/// never silently overwrite an existing dictionary and corrupt in-flight
/// frames compressed against the original bytes. Callers are responsible for
/// verifying the bytes against the advertised content fingerprint
/// <em>before</em> calling <see cref="TryInstall"/>; this interface installs
/// exactly what it is given.
/// </para>
/// </summary>
public interface ILatticeCompressionDictionarySink
{
    /// <summary>
    /// Attempts to install <paramref name="dictionary"/> under
    /// <paramref name="dictionaryId"/> so subsequent
    /// <see cref="ILatticeCompressionDictionaryProvider.TryGetDictionary(uint, out ReadOnlyMemory{byte})"/>
    /// calls for that id resolve to the supplied bytes. Returns
    /// <see langword="true"/> when the id is newly installed or already
    /// resolves to byte-identical content; returns <see langword="false"/>
    /// when the id is the reserved value <c>0</c>, when the bytes are empty,
    /// or when the id already resolves to <em>different</em> bytes (a
    /// collision the caller must not silently overwrite).
    /// </summary>
    /// <param name="dictionaryId">
    /// The stable dictionary id to install under. The reserved value <c>0</c>
    /// ("no dictionary") is always rejected.
    /// </param>
    /// <param name="dictionary">The dictionary bytes to install.</param>
    /// <returns>
    /// <see langword="true"/> when the bytes are resolvable under the id after
    /// the call; otherwise <see langword="false"/>.
    /// </returns>
    bool TryInstall(uint dictionaryId, ReadOnlyMemory<byte> dictionary);
}
