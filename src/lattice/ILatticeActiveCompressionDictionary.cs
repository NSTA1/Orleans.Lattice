namespace Orleans.Lattice;

/// <summary>
/// Optional companion to <see cref="ILatticeCompressionDictionaryProvider"/>
/// that exposes the shared compression dictionary the provider currently wants
/// senders to compress with - the "active" dictionary id. A provider implements
/// this when its active dictionary is chosen at runtime rather than pinned by a
/// static option: the auto-training provider publishes a fresh id every time it
/// rolls a newly trained dictionary, and the opted-in ship path reads
/// <see cref="ActiveDictionaryId"/> as the configured id to negotiate and stamp,
/// so a host never has to chase the dynamic id through configuration. Providers
/// whose dictionary is statically configured do not implement this interface;
/// the ship path then falls back to the static
/// <c>LatticeReplicationOptions.FramingCompressionDictionaryId</c>.
/// </summary>
public interface ILatticeActiveCompressionDictionary
{
    /// <summary>
    /// The stable id of the dictionary this provider currently wants senders to
    /// compress with, or <c>0</c> ("no dictionary") when the provider has not
    /// yet produced one (for example an auto-training provider before its first
    /// successful training pass). Never throws; reads a lock-free snapshot safe
    /// to call on the ship hot path.
    /// </summary>
    uint ActiveDictionaryId { get; }
}
