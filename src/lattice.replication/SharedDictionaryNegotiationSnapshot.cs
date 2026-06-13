namespace Orleans.Lattice.Replication;

/// <summary>
/// Point-in-time snapshot of a single peer's shared-dictionary negotiation
/// state, returned from <see cref="SharedDictionaryNegotiationState.Snapshot"/>.
/// </summary>
/// <param name="Tree">The replicated tree id.</param>
/// <param name="Peer">The remote peer cluster id.</param>
/// <param name="EffectiveDictionaryId">
/// The effective shared-dictionary id the sender stamps for this peer
/// (<c>0</c> meaning dictionary-less).
/// </param>
/// <param name="Matched">
/// <see langword="true"/> when the configured dictionary was honoured (or none
/// was configured); <see langword="false"/> when the sender fell back.
/// </param>
/// <param name="PeerCapabilityKnown">
/// <see langword="true"/> when the peer advertised a dictionary capability;
/// <see langword="false"/> when none has been observed yet.
/// </param>
/// <param name="FellBack">
/// <see langword="true"/> when the sender fell back to dictionary-less
/// compression for this peer.
/// </param>
/// <param name="FingerprintMismatch">
/// <see langword="true"/> when the fallback was caused by the peer advertising
/// the configured dictionary id with a content fingerprint that differs from
/// the sender's configured dictionary bytes (a same-id/different-bytes
/// misconfiguration).
/// </param>
public readonly record struct SharedDictionaryNegotiationSnapshot(
    string Tree,
    string Peer,
    uint EffectiveDictionaryId,
    bool Matched,
    bool PeerCapabilityKnown,
    bool FellBack,
    bool FingerprintMismatch = false);
