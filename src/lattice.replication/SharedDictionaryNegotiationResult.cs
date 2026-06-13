namespace Orleans.Lattice.Replication;

/// <summary>
/// Outcome of a per-peer shared-dictionary capability negotiation computed by
/// <see cref="SharedDictionaryNegotiation.Negotiate(uint, System.Collections.Generic.IReadOnlyCollection{uint})"/>.
/// Carries the effective shared-dictionary id the sender should stamp on the
/// next batch to a peer, plus the flags that drive telemetry. This is an
/// in-process value type consumed entirely on the sender's ship path; it is
/// never serialised or placed on the wire.
/// </summary>
/// <param name="EffectiveDictionaryId">
/// The shared-dictionary id the sender should compress the next batch with for
/// this peer: the configured id when the peer advertised it, otherwise <c>0</c>
/// ("no dictionary", i.e. dictionary-less compression).
/// </param>
/// <param name="Matched">
/// <see langword="true"/> when no negotiation was required (no dictionary was
/// configured) or the peer advertised the configured dictionary id;
/// <see langword="false"/> when the sender fell back to dictionary-less
/// compression.
/// </param>
/// <param name="PeerCapabilityKnown">
/// <see langword="true"/> when the peer advertised a dictionary capability
/// (even an empty one); <see langword="false"/> when no capability has been
/// observed yet (a peer that has not acked, or a build predating dictionary
/// negotiation).
/// </param>
/// <param name="FellBack">
/// <see langword="true"/> when the sender could not honour a configured
/// dictionary for this peer and fell back to dictionary-less compression;
/// <see langword="false"/> otherwise.
/// </param>
/// <param name="FingerprintMismatch">
/// <see langword="true"/> when the peer advertised the configured dictionary
/// id but with a content fingerprint that differs from the sender's configured
/// dictionary bytes - a same-id/different-bytes misconfiguration. The sender
/// always falls back to dictionary-less compression in this case
/// (<see cref="FellBack"/> is also <see langword="true"/>); the distinct flag
/// lets the ship path surface a recognisable telemetry outcome instead of the
/// misconfiguration manifesting as a receiver-side decode failure. Defaults to
/// <see langword="false"/> on the id-only negotiation path (a peer predating
/// the fingerprint slot) and whenever no fingerprint conflict was observed.
/// </param>
public readonly record struct SharedDictionaryNegotiationResult(
    uint EffectiveDictionaryId,
    bool Matched,
    bool PeerCapabilityKnown,
    bool FellBack,
    bool FingerprintMismatch = false);
