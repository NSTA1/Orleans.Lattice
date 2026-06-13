namespace Orleans.Lattice.Replication;

/// <summary>
/// Pure, allocation-light helper that computes the shared compression
/// dictionary a sender should use for the next batch to a peer, given the
/// dictionary id the tree is configured with and the set of dictionary ids the
/// peer has advertised on its <see cref="ReplicationAck.AdvertisedDictionaryIds"/>.
/// The rule guarantees a sender never compresses a batch with a dictionary the
/// target peer has not advertised: when the configured dictionary is absent
/// from (or the peer has not yet advertised) its capability, the sender falls
/// back to dictionary-less compression, which every peer can decode. The
/// result is consumed entirely in-process on the ship path and is never
/// serialised.
/// </summary>
public static class SharedDictionaryNegotiation
{
    /// <summary>
    /// Negotiates the effective shared-dictionary id for the next batch to a
    /// peer.
    /// </summary>
    /// <param name="configuredDictionaryId">
    /// The dictionary id the tree is configured to compress with
    /// (<see cref="LatticeReplicationOptions.FramingCompressionDictionaryId"/>).
    /// The reserved value <c>0</c> means "no dictionary configured".
    /// </param>
    /// <param name="peerAdvertisedIds">
    /// The dictionary ids the peer has advertised on its most recent
    /// <see cref="ReplicationAck.AdvertisedDictionaryIds"/>, or
    /// <see langword="null"/> when the peer has not advertised a capability
    /// yet (no ack observed, or a build predating dictionary negotiation).
    /// </param>
    /// <returns>
    /// The negotiated <see cref="SharedDictionaryNegotiationResult"/>: the
    /// effective dictionary id to stamp on the next batch (<c>0</c> meaning
    /// dictionary-less), and the flags describing whether the configured
    /// dictionary matched, whether the peer's capability was known, and
    /// whether the sender fell back.
    /// </returns>
    public static SharedDictionaryNegotiationResult Negotiate(
        uint configuredDictionaryId,
        IReadOnlyCollection<uint>? peerAdvertisedIds)
    {
        if (configuredDictionaryId == 0u)
        {
            // No shared dictionary is configured: there is nothing to
            // negotiate and the sender ships dictionary-less exactly as a
            // build without a configured dictionary would.
            return new SharedDictionaryNegotiationResult(
                EffectiveDictionaryId: 0u,
                Matched: true,
                PeerCapabilityKnown: false,
                FellBack: false);
        }

        if (peerAdvertisedIds is null)
        {
            // The peer's capability is unknown (no ack carried the slot, or
            // the peer predates dictionary negotiation). Conservatively fall
            // back to dictionary-less compression until the peer advertises.
            return new SharedDictionaryNegotiationResult(
                EffectiveDictionaryId: 0u,
                Matched: false,
                PeerCapabilityKnown: false,
                FellBack: true);
        }

        foreach (var id in peerAdvertisedIds)
        {
            if (id == configuredDictionaryId)
            {
                // The peer advertised the configured dictionary: the sender
                // may compress with it.
                return new SharedDictionaryNegotiationResult(
                    EffectiveDictionaryId: configuredDictionaryId,
                    Matched: true,
                    PeerCapabilityKnown: true,
                    FellBack: false);
            }
        }

        // The peer advertised a capability that does not include the
        // configured dictionary: fall back to dictionary-less compression so
        // the peer can always decode the frame.
        return new SharedDictionaryNegotiationResult(
            EffectiveDictionaryId: 0u,
            Matched: false,
            PeerCapabilityKnown: true,
            FellBack: true);
    }
}
