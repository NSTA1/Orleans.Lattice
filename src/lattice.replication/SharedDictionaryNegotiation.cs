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

    /// <summary>
    /// Negotiates the effective shared-dictionary id for the next batch to a
    /// peer, gating on a content fingerprint as well as the numeric id. Use
    /// this overload against a peer that advertised the fingerprint-bearing
    /// <see cref="ReplicationAck.AdvertisedDictionaries"/> capability: a
    /// dictionary is honoured only when the peer advertised the configured id
    /// <b>and</b> a matching <paramref name="configuredFingerprint"/>, so a
    /// same-id/different-bytes peer (an operator slip, or two clusters that
    /// each auto-trained an id 1 dictionary over different corpora) falls back
    /// to dictionary-less compression instead of shipping a frame the receiver
    /// would hard-fail to decode.
    /// </summary>
    /// <param name="configuredDictionaryId">
    /// The dictionary id the tree is configured to compress with
    /// (<see cref="LatticeReplicationOptions.FramingCompressionDictionaryId"/>).
    /// The reserved value <c>0</c> means "no dictionary configured".
    /// </param>
    /// <param name="configuredFingerprint">
    /// The content fingerprint of the sender's configured dictionary bytes
    /// (<see cref="CompressionDictionaryFingerprint.Compute(System.ReadOnlySpan{byte})"/>).
    /// Ignored when <paramref name="configuredDictionaryId"/> is <c>0</c>.
    /// </param>
    /// <param name="peerAdvertised">
    /// The <c>(id, fingerprint)</c> pairs the peer advertised on its most
    /// recent <see cref="ReplicationAck.AdvertisedDictionaries"/>, or
    /// <see langword="null"/> when the peer has not advertised the
    /// fingerprint-bearing capability yet (no ack observed, or a build
    /// predating the fingerprint slot).
    /// </param>
    /// <returns>
    /// The negotiated <see cref="SharedDictionaryNegotiationResult"/>. When the
    /// peer advertised the configured id but with a different fingerprint, the
    /// result both falls back (<see cref="SharedDictionaryNegotiationResult.FellBack"/>)
    /// and flags the misconfiguration
    /// (<see cref="SharedDictionaryNegotiationResult.FingerprintMismatch"/>) so
    /// the ship path can surface a distinct telemetry outcome.
    /// </returns>
    public static SharedDictionaryNegotiationResult Negotiate(
        uint configuredDictionaryId,
        ulong configuredFingerprint,
        IReadOnlyCollection<AdvertisedCompressionDictionary>? peerAdvertised)
    {
        if (configuredDictionaryId == 0u)
        {
            return new SharedDictionaryNegotiationResult(
                EffectiveDictionaryId: 0u,
                Matched: true,
                PeerCapabilityKnown: false,
                FellBack: false);
        }

        if (peerAdvertised is null)
        {
            return new SharedDictionaryNegotiationResult(
                EffectiveDictionaryId: 0u,
                Matched: false,
                PeerCapabilityKnown: false,
                FellBack: true);
        }

        var sawIdWithDifferentFingerprint = false;
        foreach (var advertised in peerAdvertised)
        {
            if (advertised.Id != configuredDictionaryId)
            {
                continue;
            }

            if (advertised.Fingerprint == configuredFingerprint)
            {
                // The peer advertised the configured dictionary with a
                // matching content fingerprint: the bytes agree on both
                // sides, so the sender may compress with it.
                return new SharedDictionaryNegotiationResult(
                    EffectiveDictionaryId: configuredDictionaryId,
                    Matched: true,
                    PeerCapabilityKnown: true,
                    FellBack: false);
            }

            // Same id, different bytes: record the mismatch but keep
            // scanning in case a later entry carries the matching
            // fingerprint (a defensive guard against duplicate advertised
            // ids; in practice a peer advertises each id once).
            sawIdWithDifferentFingerprint = true;
        }

        return new SharedDictionaryNegotiationResult(
            EffectiveDictionaryId: 0u,
            Matched: false,
            PeerCapabilityKnown: true,
            FellBack: true,
            FingerprintMismatch: sawIdWithDifferentFingerprint);
    }
}

/// <summary>
/// A single <c>(id, fingerprint)</c> entry in a receiver's advertised
/// shared-dictionary capability, carried on
/// <see cref="ReplicationAck.AdvertisedDictionaries"/>. The
/// <see cref="Fingerprint"/> is the
/// <see cref="CompressionDictionaryFingerprint.Compute(System.ReadOnlySpan{byte})"/>
/// of the receiver's dictionary bytes for <see cref="Id"/>, so an opted-in
/// sender can confirm both clusters hold byte-identical bytes behind the id
/// before compressing with it.
/// </summary>
/// <param name="Id">The stable shared-dictionary id (never the reserved <c>0</c>).</param>
/// <param name="Fingerprint">
/// The content fingerprint of the dictionary bytes the receiver resolves for
/// <paramref name="Id"/>. <c>0</c> when the receiver advertised the id but
/// could not resolve its bytes to fingerprint them.
/// </param>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.AdvertisedCompressionDictionary)]
[Immutable]
public readonly record struct AdvertisedCompressionDictionary(
    [property: Id(0)] uint Id,
    [property: Id(1)] ulong Fingerprint);

/// <summary>
/// Builds the fingerprint-bearing shared-dictionary capability a receiver
/// advertises on <see cref="ReplicationAck.AdvertisedDictionaries"/>. Resolves
/// every id the provider's <see cref="ILatticeCompressionDictionaryCatalog"/>
/// reports, computes each dictionary's content fingerprint via
/// <see cref="CompressionDictionaryFingerprint"/>, and returns a snapshot
/// ordered by id for a deterministic advertisement.
/// </summary>
public static class CompressionDictionaryAdvertisement
{
    private static readonly Comparison<AdvertisedCompressionDictionary> ByIdAscending =
        static (a, b) => a.Id.CompareTo(b.Id);

    // Per-(provider, id) fingerprint cache. A fingerprint for a given id is
    // immutable for the lifetime of the deployment (the provider contract
    // forbids changing the bytes behind an id), so the hash of a potentially
    // large dictionary need only be computed once per id rather than on every
    // advertisement. Keyed weakly on the provider so a discarded provider does
    // not pin its cache. A newly trained id misses the cache and is computed
    // and stored on first advertisement.
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<
        ILatticeCompressionDictionaryProvider,
        System.Collections.Concurrent.ConcurrentDictionary<uint, ulong>> FingerprintCache = new();

    /// <summary>
    /// Builds the advertised <c>(id, fingerprint)</c> set for the supplied
    /// provider. Returns <see langword="null"/> when
    /// <paramref name="provider"/> is <see langword="null"/>, does not expose a
    /// <see cref="ILatticeCompressionDictionaryCatalog"/>, or holds no
    /// dictionaries - the same "no advertised capability" signal a receiver
    /// predating dictionary negotiation produces.
    /// </summary>
    /// <param name="provider">The receiver's shared-dictionary provider.</param>
    /// <returns>
    /// A snapshot of the receiver's <c>(id, fingerprint)</c> pairs ordered by
    /// id, or <see langword="null"/> when there is nothing to advertise.
    /// </returns>
    public static AdvertisedCompressionDictionary[]? Build(
        ILatticeCompressionDictionaryProvider? provider)
    {
        if (provider is not ILatticeCompressionDictionaryCatalog catalog)
        {
            return null;
        }

        var ids = catalog.AvailableDictionaryIds;
        if (ids.Count == 0)
        {
            return null;
        }

        var cache = FingerprintCache.GetOrCreateValue(provider);
        var result = new AdvertisedCompressionDictionary[ids.Count];
        var i = 0;
        foreach (var id in ids)
        {
            var fingerprint = 0UL;
            if (id != 0u)
            {
                if (!cache.TryGetValue(id, out fingerprint)
                    && provider.TryGetDictionary(id, out var bytes))
                {
                    fingerprint = CompressionDictionaryFingerprint.Compute(bytes.Span);
                    cache.TryAdd(id, fingerprint);
                }
            }
            result[i++] = new AdvertisedCompressionDictionary(id, fingerprint);
        }

        Array.Sort(result, ByIdAscending);
        return result;
    }
}

/// <summary>
/// Computes a stable, non-cryptographic content fingerprint of a shared
/// compression dictionary's bytes. The fingerprint travels alongside the
/// dictionary id on a receiver's
/// <see cref="ReplicationAck.AdvertisedDictionaries"/> capability and lets an
/// opted-in sender gate dictionary compression on <c>(id, fingerprint)</c>
/// rather than the bare numeric id alone, so two deployments that map the same
/// id to different bytes - an operator slip, or the guaranteed collision when
/// two clusters both auto-train and each labels its first dictionary id 1 -
/// never negotiate a match and never produce a receiver-side decode failure.
/// <para>
/// The hash is the 64-bit FNV-1a of the raw dictionary bytes, matching the
/// algorithm <see cref="EncodedBatchHeader"/> uses to hash the origin
/// cluster id. FNV-1a is deterministic across processes and architectures (it operates on
/// the byte sequence directly, with no endianness or salt), so a sender and a
/// receiver that hold byte-identical dictionary bytes always compute the same
/// fingerprint. The hash is not collision-resistant against an adversary; it
/// exists to catch accidental same-id/different-bytes misconfiguration, not to
/// authenticate the dictionary.
/// </para>
/// </summary>
public static class CompressionDictionaryFingerprint
{
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    /// <summary>
    /// Computes the 64-bit FNV-1a content fingerprint of the supplied
    /// dictionary bytes. The same byte sequence always yields the same value;
    /// an empty span yields the FNV offset basis. Allocation-free.
    /// </summary>
    /// <param name="dictionaryBytes">The raw shared-dictionary bytes.</param>
    /// <returns>The stable content fingerprint.</returns>
    public static ulong Compute(ReadOnlySpan<byte> dictionaryBytes)
    {
        var hash = FnvOffsetBasis;
        for (var i = 0; i < dictionaryBytes.Length; i++)
        {
            hash ^= dictionaryBytes[i];
            hash *= FnvPrime;
        }
        return hash;
    }
}
