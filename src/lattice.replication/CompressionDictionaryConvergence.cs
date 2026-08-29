using Orleans.Lattice;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Pure-orchestration helper that converges the local silo onto a peer's
/// trained shared compression dictionary: for every <c>(id, fingerprint)</c>
/// the peer advertised that the local provider does not already hold, it
/// pulls the bytes behind the id over the transport
/// (<see cref="IReplicationDigestProbeTransport.PullCompressionDictionaryAsync"/>),
/// verifies the returned bytes against the advertised fingerprint
/// (<see cref="CompressionDictionaryFingerprint"/>), and installs them through
/// the provider's <see cref="ILatticeCompressionDictionarySink"/>. This is the
/// receiver half of the self-distributing shared dictionary: once converged,
/// the local silo can decode frames the peer compressed against the dictionary
/// and re-advertise the id onward so the whole fleet settles on one dictionary
/// without any operator distributing bytes by hand.
/// <para>
/// Every pull is fingerprint-gated end to end: bytes are installed only when
/// the recomputed fingerprint of the returned bytes equals both the
/// fingerprint the peer advertised and the fingerprint the peer echoed on the
/// response. A mismatch (corruption in flight, or a peer that mislabelled the
/// id) is discarded without installing, so a pulled payload can never corrupt
/// an existing dictionary or be silently trusted.
/// </para>
/// </summary>
public static class CompressionDictionaryConvergence
{
    /// <summary>
    /// Converges the local provider onto the peer's advertised dictionaries.
    /// Returns the number of dictionaries newly installed by this call. A
    /// provider that is not an <see cref="ILatticeCompressionDictionarySink"/>,
    /// an empty or <see langword="null"/> advertisement, or a peer that serves
    /// nothing new all yield <c>0</c>.
    /// </summary>
    /// <param name="transport">The probe transport that performs the pull RPC.</param>
    /// <param name="provider">
    /// The local shared-dictionary provider. Must implement
    /// <see cref="ILatticeCompressionDictionarySink"/> for any convergence to
    /// occur; otherwise the call is a no-op.
    /// </param>
    /// <param name="targetClusterId">The peer cluster id to pull from.</param>
    /// <param name="peerAdvertised">
    /// The <c>(id, fingerprint)</c> pairs the peer advertised on its most
    /// recent <see cref="ReplicationAck.AdvertisedDictionaries"/>, or
    /// <see langword="null"/> when none were observed.
    /// </param>
    /// <param name="treeId">The tree id, used as the convergence metric's tree tag.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The count of dictionaries newly installed locally.</returns>
    public static async Task<int> ConvergeAsync(
        IReplicationDigestProbeTransport transport,
        ILatticeCompressionDictionaryProvider provider,
        string targetClusterId,
        IReadOnlyCollection<AdvertisedCompressionDictionary>? peerAdvertised,
        string treeId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(provider);

        if (peerAdvertised is null
            || peerAdvertised.Count == 0
            || provider is not ILatticeCompressionDictionarySink sink)
        {
            return 0;
        }

        var installed = 0;
        foreach (var advertised in peerAdvertised)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // The reserved id 0 ("no dictionary") is never distributed, an
            // id the peer could not fingerprint (advertised fingerprint 0)
            // cannot be verified, and an id already resolvable locally needs
            // no pull.
            if (advertised.Id == 0u
                || advertised.Fingerprint == 0UL
                || provider.TryGetDictionary(advertised.Id, out _))
            {
                continue;
            }

            var response = await transport.PullCompressionDictionaryAsync(
                targetClusterId,
                new CompressionDictionaryPullRequest { DictionaryId = advertised.Id },
                cancellationToken);

            if (!response.ExchangeSupported
                || !response.Found
                || response.DictionaryId != advertised.Id
                || response.Dictionary.IsEmpty)
            {
                LatticeReplicationMetrics.DictionaryConvergence.Add(
                    1,
                    new System.Diagnostics.TagList
                    {
                        new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
                        new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, targetClusterId),
                        new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.DictionaryConvergenceOutcomeUnavailable),
                        LatticeTenantLabel.ForTree(treeId),
                    });
                continue;
            }

            var computed = CompressionDictionaryFingerprint.Compute(response.Dictionary.Span);
            var verified = computed == advertised.Fingerprint
                && (response.Fingerprint == 0UL || response.Fingerprint == computed);

            var accepted = verified && sink.TryInstall(advertised.Id, response.Dictionary);
            if (accepted)
            {
                installed++;
            }

            LatticeReplicationMetrics.DictionaryConvergence.Add(
                1,
                new System.Diagnostics.TagList
                {
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, targetClusterId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, accepted ? LatticeReplicationMetrics.DictionaryConvergenceOutcomeInstalled : LatticeReplicationMetrics.DictionaryConvergenceOutcomeRejected),
                    LatticeTenantLabel.ForTree(treeId),
                });
        }

        return installed;
    }
}
