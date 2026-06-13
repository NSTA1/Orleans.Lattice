using System.Collections.Concurrent;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Process-wide record of the most recent per-<c>(tree, peer)</c> shared-
/// dictionary negotiation outcome. Mirrors the role of
/// <see cref="WireVersionNegotiationState"/> for the dictionary channel: the
/// per-<c>(tree, peer)</c> shipper records the outcome each pump tick once it
/// has negotiated the effective dictionary against the peer's advertised
/// capability, and <see cref="Snapshot"/> exposes the current state for
/// diagnostics and tests. Unlike the wire-version state this type registers no
/// observable gauges - the negotiation outcome and the share of batches
/// shipped with versus without a dictionary are published as counters on
/// <see cref="LatticeReplicationMetrics"/> from the ship path. Registered as a
/// DI singleton by <c>AddLatticeReplication</c>.
/// </summary>
/// <remarks>
/// The class is thread-safe: concurrent updates to different
/// <c>(tree, peer)</c> pairs do not contend, and updates to the same pair take
/// a per-entry lock.
/// </remarks>
public sealed class SharedDictionaryNegotiationState
{
    private readonly ConcurrentDictionary<PeerKey, NegotiationEntry> _state = new();

    /// <summary>
    /// Records the negotiated shared-dictionary outcome for a
    /// <c>(tree, peer)</c> pair, overwriting any prior outcome for the pair
    /// (so a reconnect or capability change is reflected on the next tick).
    /// </summary>
    /// <param name="tree">The replicated tree id.</param>
    /// <param name="peer">The remote peer cluster id.</param>
    /// <param name="result">The negotiation outcome to record.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="tree"/> or <paramref name="peer"/> is
    /// <see langword="null"/>.
    /// </exception>
    public void Record(string tree, string peer, SharedDictionaryNegotiationResult result)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        var entry = _state.GetOrAdd(new PeerKey(tree, peer), static _ => new NegotiationEntry());
        lock (entry)
        {
            entry.EffectiveDictionaryId = result.EffectiveDictionaryId;
            entry.Matched = result.Matched;
            entry.PeerCapabilityKnown = result.PeerCapabilityKnown;
            entry.FellBack = result.FellBack;
            entry.FingerprintMismatch = result.FingerprintMismatch;
        }
    }

    /// <summary>
    /// Returns a point-in-time snapshot of every recorded <c>(tree, peer)</c>
    /// pair's shared-dictionary negotiation state. Useful for diagnostics and
    /// for asserting on negotiation inputs in tests.
    /// </summary>
    public IReadOnlyCollection<SharedDictionaryNegotiationSnapshot> Snapshot()
    {
        var list = new List<SharedDictionaryNegotiationSnapshot>(_state.Count);
        foreach (var kv in _state)
        {
            uint effective;
            bool matched, known, fellBack, fingerprintMismatch;
            lock (kv.Value)
            {
                effective = kv.Value.EffectiveDictionaryId;
                matched = kv.Value.Matched;
                known = kv.Value.PeerCapabilityKnown;
                fellBack = kv.Value.FellBack;
                fingerprintMismatch = kv.Value.FingerprintMismatch;
            }
            list.Add(new SharedDictionaryNegotiationSnapshot(
                kv.Key.Tree, kv.Key.Peer, effective, matched, known, fellBack, fingerprintMismatch));
        }
        return list;
    }

    private readonly record struct PeerKey(string Tree, string Peer);

    private sealed class NegotiationEntry
    {
        public uint EffectiveDictionaryId;
        public bool Matched;
        public bool PeerCapabilityKnown;
        public bool FellBack;
        public bool FingerprintMismatch;
    }
}
