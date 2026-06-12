using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side acknowledgement returned from
/// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>.
/// Carries the per-origin high-water-mark the receiver advanced to as a
/// result of applying the batch, so the sender can advance its own
/// per-peer cursor strictly to that point on success - the canonical
/// "advance-cursor-on-ack" semantic the design doc requires.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationAck)]
[Immutable]
public readonly record struct ReplicationAck
{
    /// <summary>
    /// <see langword="true"/> when the receiver successfully received and
    /// processed the batch. <see langword="false"/> when the receiver
    /// rejected the batch outright (transport-level error, schema
    /// mismatch, unknown tree, etc.) and the sender should not advance
    /// its cursor past the batch's start.
    /// <para>
    /// Note that <see cref="Accepted"/> is <see langword="true"/> even
    /// when every entry in the batch was de-duplicated by the per-origin
    /// high-water-mark - dedup is a successful idempotent apply, not a
    /// rejection. In that case <see cref="HighestAppliedHlc"/> reflects
    /// the receiver's existing HWM and the sender's cursor still
    /// advances.
    /// </para>
    /// </summary>
    [Id(0)] public bool Accepted { get; init; }

    /// <summary>
    /// The per-origin high-water-mark for
    /// <c>(ReplicationBatch.TreeName, ReplicationBatch.OriginClusterId)</c>
    /// after the receiver finished processing the batch. The sender
    /// advances its per-peer cursor strictly to this value on success;
    /// on a partial apply (some entries applied, some failed) the
    /// receiver still returns the highest HLC it actually advanced its
    /// HWM to, and the sender resumes from there.
    /// <para>
    /// When <see cref="Accepted"/> is <see langword="false"/> this value
    /// is undefined and the sender must not consume it.
    /// </para>
    /// </summary>
    [Id(1)] public HybridLogicalClock HighestAppliedHlc { get; init; }

    /// <summary>
    /// Receiver-side blocked-floor pin for the tree at the moment
    /// the ack was constructed: the lowest HLC across every partially-
    /// staged atomic batch the receiver is currently buffering, or
    /// <see langword="null"/> when the receiver has no in-flight
    /// atomic-batch admissions for this tree (or the receiver is
    /// pre-Phase-9 and never stamped the slot at all).
    /// <para>
    /// On a positive ack the sender publishes this value to its local
    /// <see cref="IWalCursorRegistry"/> under a
    /// peer-specific consumer id so the producer-side WAL GC AND-s
    /// the strict-less <c>entry.Timestamp &lt; blockedFloor</c>
    /// clause into its trim predicate. This is the cross-cluster
    /// propagation channel for the receiver's TX-aware GC pin: with
    /// it, a buffered atomic batch on cluster B prevents cluster A
    /// from trimming the corresponding WAL entries even when wall-
    /// clock TTL would otherwise allow it. When the field is
    /// <see langword="null"/> the producer-side registry pin is
    /// cleared (or never registered), and the sender's GC degrades
    /// cleanly to the cursor + TTL + causal-stable predicate.
    /// </para>
    /// <para>
    /// Strictly additive on the wire: pre-Phase-9 receivers omit the
    /// slot entirely (decodes as <see langword="null"/>); pre-Phase-9
    /// senders ignore the field. The slot is therefore safe to roll
    /// out independently on either side of a peering.
    /// </para>
    /// </summary>
    [Id(2)] public HybridLogicalClock? BlockedAtHlc { get; init; }

    /// <summary>
    /// Receiver-side flow-control hint: the largest batch the receiver
    /// would like the sender to ship on the next pump tick, expressed
    /// as a strictly-positive entry count. The sender clamps the value
    /// to the closed interval
    /// <c>[1, LatticeReplicationOptions.ShipBatchSize]</c> and uses it
    /// as the per-tick batch cap until the receiver lifts or revises
    /// the hint. A value of <see langword="null"/> means "no preference";
    /// the sender resumes shipping at its configured
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/> on the
    /// next tick, which is the canonical re-acceleration signal once
    /// the receiver has recovered from a transient load spike.
    /// <para>
    /// Strictly additive on the wire: receivers built before the
    /// receiver-side flow-control wave omit the slot entirely
    /// (decodes as <see langword="null"/>); senders built before that
    /// wave ignore the field. The slot is therefore safe to roll out
    /// independently on either side of a peering.
    /// </para>
    /// <para>
    /// The receiver-side source of the hint is the pluggable
    /// <c>IReceiverFlowControlPolicy</c> seam. The default
    /// implementation is a no-op that always returns <c>null</c>
    /// hints, preserving today's blind-push behaviour for hosts that
    /// have not opted in.
    /// </para>
    /// </summary>
    [Id(3)] public int? SuggestedBatchSize { get; init; }

    /// <summary>
    /// Receiver-side flow-control hint: number of milliseconds the
    /// sender should pause before its next pump tick to this peer.
    /// Composes with the shipper's existing exponential-backoff retry
    /// budget by advancing the per-peer retry deadline to
    /// <c>max(currentBackoffDeadline, now + PauseForMs)</c>, so a
    /// receiver-requested pause never shortens an in-progress backoff.
    /// A value of <see langword="null"/> or <c>&lt;= 0</c> means
    /// "no pause requested"; the sender's next tick fires on its
    /// normal cadence.
    /// <para>
    /// Strictly additive on the wire (same compat profile as
    /// <see cref="SuggestedBatchSize"/>). The slot is the canonical
    /// way for a struggling receiver to throttle a sender without
    /// timing out the RPC.
    /// </para>
    /// </summary>
    [Id(4)] public int? PauseForMs { get; init; }

    /// <summary>
    /// The maximum framing wire-format version this receiver can decode
    /// (its build's <see cref="EncodedBatchHeader.CurrentWireVersion"/>).
    /// Advertised on every ack so an opted-in sender can observe the
    /// peer's capability and compute the negotiated target version
    /// (<c>min(localCurrent, peerAdvertised)</c>) for telemetry and the
    /// minimum-floor fail-fast guard, then down-stamp the outbound
    /// framing header to that target via
    /// <see cref="WireVersionDownEncoder"/> so a not-yet-upgraded
    /// receiver decodes and applies the frame during a rolling upgrade.
    /// A value of <see langword="null"/> means the receiver did not
    /// advertise a capability (a build predating wire-version
    /// negotiation); the sender treats that peer's capability as
    /// unknown and falls back to the conservative
    /// <c>UnknownPeerWireVersionFloor</c> until a later ack carries the
    /// slot.
    /// <para>
    /// Strictly additive on the wire (same compat profile as
    /// <see cref="SuggestedBatchSize"/> and <see cref="PauseForMs"/>):
    /// receivers built before wire-version negotiation omit the slot
    /// entirely (decodes as <see langword="null"/>); senders built
    /// before negotiation ignore the field. The slot is therefore safe
    /// to roll out independently on either side of a peering.
    /// </para>
    /// </summary>
    [Id(5)] public int? SupportedWireVersion { get; init; }
}

/// <summary>
/// Pure, allocation-free helper that computes the wire-format version a
/// sender would target for a given peer, applying the capability
/// advertised on <see cref="ReplicationAck.SupportedWireVersion"/>.
/// This is the reusable negotiation surface that records the negotiated
/// target for telemetry and preserves the canonical fail-fast hard
/// error when a peer falls below the sender's minimum-supported floor.
/// The negotiated target is consumed by
/// <see cref="WireVersionDownEncoder"/>, which down-stamps the outbound
/// framing header so a sender on the current build can ship a frame an
/// older receiver decodes and applies during a rolling upgrade; a
/// same-version target stays a verbatim no-op.
/// </summary>
public static class WireVersionNegotiation
{
    /// <summary>
    /// Negotiates the effective wire-format version for the next batch
    /// to a peer.
    /// </summary>
    /// <param name="localCurrentVersion">
    /// The sender's current wire version
    /// (<see cref="EncodedBatchHeader.CurrentWireVersion"/>). Must be at
    /// least <c>1</c>.
    /// </param>
    /// <param name="minimumSupportedVersion">
    /// The oldest wire version the sender is willing to interoperate
    /// with. A peer advertising a version strictly below this throws
    /// <see cref="NotSupportedException"/> - the genuinely-unsupported
    /// case that must still fail fast. Must lie in the closed interval
    /// <c>[1, localCurrentVersion]</c>.
    /// </param>
    /// <param name="unknownPeerFloorVersion">
    /// The conservative version the sender encodes at until the peer's
    /// capability is known (no ack has advertised a version yet). Must
    /// lie in the closed interval
    /// <c>[minimumSupportedVersion, localCurrentVersion]</c>.
    /// </param>
    /// <param name="peerAdvertisedVersion">
    /// The peer's most recently advertised
    /// <see cref="ReplicationAck.SupportedWireVersion"/>, or
    /// <see langword="null"/> when the peer has not advertised a
    /// capability yet.
    /// </param>
    /// <returns>
    /// The negotiated <see cref="WireVersionNegotiationResult"/>: the
    /// effective version to encode at, whether a downgrade is in
    /// effect, and whether the peer's capability was known.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="localCurrentVersion"/>,
    /// <paramref name="minimumSupportedVersion"/>, or
    /// <paramref name="unknownPeerFloorVersion"/> violate the documented
    /// ordering constraints.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// Thrown when <paramref name="peerAdvertisedVersion"/> is strictly
    /// less than <paramref name="minimumSupportedVersion"/> - the peer
    /// is older than the sender's minimum-supported floor and cannot be
    /// down-encoded for.
    /// </exception>
    public static WireVersionNegotiationResult Negotiate(
        int localCurrentVersion,
        int minimumSupportedVersion,
        int unknownPeerFloorVersion,
        int? peerAdvertisedVersion)
    {
        if (localCurrentVersion < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(localCurrentVersion), localCurrentVersion,
                "The local current wire version must be at least 1.");
        }

        if (minimumSupportedVersion < 1 || minimumSupportedVersion > localCurrentVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(minimumSupportedVersion), minimumSupportedVersion,
                $"The minimum supported wire version must lie in the closed interval "
                + $"[1, {localCurrentVersion}].");
        }

        if (unknownPeerFloorVersion < minimumSupportedVersion
            || unknownPeerFloorVersion > localCurrentVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(unknownPeerFloorVersion), unknownPeerFloorVersion,
                $"The unknown-peer floor version must lie in the closed interval "
                + $"[{minimumSupportedVersion}, {localCurrentVersion}].");
        }

        if (peerAdvertisedVersion is not { } peer)
        {
            // Capability not yet known: encode at the conservative
            // floor. The floor is bounded above by localCurrentVersion,
            // so a downgrade is active iff the host configured a floor
            // below its current version.
            return new WireVersionNegotiationResult
            {
                EffectiveWireVersion = unknownPeerFloorVersion,
                DowngradeActive = unknownPeerFloorVersion < localCurrentVersion,
                PeerCapabilityKnown = false,
            };
        }

        if (peer < minimumSupportedVersion)
        {
            throw new NotSupportedException(
                $"Peer advertised wire version {peer}, which is older than the sender's "
                + $"minimum supported version {minimumSupportedVersion}; the sender cannot "
                + "down-encode that far. Upgrade the peer before it can resume receiving "
                + "from this sender.");
        }

        var effective = Math.Min(localCurrentVersion, peer);
        return new WireVersionNegotiationResult
        {
            EffectiveWireVersion = effective,
            DowngradeActive = effective < localCurrentVersion,
            PeerCapabilityKnown = true,
        };
    }
}

/// <summary>
/// Outcome of a single wire-version capability negotiation between the
/// local sender and a remote peer, computed by
/// <see cref="WireVersionNegotiation.Negotiate(int, int, int, int?)"/>.
/// <para>
/// The result is a pure in-process value type - it is not Orleans
/// serialised and never travels on the wire. The advertised peer
/// capability that feeds the negotiation travels additively on
/// <see cref="ReplicationAck.SupportedWireVersion"/>; this type is the
/// sender-side projection the shipper records (the negotiated target
/// version, and whether that target is below the sender's current
/// version for a mixed-version fleet).
/// </para>
/// </summary>
public readonly record struct WireVersionNegotiationResult
{
    /// <summary>
    /// The negotiated target wire-format version for this peer:
    /// <c>min(localCurrent, peerAdvertised)</c> once the peer's
    /// capability is known, or the conservative unknown-peer floor until
    /// the first acknowledgement advertises a version. Always lies in
    /// the closed interval <c>[minimumSupported, localCurrent]</c>. This
    /// is the version <see cref="WireVersionDownEncoder"/> stamps on the
    /// outbound framing header: a same-version target is a verbatim
    /// no-op, while an older target down-stamps the header so a
    /// not-yet-upgraded receiver decodes and applies the frame.
    /// </summary>
    public int EffectiveWireVersion { get; init; }

    /// <summary>
    /// <see langword="true"/> when <see cref="EffectiveWireVersion"/>
    /// is strictly less than the sender's current wire version - i.e.
    /// the negotiated target is below the local current version, so
    /// <see cref="WireVersionDownEncoder"/> down-stamps the framing
    /// header for this older peer. Surfaced to operators through the
    /// <c>replication.wire_version.downgrade_active</c> gauge so a
    /// mixed-version fleet is observable during a rolling upgrade.
    /// </summary>
    public bool DowngradeActive { get; init; }

    /// <summary>
    /// <see langword="true"/> when the peer has advertised a supported
    /// wire version (via <see cref="ReplicationAck.SupportedWireVersion"/>)
    /// and the negotiation used that value;
    /// <see langword="false"/> when the peer's capability is not yet
    /// known and the conservative unknown-peer floor was used instead.
    /// </summary>
    public bool PeerCapabilityKnown { get; init; }
}

/// <summary>
/// Version-adaptive framing helper that prepares an outbound batch's
/// fixed <see cref="EncodedBatchHeader"/> for a negotiated effective
/// wire version older than the sender's current build, so a sender
/// running the current build can ship a frame a not-yet-upgraded
/// receiver decodes <em>and</em> applies during a rolling upgrade. This
/// is the consumer of the negotiated target version computed by
/// <see cref="WireVersionNegotiation.Negotiate(int, int, int, int?)"/>:
/// the negotiation observes the peer's advertised capability and the
/// shipper threads the resulting
/// <see cref="WireVersionNegotiationResult.EffectiveWireVersion"/>
/// through this helper onto the framing header it stamps.
/// <para>
/// <b>Same-version is a true no-op.</b> When the effective version
/// equals <see cref="EncodedBatchHeader.CurrentWireVersion"/> the
/// helper returns the supplied header unchanged and the shipper keeps
/// its verbatim pre-encoded entry hot path: no entry segment is
/// re-encoded and the bytes on the wire are byte-identical to a build
/// that never negotiated. The re-encode cost is paid only when a frame
/// is genuinely down-stamped for an older peer.
/// </para>
/// <para>
/// <b>Why the down-encode is header-only.</b> Each prior framing
/// version elided a per-entry field rather than adding one, and the
/// surviving entry-segment bytes the current build produces are already
/// a strict subset of what an older receiver expects:
/// </para>
/// <list type="bullet">
///   <item><description>
///     Wire version 4 elided the per-entry <c>WalRecord.TreeId</c> slot;
///     the current build also elides it, and a version-4 receiver
///     re-stamps the tree id from the framing tail's <c>TreeName</c>. The
///     entry segments are therefore identical between version 4 and
///     version 5.
///   </description></item>
///   <item><description>
///     Wire version 5 hoisted the per-entry merge mode into the header's
///     packed slot. <c>WalRecord.Mode</c> carries no Orleans <c>[Id]</c>
///     tag, so it is never serialised onto an entry segment in any
///     version, and a version-4 producer's per-entry mode was uniformly
///     the <see cref="LatticeMergeMode.LwwRegister"/> enum default
///     (omitted by the serializer). A version-4 receiver therefore reads
///     <see cref="LatticeMergeMode.LwwRegister"/> for every entry.
///   </description></item>
/// </list>
/// <para>
/// Down-stamping to version 4 is consequently exact when - and only
/// when - the batch's merge mode is
/// <see cref="LatticeMergeMode.LwwRegister"/> and the framing tail is
/// uncompressed. A version-4 receiver reading the version-5 header's
/// trailing packed 32-bit slot interprets bits 16-23 as part of its
/// 24-bit <see cref="EncodedBatchHeader.AtomicBatchSpanCount"/>; those
/// bits are zero precisely when <see cref="EncodedBatchHeader.Mode"/> is
/// the <see cref="LatticeMergeMode.LwwRegister"/> default, so the header
/// bytes are then fully version-4-compatible. A CRDT-mode batch cannot
/// be down-stamped: its per-entry merge dispatch depends on the hoisted
/// header mode that a pre-version-5 receiver cannot read, so
/// down-stamping would silently mis-apply the entries. The helper fails
/// fast with <see cref="NotSupportedException"/> in that case (the
/// genuinely un-down-encodable case), exactly as the negotiation floor
/// guard does for a peer below
/// <see cref="LatticeReplicationOptions.MinimumSupportedWireVersion"/>.
/// </para>
/// <para>
/// Framing-tail compression rides the header without a wire-version
/// bump, so a pre-version-5 receiver is not guaranteed to carry the
/// matching <see cref="ILatticeCompressor"/>; the helper therefore also
/// refuses to down-stamp a compressed batch. Operators running a
/// heterogeneous fleet leave
/// <see cref="LatticeReplicationOptions.FramingCompression"/> at
/// <see cref="LatticeCompression.None"/> until the fleet is uniform.
/// </para>
/// </summary>
public static class WireVersionDownEncoder
{
    /// <summary>
    /// The oldest framing wire version this build can down-stamp a batch
    /// to. Older receivers expect per-entry field shapes the current
    /// build no longer carries on the entry segments (for example the
    /// pre-version-4 per-entry <c>WalRecord.TreeId</c> tag), so a frame
    /// cannot be made decodable for them without re-encoding every
    /// entry; the helper fails fast for an effective version below this
    /// floor. Independent of
    /// <see cref="LatticeReplicationOptions.MinimumSupportedWireVersion"/>:
    /// a host may set a lower negotiation floor, but a peer that
    /// advertises a version in
    /// <c>[MinimumSupportedWireVersion, MinimumDownEncodableWireVersion)</c>
    /// still surfaces a fail-fast error on the ship path rather than
    /// receiving a corrupt frame.
    /// </summary>
    public const int MinimumDownEncodableWireVersion = 4;

    /// <summary>
    /// Validates that a batch with the supplied <paramref name="mode"/>
    /// and <paramref name="compression"/> can be safely framed at
    /// <paramref name="effectiveWireVersion"/>. A no-op when
    /// <paramref name="effectiveWireVersion"/> equals
    /// <see cref="EncodedBatchHeader.CurrentWireVersion"/> (same-version
    /// peers are never down-stamped). For an older effective version it
    /// enforces the down-stamp preconditions: the version must be at
    /// least <see cref="MinimumDownEncodableWireVersion"/>, the merge
    /// mode must be <see cref="LatticeMergeMode.LwwRegister"/>, and the
    /// framing tail must be uncompressed
    /// (<see cref="LatticeCompression.None"/>).
    /// </summary>
    /// <param name="effectiveWireVersion">
    /// The negotiated target framing wire version. Must lie in the
    /// closed interval
    /// <c>[1, EncodedBatchHeader.CurrentWireVersion]</c>.
    /// </param>
    /// <param name="mode">The batch's per-tree merge mode.</param>
    /// <param name="compression">The framing-tail compression tag the batch will carry.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="effectiveWireVersion"/> is less than
    /// <c>1</c> or greater than
    /// <see cref="EncodedBatchHeader.CurrentWireVersion"/>.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// Thrown when the batch cannot be down-stamped to
    /// <paramref name="effectiveWireVersion"/>: the version is below
    /// <see cref="MinimumDownEncodableWireVersion"/>, the merge mode is a
    /// CRDT mode (anything other than
    /// <see cref="LatticeMergeMode.LwwRegister"/>), or the framing tail
    /// is compressed.
    /// </exception>
    public static void EnsureDownEncodable(
        int effectiveWireVersion,
        LatticeMergeMode mode,
        LatticeCompression compression)
    {
        if (effectiveWireVersion < 1 || effectiveWireVersion > EncodedBatchHeader.CurrentWireVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(effectiveWireVersion), effectiveWireVersion,
                $"The effective wire version must lie in the closed interval "
                + $"[1, {EncodedBatchHeader.CurrentWireVersion}].");
        }

        if (effectiveWireVersion == EncodedBatchHeader.CurrentWireVersion)
        {
            // Same-version peer: nothing to down-stamp, the verbatim hot
            // path applies.
            return;
        }

        if (effectiveWireVersion < MinimumDownEncodableWireVersion)
        {
            throw new NotSupportedException(
                $"Cannot down-stamp a batch to framing wire version {effectiveWireVersion}: the "
                + $"oldest down-encodable version is {MinimumDownEncodableWireVersion}. Older "
                + "receivers expect per-entry field shapes this build no longer carries on the "
                + "encoded entry segments; upgrade the peer before it can resume receiving from "
                + "this sender.");
        }

        if (mode != LatticeMergeMode.LwwRegister)
        {
            throw new NotSupportedException(
                $"Cannot down-stamp a {mode} batch to framing wire version {effectiveWireVersion}: "
                + "the per-tree merge mode is hoisted into the version-5 framing header, which a "
                + "pre-version-5 receiver cannot read, so it would apply every entry as "
                + $"{nameof(LatticeMergeMode)}.{nameof(LatticeMergeMode.LwwRegister)} and silently "
                + "diverge. Only last-writer-wins trees can be down-stamped; upgrade the peer "
                + "before replicating a CRDT-mode tree to it.");
        }

        if (compression != LatticeCompression.None)
        {
            throw new NotSupportedException(
                $"Cannot down-stamp a {compression}-compressed batch to framing wire version "
                + $"{effectiveWireVersion}: framing-tail compression is not guaranteed to be "
                + "decodable by a pre-version-5 receiver. Disable "
                + $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.FramingCompression)} "
                + "for trees replicated to a mixed-version fleet, or upgrade the peer first.");
        }
    }

    /// <summary>
    /// Returns the supplied <paramref name="header"/> stamped with
    /// <paramref name="effectiveWireVersion"/>, validating the
    /// down-stamp preconditions first via
    /// <see cref="EnsureDownEncodable(int, LatticeMergeMode, LatticeCompression)"/>.
    /// When <paramref name="effectiveWireVersion"/> already equals
    /// <see cref="EncodedBatchHeader.WireVersion"/> the input header is
    /// returned unchanged (a true no-op; no copy is made), so a
    /// same-version peer pays nothing.
    /// </summary>
    /// <param name="header">
    /// The framing header built for the current wire version. Its
    /// <see cref="EncodedBatchHeader.Mode"/> and
    /// <see cref="EncodedBatchHeader.Compression"/> drive the down-stamp
    /// validation.
    /// </param>
    /// <param name="effectiveWireVersion">
    /// The negotiated target framing wire version to stamp.
    /// </param>
    /// <returns>
    /// The header to frame the batch with: the input unchanged for a
    /// same-version peer, or a copy whose
    /// <see cref="EncodedBatchHeader.WireVersion"/> is the negotiated
    /// older version for a down-stamped peer.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="effectiveWireVersion"/> is out of the
    /// supported range (see
    /// <see cref="EnsureDownEncodable(int, LatticeMergeMode, LatticeCompression)"/>).
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// Thrown when the batch cannot be down-stamped to
    /// <paramref name="effectiveWireVersion"/> (see
    /// <see cref="EnsureDownEncodable(int, LatticeMergeMode, LatticeCompression)"/>).
    /// </exception>
    public static EncodedBatchHeader PrepareHeader(
        in EncodedBatchHeader header,
        int effectiveWireVersion)
    {
        EnsureDownEncodable(effectiveWireVersion, header.Mode, header.Compression);

        if (effectiveWireVersion == header.WireVersion)
        {
            return header;
        }

        return header with { WireVersion = effectiveWireVersion };
    }
}
