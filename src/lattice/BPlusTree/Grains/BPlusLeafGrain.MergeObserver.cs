using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Post-merge observer wiring for the leaf grain. Resolves the registered
/// <see cref="ILatticeMergeObserver"/> (and, for supplying decoded inputs, the
/// registered <see cref="ILatticeValueDecoder"/>) once per activation, and
/// invokes the observer after a per-key LWW or CRDT merge completes so a schema
/// add-on can upcast the decoded inputs, validate / normalise the merged
/// result, and choose a <see cref="LatticeMergeOutcome"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>CRDT non-mutation invariant.</b>
/// <see cref="MergeOutcomeKind.AcceptTransformed"/> is rejected (throws
/// <see cref="InvalidOperationException"/>) for any record whose
/// <see cref="LatticeMergeMode"/> is not
/// <see cref="LatticeMergeMode.LwwRegister"/>: rewriting the canonical merged
/// bytes of a typed CRDT record would break WAL-replay determinism, because a
/// cold rebuild folds the durable delta into the prior visible state and must
/// land on identical bytes. Transform is permitted only for LWW records, where
/// the durable WAL record carries the full winning value.
/// </para>
/// <para>
/// <b>Zero-cost default.</b> With only <c>AddLattice</c> registered the observer
/// is <see cref="NullLatticeMergeObserver"/>; <see cref="MergeObserverActive"/>
/// resolves it once per activation and caches an inactive flag, so every merge
/// short-circuits on a cached <c>bool</c> - no <see cref="LatticeMergeContext"/>
/// is constructed, no value is decoded, and the merge path is byte-for-byte
/// identical to the pre-seam behaviour with no per-merge allocation.
/// </para>
/// </remarks>
internal sealed partial class BPlusLeafGrain
{
    private ILatticeMergeObserver? _mergeObserver;
    private bool _mergeObserverResolved;
    private bool _mergeObserverActive;

    private ILatticeValueDecoder? _mergeInputDecoder;
    private bool _mergeInputDecoderResolved;
    private bool _mergeInputDecoderActive;

    private ILatticeEnvelopeCodec? _envelopeCodec;
    private bool _envelopeCodecResolved;
    private bool _envelopeCodecActive;

    /// <summary>
    /// <c>true</c> when a non-null-default <see cref="ILatticeEnvelopeCodec"/> is
    /// registered and active for this leaf's tree. Resolved once per activation and
    /// cached, so the LWW and CRDT merge / apply paths pay only a cached <c>bool</c>
    /// check when no codec is wired. Gates both the per-record version read (for the
    /// merge context) and the version-agnostic delta strip (for the CRDT fold).
    /// </summary>
    private bool EnvelopeCodecActive
    {
        get
        {
            if (!_envelopeCodecResolved)
            {
                _envelopeCodec = context.ActivationServices.GetService<ILatticeEnvelopeCodec>();
                _envelopeCodecActive = _envelopeCodec is not null
                    and not NullLatticeEnvelopeCodec
                    && _envelopeCodec.IsActive(state.State.TreeId ?? string.Empty);
                _envelopeCodecResolved = true;
            }
            return _envelopeCodecActive;
        }
    }

    /// <summary>
    /// Resolves the registered <see cref="ILatticeEnvelopeCodec"/> instance from the
    /// activation services (or <c>null</c> when none is registered), for handing to a
    /// <see cref="SnapshotProjectionFolder"/> so a restore-replay fold strips the same
    /// version envelope the live apply strips. The folder guards its own
    /// active / null check, so this returns the raw resolved service.
    /// </summary>
    private ILatticeEnvelopeCodec? ResolveEnvelopeCodec()
    {
        _ = EnvelopeCodecActive; // ensure the codec is resolved and cached
        return _envelopeCodec;
    }

    /// <summary>
    /// Reads the durable schema version stamped on a stored value's envelope, or
    /// <c>0</c> when no codec is active or the value carries no envelope. Cheap
    /// header read; no allocation.
    /// </summary>
    private uint ReadEnvelopeVersion(byte[]? value) =>
        EnvelopeCodecActive ? _envelopeCodec!.ReadVersion(value) : 0;

    /// <summary>
    /// Strips the version envelope from a durable CRDT delta so the raw typed-CRDT
    /// body can be deserialized and folded. Identity (same reference) when no codec
    /// is active or the delta carries no envelope. Version-agnostic and never
    /// upcasts, so apply-time and every replay fold identical bytes - see the
    /// determinism remarks on <see cref="ILatticeEnvelopeCodec"/>.
    /// </summary>
    private byte[] StripDeltaForFold(byte[] delta) =>
        EnvelopeCodecActive ? _envelopeCodec!.StripForFold(delta) : delta;

    /// <summary>
    /// <c>true</c> when a non-null-default <see cref="ILatticeMergeObserver"/> is
    /// registered. Resolved once per activation and cached, so the LWW and CRDT
    /// merge paths pay only a cached <c>bool</c> check when no observer is wired.
    /// </summary>
    private bool MergeObserverActive
    {
        get
        {
            if (!_mergeObserverResolved)
            {
                _mergeObserver = context.ActivationServices.GetService<ILatticeMergeObserver>();
                // Null default (NullLatticeMergeObserver) accepts every merge
                // verbatim, so treat it as inactive: no observer call, no
                // context construction, no allocation on the merge path.
                _mergeObserverActive = _mergeObserver is not null and not NullLatticeMergeObserver;
                _mergeObserverResolved = true;
            }
            return _mergeObserverActive;
        }
    }

    /// <summary>
    /// Decodes a stored value into the logical (envelope-stripped) form the
    /// observer reasons about, using the registered
    /// <see cref="ILatticeValueDecoder"/> when it is active for this leaf's
    /// tree. Pass-through (identity) when no decoder is active, which is the
    /// default, so the decoded form equals the stored form.
    /// </summary>
    private async ValueTask<byte[]?> DecodeMergeInputAsync(byte[]? storedValue, CancellationToken cancellationToken)
    {
        if (storedValue is null)
        {
            return null;
        }

        if (!_mergeInputDecoderResolved)
        {
            _mergeInputDecoder = context.ActivationServices.GetService<ILatticeValueDecoder>();
            _mergeInputDecoderActive = _mergeInputDecoder is not null
                && _mergeInputDecoder.IsActive(state.State.TreeId ?? string.Empty);
            _mergeInputDecoderResolved = true;
        }

        if (!_mergeInputDecoderActive)
        {
            return storedValue;
        }

        return await _mergeInputDecoder!
            .DecodeAsync(state.State.TreeId ?? string.Empty, storedValue, cancellationToken)
            ;
    }

    /// <summary>
    /// Invokes the post-merge observer for a completed per-key merge and returns
    /// the canonical <b>stored</b> bytes to persist. The returned array equals
    /// <paramref name="mergedStored"/> for an <see cref="MergeOutcomeKind.Accept"/>
    /// or <see cref="MergeOutcomeKind.AcceptWithEvent"/> outcome; for an
    /// <see cref="MergeOutcomeKind.AcceptTransformed"/> outcome (LWW only) it is
    /// the observer-supplied replacement, which the observer authors in stored
    /// (enveloped) form.
    /// </summary>
    /// <param name="key">The merged key.</param>
    /// <param name="mode">The declared merge mode for the record (from <c>WalRecord.Mode</c>).</param>
    /// <param name="localStored">The prior (local) stored value, or <c>null</c> when the key had no prior value.</param>
    /// <param name="incomingStored">The incoming stored value, or <c>null</c> when the change was a typed CRDT delta.</param>
    /// <param name="mergedStored">The canonical stored merged result.</param>
    /// <param name="cancellationToken">Cancels the observation.</param>
    /// <param name="incomingDeltaForVersion">
    /// For a typed CRDT merge (where <paramref name="incomingStored"/> is <c>null</c>),
    /// the stored (possibly enveloped) delta bytes the incoming schema version is
    /// read from; ignored for LWW, where the version is read from
    /// <paramref name="incomingStored"/>.
    /// </param>
    /// <returns>The canonical stored bytes to persist.</returns>
    /// <exception cref="InvalidOperationException">
    /// The observer returned <see cref="MergeOutcomeKind.AcceptTransformed"/> for
    /// a record whose <paramref name="mode"/> is not
    /// <see cref="LatticeMergeMode.LwwRegister"/>.
    /// </exception>
    private async ValueTask<byte[]> ApplyMergeObserverAsync(
        string key,
        LatticeMergeMode mode,
        byte[]? localStored,
        byte[]? incomingStored,
        byte[] mergedStored,
        CancellationToken cancellationToken,
        byte[]? incomingDeltaForVersion = null)
    {
        // Read the per-record durable schema versions from the stored (enveloped)
        // inputs before they are decoded (envelope-stripped). Identity 0 when no
        // codec is active. The incoming version for a CRDT merge comes from the
        // delta bytes (incomingStored is null for a typed delta).
        var localVersion = ReadEnvelopeVersion(localStored);
        var incomingVersion = ReadEnvelopeVersion(incomingStored ?? incomingDeltaForVersion);

        // Decode the inputs / result into the logical form the observer reasons
        // about. Identity on the default (no decoder) path.
        var localDecoded = await DecodeMergeInputAsync(localStored, cancellationToken);
        var incomingDecoded = await DecodeMergeInputAsync(incomingStored, cancellationToken);
        var mergedDecoded = await DecodeMergeInputAsync(mergedStored, cancellationToken)
            ?? mergedStored;

        var ctx = new LatticeMergeContext(
            key, mode, localDecoded, incomingDecoded, mergedDecoded,
            state.State.TreeId, localVersion, incomingVersion);
        var outcome = await _mergeObserver!.OnMergedAsync(in ctx, cancellationToken);

        switch (outcome.Kind)
        {
            case MergeOutcomeKind.AcceptTransformed:
                if (mode != LatticeMergeMode.LwwRegister)
                {
                    // Mutating the canonical merged bytes of a typed CRDT record
                    // would break WAL-replay determinism: a cold rebuild folds
                    // the durable delta into the prior visible state and must
                    // reconstruct identical bytes. Only LWW records, whose WAL
                    // record carries the full winning value, may be transformed.
                    throw new InvalidOperationException(
                        "A merge observer returned AcceptTransformed for a record whose merge mode is '"
                        + mode
                        + "'. Transform is permitted only for LatticeMergeMode.LwwRegister; a CRDT-mode "
                        + "merge is non-mutating (Accept / AcceptWithEvent only) so canonical merged "
                        + "bytes stay deterministic under WAL replay.");
                }

                return outcome.TransformedValue!;

            case MergeOutcomeKind.AcceptWithEvent:
            case MergeOutcomeKind.Accept:
            default:
                // Non-mutating: keep the canonical stored bytes. The
                // AcceptWithEvent reason is a non-mutating annotation surfaced to
                // a future event sink; it does not alter the stored value.
                return mergedStored;
        }
    }
}
