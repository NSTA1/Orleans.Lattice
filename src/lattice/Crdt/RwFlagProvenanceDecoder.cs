using System.Collections.Generic;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the remove-wins (disable-wins) flag
/// shape (<see cref="LatticeMergeMode.RwFlag"/>). Turns a <see cref="RwFlag"/>'s
/// stored state or a sequence of <see cref="RwFlagDelta"/> author deltas into
/// <see cref="CrdtMemberChange"/> events.
/// <para>
/// <strong>The flag itself is the member; remove wins.</strong> A
/// <see cref="RwFlag"/> tracks presence rather than a set of element values, so
/// every decoded event has an empty <see cref="CrdtMemberChange.Element"/>.
/// Each enable dot maps to an <see cref="CrdtMemberChangeKind.Added"/> event and
/// each disable (remove) dot to a <see cref="CrdtMemberChangeKind.Removed"/>
/// event, preserving the causal dot context. The remove-wins tie-break (a
/// disable an enable has not observed keeps the flag off) is a property of the
/// folded state's presence, not of the provenance stream - the stream records
/// every authored enable and disable. The observed-enable tombstones (disable
/// dots an enable has cancelled) are bookkeeping for that tie-break and are not
/// surfaced as separate events: the enable that authored them already appears
/// as its own <see cref="CrdtMemberChangeKind.Added"/> event, and the disable it
/// cancelled already appears as its own <see cref="CrdtMemberChangeKind.Removed"/>
/// event.
/// </para>
/// </summary>
public sealed class RwFlagProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static RwFlagProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.RwFlag;

    /// <summary>
    /// Decodes an ordered <see cref="RwFlagDelta"/> sequence into member-change
    /// events in operation order: enable dots before disable dots within a
    /// delta, the supplied order across deltas. The observed-enable tombstones
    /// are not emitted (see the type remarks). Each event has an empty element
    /// and carries the originating delta's wall-clock stamp when one was
    /// supplied.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be a
    /// <see cref="RwFlagDelta"/>.
    /// </param>
    /// <returns>The decoded member-change events, in operation order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (RwFlagDelta)deltas[i].Delta;
            if (delta.Enables is { Count: > 0 } enables) total += enables.Count;
            if (delta.Disables is { Count: > 0 } disables) total += disables.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (RwFlagDelta)entry.Delta;
            FlagProvenance.EmitDots(result, delta.Enables, CrdtMemberChangeKind.Added, entry.WallClock);
            FlagProvenance.EmitDots(result, delta.Disables, CrdtMemberChangeKind.Removed, entry.WallClock);
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="RwFlag"/>:
    /// each enable dot yields an <see cref="CrdtMemberChangeKind.Added"/> event
    /// and each disable dot a <see cref="CrdtMemberChangeKind.Removed"/> event,
    /// ordered deterministically by replica then causal ordinal then kind. The
    /// observed-enable tombstones are not surfaced separately (the disable they
    /// cancel is already emitted from <see cref="RwFlag.Disables"/>). Every event
    /// has an empty element and a <see langword="null"/>
    /// <see cref="CrdtMemberChange.WallClock"/>.
    /// </summary>
    /// <param name="state">The <see cref="RwFlag"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var flag = (RwFlag)state;
        var total = flag.Enables.Count + flag.Disables.Count;
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        FlagProvenance.EmitDots(result, flag.Enables, CrdtMemberChangeKind.Added, null);
        FlagProvenance.EmitDots(result, flag.Disables, CrdtMemberChangeKind.Removed, null);
        result.Sort(CrdtMemberChangeCausalComparer.Instance);
        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="RwFlag"/> into a single current-state member
    /// carrying its boolean state (<c>"enabled"</c> or <c>"disabled"</c>) under the
    /// remove-wins resolution. A flag that has never been enabled or disabled (no
    /// dots at all) projects to no members. Unlike
    /// <see cref="DecodeState(object)"/>, which surfaces every enable and disable
    /// dot, this returns only the resolved current presence.
    /// </summary>
    /// <param name="state">The <see cref="RwFlag"/> to project.</param>
    /// <returns>A single boolean-state member, or an empty list for an untouched flag.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var flag = (RwFlag)state;
        var hasAnyDot = flag.Enables.Count > 0 || flag.Disables.Count > 0;
        return FlagProvenance.CurrentValue(hasAnyDot, flag.IsEnabled);
    }
}
