using System.Collections.Generic;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the observed-remove (enable-wins)
/// flag shape (<see cref="LatticeMergeMode.OrFlag"/>). Turns an
/// <see cref="OrFlag"/>'s stored state or a sequence of
/// <see cref="OrFlagDelta"/> author deltas into <see cref="CrdtMemberChange"/>
/// events.
/// <para>
/// <strong>The flag itself is the member.</strong> An <see cref="OrFlag"/>
/// tracks presence rather than a set of element values, so it carries no
/// element payload: every decoded event has an empty
/// <see cref="CrdtMemberChange.Element"/> and the "member" it refers to is the
/// flag. Each enable dot maps to an <see cref="CrdtMemberChangeKind.Added"/>
/// event and each disable (observed-remove) dot to a
/// <see cref="CrdtMemberChangeKind.Removed"/> event, preserving the causal dot
/// context so concurrent enable / disable from different replicas are all
/// represented (add-wins: a disable cancels only the enable dots it observed).
/// </para>
/// </summary>
public sealed class OrFlagProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static OrFlagProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.OrFlag;

    /// <summary>
    /// Decodes an ordered <see cref="OrFlagDelta"/> sequence into member-change
    /// events in operation order: enable dots before disable dots within a
    /// delta, the supplied order across deltas. Each event has an empty element
    /// and carries the originating delta's wall-clock stamp when one was
    /// supplied.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be an
    /// <see cref="OrFlagDelta"/>.
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
            var delta = (OrFlagDelta)deltas[i].Delta;
            if (delta.Enables is { Count: > 0 } enables) total += enables.Count;
            if (delta.Disables is { Count: > 0 } disables) total += disables.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (OrFlagDelta)entry.Delta;
            FlagProvenance.EmitDots(result, delta.Enables, CrdtMemberChangeKind.Added, entry.WallClock);
            FlagProvenance.EmitDots(result, delta.Disables, CrdtMemberChangeKind.Removed, entry.WallClock);
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="OrFlag"/>:
    /// each enable dot yields an <see cref="CrdtMemberChangeKind.Added"/> event
    /// and each tombstone (disable) dot a
    /// <see cref="CrdtMemberChangeKind.Removed"/> event, ordered
    /// deterministically by replica then causal ordinal then kind. Every event
    /// has an empty element and a <see langword="null"/>
    /// <see cref="CrdtMemberChange.WallClock"/>.
    /// </summary>
    /// <param name="state">The <see cref="OrFlag"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var flag = (OrFlag)state;
        var total = flag.Enables.Count + flag.Tombstones.Count;
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        FlagProvenance.EmitDots(result, flag.Enables, CrdtMemberChangeKind.Added, null);
        FlagProvenance.EmitDots(result, flag.Tombstones, CrdtMemberChangeKind.Removed, null);
        result.Sort(CrdtMemberChangeCausalComparer.Instance);
        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="OrFlag"/> into a single current-state member
    /// carrying its boolean state (<c>"enabled"</c> or <c>"disabled"</c>). A flag
    /// that has never been enabled or disabled (no dots at all) projects to no
    /// members. Unlike <see cref="DecodeState(object)"/>, which surfaces every
    /// enable and disable dot, this returns only the resolved current presence.
    /// </summary>
    /// <param name="state">The <see cref="OrFlag"/> to project.</param>
    /// <returns>A single boolean-state member, or an empty list for an untouched flag.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var flag = (OrFlag)state;
        var hasAnyDot = flag.Enables.Count > 0 || flag.Tombstones.Count > 0;
        return FlagProvenance.CurrentValue(hasAnyDot, flag.IsEnabled);
    }
}
