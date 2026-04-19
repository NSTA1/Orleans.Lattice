using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// Progress of a resumable range-delete cursor (F-033) after one
/// <see cref="ILattice.DeleteRangeStepAsync"/> call.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorDeleteProgress)]
[Immutable]
public readonly record struct LatticeCursorDeleteProgress
{
    /// <summary>Number of live keys tombstoned by this step (0 when exhausted).</summary>
    [Id(0)] public int DeletedThisStep { get; init; }

    /// <summary>Total keys tombstoned by this cursor across every step so far.</summary>
    [Id(1)] public int DeletedTotal { get; init; }

    /// <summary>
    /// <c>true</c> once the cursor's range has been fully drained. No further
    /// keys remain; subsequent <see cref="ILattice.DeleteRangeStepAsync"/>
    /// calls are idempotent no-ops.
    /// </summary>
    [Id(2)] public bool IsComplete { get; init; }
}
