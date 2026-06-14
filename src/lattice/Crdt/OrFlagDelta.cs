namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for an observed-remove (enable-wins) flag mutation.
/// Carries the enable dots added and the disable dots observed since the
/// receiver's cursor; the dot context
/// (<see cref="OrSetDot.ReplicaId"/> + <see cref="OrSetDot.Counter"/>) is
/// what makes <see cref="OrFlag"/>s converge under concurrent active-active
/// updates where post-merge LWW-on-bytes would silently drop one side's
/// enable.
/// <para>
/// Apply semantics on the receiver: union <see cref="Enables"/> into the
/// local enable-dot set, then union <see cref="Disables"/> into the local
/// tombstone set. The result is independent of arrival order, duplicate
/// delivery, and partial overlap with the local state.
/// </para>
/// <para>
/// Emitters always populate both collections (use empty arrays for
/// "no enables" / "no disables"); use <see cref="Empty"/> to author a
/// no-op delta without allocating fresh empty arrays. The
/// <see langword="default"/> instance has <c>null</c> collections and is
/// intended only as the zero-value of the struct - consumers should either
/// treat <c>null</c> as empty or assert non-null at the apply boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrFlagDelta)]
[Immutable]
public readonly record struct OrFlagDelta
{
    /// <summary>
    /// The enable dots added since the receiver's cursor. An empty list
    /// indicates a delta that contains only disables.
    /// </summary>
    [Id(0)] public IReadOnlyList<OrSetDot> Enables { get; init; }

    /// <summary>
    /// The enable dots the originator has now observed-as-disabled. An
    /// empty list indicates a delta that contains only enables.
    /// </summary>
    [Id(1)] public IReadOnlyList<OrSetDot> Disables { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null) <see cref="Enables"/>
    /// and <see cref="Disables"/> collections. Backed by
    /// <see cref="Array.Empty{T}"/> so repeated access does not allocate.
    /// </summary>
    public static OrFlagDelta Empty { get; } = new()
    {
        Enables = Array.Empty<OrSetDot>(),
        Disables = Array.Empty<OrSetDot>(),
    };
}
