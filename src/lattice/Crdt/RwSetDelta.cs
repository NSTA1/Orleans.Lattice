namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a remove-wins observed-remove set mutation - the
/// set-granularity generalisation of <see cref="RwFlagDelta"/>. Carries the
/// add dots authored, the remove dots authored, and the remove dots an
/// observed add has now cancelled (tombstones) since the receiver's cursor,
/// each attached to its element. The dot context
/// (<see cref="OrSetDeltaDot.ReplicaId"/> + <see cref="OrSetDeltaDot.Counter"/>)
/// is what makes <see cref="RwSet"/>s converge under concurrent active-active
/// updates where post-merge LWW-on-bytes would silently drop one side's
/// remove.
/// <para>
/// Apply semantics on the receiver: union <see cref="Adds"/> into the local
/// per-element add-dot map, union <see cref="Removes"/> into the local
/// remove-dot map, then union <see cref="Tombstones"/> into the local
/// tombstone map. A concurrent add and remove of the same element converge
/// remove-wins: the remove dot survives unless an add explicitly tombstoned
/// it. The result is independent of arrival order, duplicate delivery, and
/// partial overlap with the local state.
/// </para>
/// <para>
/// Emitters always populate every collection (use empty arrays for
/// "no adds" / "no removes" / "no tombstones"); use <see cref="Empty"/> to
/// author a no-op delta without allocating fresh empty arrays. The
/// <see langword="default"/> instance has <c>null</c> collections and is
/// intended only as the zero-value of the struct - consumers should either
/// treat <c>null</c> as empty or assert non-null at the apply boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RwSetDelta)]
[Immutable]
public readonly record struct RwSetDelta
{
    /// <summary>
    /// The (element, dot) add pairs authored since the receiver's cursor.
    /// An empty list indicates a delta that asserts no new membership.
    /// </summary>
    [Id(0)] public IReadOnlyList<OrSetDeltaDot> Adds { get; init; }

    /// <summary>
    /// The (element, dot) remove pairs authored since the receiver's cursor.
    /// An empty list indicates a delta that contains no new removes.
    /// </summary>
    [Id(1)] public IReadOnlyList<OrSetDeltaDot> Removes { get; init; }

    /// <summary>
    /// The (element, dot) remove pairs an observed add has now
    /// cancelled. An empty list indicates a delta whose adds cancelled no
    /// prior remove.
    /// </summary>
    [Id(2)] public IReadOnlyList<OrSetDeltaDot> Tombstones { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null) <see cref="Adds"/>,
    /// <see cref="Removes"/>, and <see cref="Tombstones"/> collections.
    /// Backed by <see cref="Array.Empty{T}"/> so repeated access does not
    /// allocate.
    /// </summary>
    public static RwSetDelta Empty { get; } = new()
    {
        Adds = Array.Empty<OrSetDeltaDot>(),
        Removes = Array.Empty<OrSetDeltaDot>(),
        Tombstones = Array.Empty<OrSetDeltaDot>(),
    };
}
