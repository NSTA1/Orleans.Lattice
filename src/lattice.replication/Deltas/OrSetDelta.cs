namespace Orleans.Lattice.Replication;

/// <summary>
/// Typed delta record for an observed-remove (OR) set mutation. Carries
/// the dots added and the dots removed since the receiver's cursor; the
/// dot context (<see cref="OrSetDot.ReplicaId"/> + <see cref="OrSetDot.Counter"/>)
/// is what makes OR-Sets converge under concurrent active-active updates
/// where post-merge LWW-on-bytes would silently drop one side's add.
/// <para>
/// Apply semantics on the receiver: union <see cref="Adds"/> into the
/// local element/dot map, then drop every (element, dot) pair listed in
/// <see cref="Removes"/>. The result is independent of arrival order,
/// duplicate delivery, and partial overlap with the local state.
/// </para>
/// <para>
/// Emitters always populate both collections (use empty arrays for
/// "no adds" / "no removes"); use <see cref="Empty"/> to author a
/// no-op delta without allocating fresh empty arrays. The
/// <see cref="default"/> instance has <c>null</c> collections and is
/// intended only as the zero-value of the struct - consumers should
/// either treat <c>null</c> as empty or assert non-null at the apply
/// boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.OrSetDelta)]
[Immutable]
public readonly record struct OrSetDelta
{
    /// <summary>
    /// The (element, dot) pairs added since the receiver's cursor.
    /// An empty list indicates a delta that contains only removes.
    /// </summary>
    [Id(0)] public IReadOnlyList<OrSetDot> Adds { get; init; }

    /// <summary>
    /// The (element, dot) pairs whose adds the originator has now
    /// observed-as-removed. An empty list indicates a delta that
    /// contains only adds.
    /// </summary>
    [Id(1)] public IReadOnlyList<OrSetDot> Removes { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null) <see cref="Adds"/>
    /// and <see cref="Removes"/> collections. Backed by
    /// <see cref="Array.Empty{T}"/> so repeated access does not allocate.
    /// </summary>
    public static OrSetDelta Empty { get; } = new()
    {
        Adds = Array.Empty<OrSetDot>(),
        Removes = Array.Empty<OrSetDot>(),
    };
}
