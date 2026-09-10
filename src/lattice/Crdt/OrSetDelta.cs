namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for an observed-remove (OR) set mutation. Carries
/// the dots added and the dots removed since the receiver's cursor; the
/// dot context (<see cref="OrSetDeltaDot.ReplicaId"/> + <see cref="OrSetDeltaDot.Counter"/>)
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
/// <see langword="default"/> instance has <c>null</c> collections and is
/// intended only as the zero-value of the struct - consumers should
/// either treat <c>null</c> as empty or assert non-null at the apply
/// boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrSetDelta)]
[Immutable]
public readonly record struct OrSetDelta
{
    /// <summary>
    /// The (element, dot) pairs added since the receiver's cursor.
    /// An empty list indicates a delta that contains only removes.
    /// </summary>
    [Id(0)] public IReadOnlyList<OrSetDeltaDot> Adds { get; init; }

    /// <summary>
    /// The (element, dot) pairs whose adds the originator has now
    /// observed-as-removed. An empty list indicates a delta that
    /// contains only adds.
    /// </summary>
    [Id(1)] public IReadOnlyList<OrSetDeltaDot> Removes { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null) <see cref="Adds"/>
    /// and <see cref="Removes"/> collections. Backed by
    /// <see cref="Array.Empty{T}"/> so repeated access does not allocate.
    /// </summary>
    public static OrSetDelta Empty { get; } = new()
    {
        Adds = Array.Empty<OrSetDeltaDot>(),
        Removes = Array.Empty<OrSetDeltaDot>(),
    };

    /// <summary>
    /// Compares two deltas by value, with <see cref="Adds"/> and
    /// <see cref="Removes"/> compared element-by-element using the
    /// value equality of <see cref="OrSetDeltaDot"/>. The compiler-generated
    /// record-struct equality compares the collection references with
    /// <see cref="EqualityComparer{T}.Default"/>, so two structurally
    /// identical deltas built from independently allocated collections - and,
    /// in particular, a delta and its post-serialization self - would
    /// otherwise never compare equal.
    /// </summary>
    /// <param name="other">The delta to compare against.</param>
    public bool Equals(OrSetDelta other) =>
        ListEqual(Adds, other.Adds) && ListEqual(Removes, other.Removes);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        AddList(ref hash, Adds);
        AddList(ref hash, Removes);
        return hash.ToHashCode();
    }

    private static bool ListEqual(IReadOnlyList<OrSetDeltaDot>? left, IReadOnlyList<OrSetDeltaDot>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        for (var i = 0; i < left.Count; i++)
        {
            if (!left[i].Equals(right[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static void AddList(ref HashCode hash, IReadOnlyList<OrSetDeltaDot>? list)
    {
        if (list is null)
        {
            hash.Add(0);
            return;
        }

        hash.Add(list.Count);
        foreach (var element in list)
        {
            hash.Add(element);
        }
    }
}
