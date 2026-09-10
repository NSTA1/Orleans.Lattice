namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a remove-wins (disable-wins) flag mutation.
/// Carries the enable dots added, the disable dots added, and the disable
/// dots an enable has now observed-as-cancelled (tombstones) since the
/// receiver's cursor; the dot context
/// (<see cref="OrSetDot.ReplicaId"/> + <see cref="OrSetDot.Counter"/>) is
/// what makes <see cref="RwFlag"/>s converge under concurrent active-active
/// updates where post-merge LWW-on-bytes would silently drop one side's
/// disable.
/// <para>
/// Apply semantics on the receiver: union <see cref="Enables"/> into the
/// local enable-dot set, union <see cref="Disables"/> into the local
/// disable-dot set, then union <see cref="Tombstones"/> into the local
/// tombstone set. The result is independent of arrival order, duplicate
/// delivery, and partial overlap with the local state.
/// </para>
/// <para>
/// Emitters always populate every collection (use empty arrays for
/// "no enables" / "no disables" / "no tombstones"); use <see cref="Empty"/>
/// to author a no-op delta without allocating fresh empty arrays. The
/// <see langword="default"/> instance has <c>null</c> collections and is
/// intended only as the zero-value of the struct - consumers should either
/// treat <c>null</c> as empty or assert non-null at the apply boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RwFlagDelta)]
[Immutable]
public readonly record struct RwFlagDelta
{
    /// <summary>
    /// The enable dots added since the receiver's cursor. An empty list
    /// indicates a delta that asserts no new presence.
    /// </summary>
    [Id(0)] public IReadOnlyList<OrSetDot> Enables { get; init; }

    /// <summary>
    /// The disable (remove) dots added since the receiver's cursor. An empty
    /// list indicates a delta that contains no new removes.
    /// </summary>
    [Id(1)] public IReadOnlyList<OrSetDot> Disables { get; init; }

    /// <summary>
    /// The disable dots an enable has now observed-as-cancelled. An empty
    /// list indicates a delta whose enables cancelled no prior disable.
    /// </summary>
    [Id(2)] public IReadOnlyList<OrSetDot> Tombstones { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null) <see cref="Enables"/>,
    /// <see cref="Disables"/>, and <see cref="Tombstones"/> collections.
    /// Backed by <see cref="Array.Empty{T}"/> so repeated access does not
    /// allocate.
    /// </summary>
    public static RwFlagDelta Empty { get; } = new()
    {
        Enables = Array.Empty<OrSetDot>(),
        Disables = Array.Empty<OrSetDot>(),
        Tombstones = Array.Empty<OrSetDot>(),
    };

    /// <summary>
    /// Compares two deltas by value, with <see cref="Enables"/>,
    /// <see cref="Disables"/>, and <see cref="Tombstones"/> compared
    /// element-by-element using the value equality of <see cref="OrSetDot"/>.
    /// The compiler-generated record-struct equality compares the collection
    /// references with <see cref="EqualityComparer{T}.Default"/>, so two
    /// structurally identical deltas built from independently allocated
    /// collections - and, in particular, a delta and its post-serialization
    /// self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The delta to compare against.</param>
    public bool Equals(RwFlagDelta other) =>
        ListEqual(Enables, other.Enables)
        && ListEqual(Disables, other.Disables)
        && ListEqual(Tombstones, other.Tombstones);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        AddList(ref hash, Enables);
        AddList(ref hash, Disables);
        AddList(ref hash, Tombstones);
        return hash.ToHashCode();
    }

    private static bool ListEqual(IReadOnlyList<OrSetDot>? left, IReadOnlyList<OrSetDot>? right)
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

    private static void AddList(ref HashCode hash, IReadOnlyList<OrSetDot>? list)
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
