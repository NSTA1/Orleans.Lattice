namespace Orleans.Lattice;

/// <summary>
/// Element-wise value equality and hashing for the <see cref="IReadOnlyList{T}"/>
/// collections the CRDT delta records carry. A record's compiler-generated
/// equality compares a collection member by reference, so two structurally
/// identical deltas built from independently allocated collections - and, in
/// particular, a delta and its post-serialization self - would otherwise never
/// compare equal.
/// <para>
/// Both members are generic over an element constrained to
/// <see cref="IEquatable{T}"/>. Every element the deltas carry is a struct, so the
/// JIT specialises them per element type and each comparison and hash binds
/// directly to the element's own <see cref="IEquatable{T}.Equals(T)"/> with no
/// boxing.
/// </para>
/// </summary>
internal static class CrdtDeltaListEquality
{
    /// <summary>
    /// Returns <see langword="true"/> when both lists are the same instance, both
    /// are <see langword="null"/>, or both hold equal elements in the same order.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="left">The first list; may be <see langword="null"/>.</param>
    /// <param name="right">The second list; may be <see langword="null"/>.</param>
    internal static bool ListEqual<T>(IReadOnlyList<T>? left, IReadOnlyList<T>? right)
        where T : IEquatable<T>
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

    /// <summary>
    /// Folds <paramref name="list"/> into <paramref name="hash"/> consistently with
    /// <see cref="ListEqual{T}"/>: its count followed by every element in order, or
    /// a single <c>0</c> for a <see langword="null"/> list.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="hash">The hash being accumulated.</param>
    /// <param name="list">The list to fold in; may be <see langword="null"/>.</param>
    internal static void AddList<T>(ref HashCode hash, IReadOnlyList<T>? list)
        where T : IEquatable<T>
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
