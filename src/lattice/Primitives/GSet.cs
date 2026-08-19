using System.Buffers;

namespace Orleans.Lattice;

/// <summary>
/// A grow-only (G) set CRDT: a set of opaque <c>byte[]</c> elements with
/// value-equality by content. <see cref="Add(byte[])"/> inserts an element
/// (idempotent); state-level <see cref="Merge(GSet, GSet)"/> is the set
/// <em>union</em> of both replicas' elements, which is trivially commutative,
/// associative, and idempotent under arbitrary delivery order.
/// <para>
/// The set is <strong>grow-only by design</strong>: it carries no dots and no
/// tombstones and exposes no remove operation. When an element must ever be
/// removed, reach for <see cref="OrSet"/> (add-wins observed-remove) or the
/// remove-wins set instead - a grow-only set cannot represent a removal and a
/// merge could never converge one away.
/// </para>
/// <para>
/// Element identity is by content (byte equality), encoded internally as a
/// base64 string for serialization stability. Empty arrays are valid elements;
/// <c>null</c> is rejected.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GSet)]
public sealed class GSet : ICrdt<GSet>
{
    // Elements whose base64 encoding fits in this many chars are keyed through
    // a stack buffer; larger elements rent from the shared pool. 256 chars
    // covers elements up to 192 bytes with no allocation. Mirrors
    // OrSet.MaxStackBase64Chars.
    private const int MaxStackBase64Chars = 256;

    private static int Base64CharCount(int byteCount) => checked((byteCount + 2) / 3 * 4);

    /// <summary>
    /// The set elements, keyed by the base64 encoding of the element bytes. An
    /// element is a member of the set if and only if its base64 key is present.
    /// </summary>
    [Id(0)]
    public HashSet<string> Elements { get; set; } = [];

    /// <summary>Returns <c>true</c> when the set contains no elements.</summary>
    public bool IsEmpty => Elements.Count == 0;

    /// <inheritdoc />
    /// <remarks>
    /// A <see cref="GSet"/> is bottom when it is empty - it carries no live
    /// state - so a containing composite (e.g.
    /// <see cref="OrMap{TKey, TValue}"/>) treats the slot as absent.
    /// </remarks>
    public bool IsBottom => IsEmpty;

    /// <summary>Returns the number of elements in the set.</summary>
    public int Count => Elements.Count;

    /// <summary>
    /// Adds <paramref name="element"/> to the set. Idempotent: adding an
    /// element already present is a no-op. Returns <c>true</c> when the element
    /// was not already present.
    /// </summary>
    /// <param name="element">The element bytes to add. Must not be <c>null</c>.</param>
    public bool Add(byte[] element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            var key = buffer[..written];
            // Single-probe insert: the span alternate-lookup hashes the base64
            // key once and materialises the string only when the element is
            // genuinely new (returning true), so a re-add allocates nothing and
            // hits the set exactly once. This avoids the extra Contains probe
            // the previous Contains-then-Add form paid on every add.
            return Elements.GetAlternateLookup<ReadOnlySpan<char>>().Add(key);
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>Returns <c>true</c> when <paramref name="element"/> is a member of the set.</summary>
    /// <param name="element">The element bytes to test. Must not be <c>null</c>.</param>
    public bool Contains(byte[] element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            return Elements.GetAlternateLookup<ReadOnlySpan<char>>().Contains(buffer[..written]);
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Enumerates the elements of the set in deterministic order. Order is the
    /// ordinal sort of each element's base64 encoding (the internal key form),
    /// which is stable across replicas but is not the same as ordering by the
    /// raw element bytes.
    /// </summary>
    public IEnumerable<byte[]> Values()
    {
        if (Elements.Count == 0) yield break;

        var keys = new List<string>(Elements);
        keys.Sort(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            yield return Convert.FromBase64String(key);
        }
    }

    /// <summary>
    /// Lattice merge: the set union of <paramref name="left"/> and
    /// <paramref name="right"/>. Commutative, associative, idempotent.
    /// </summary>
    public static GSet Merge(GSet left, GSet right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        // Build the union presized to the combined element-count upper bound and
        // fill it once, instead of cloning the left operand (a set sized to
        // left.Count) and then growing it through UnionWith - which reallocates
        // the backing store one or more times and discards the clone's arrays.
        // A single presized allocation replaces that clone-then-grow churn; the
        // resulting union is identical.
        var union = new HashSet<string>(left.Elements.Count + right.Elements.Count, StringComparer.Ordinal);
        union.UnionWith(left.Elements);
        union.UnionWith(right.Elements);
        return new GSet { Elements = union };
    }

    /// <summary>
    /// In-place lattice merge: unions <paramref name="other"/>'s elements into
    /// this set. Equivalent to <see cref="Merge(GSet, GSet)"/> followed by
    /// replacing the receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(GSet other)
    {
        ArgumentNullException.ThrowIfNull(other);
        Elements.UnionWith(other.Elements);
    }

    /// <summary>Creates a deep copy of this set.</summary>
    public GSet Clone() => new()
    {
        Elements = new HashSet<string>(Elements, StringComparer.Ordinal),
    };

    /// <summary>
    /// Folds a <see cref="GSetDelta"/> into this set: every element in
    /// <see cref="GSetDelta.Adds"/> is unioned into <see cref="Elements"/>. The
    /// merge is commutative, associative, and idempotent against arrival order
    /// and duplicate delivery - applying the same delta twice yields the same
    /// state because the element set is a union.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. An empty
    /// collection is valid; a <c>null</c> collection is treated as empty.
    /// </param>
    public void MergeDelta(GSetDelta delta)
    {
        var adds = delta.Adds;
        if (adds is not { Count: > 0 }) return;
        for (var i = 0; i < adds.Count; i++)
        {
            var element = adds[i];
            if (element is null) continue;
            Add(element);
        }
    }
}
