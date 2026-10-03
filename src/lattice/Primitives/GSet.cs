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
    public HashSet<string> Elements { get; set; }

    /// <summary>Creates an empty grow-only set.</summary>
    public GSet() => Elements = [];

    // Direct-assign constructor for the clone/merge fast paths: takes ownership
    // of an already-built backing store so the factory allocates no discarded
    // empty-collection shell from a field initializer that an object
    // initializer would immediately overwrite. Mirrors OrSet / GCounter.
    private GSet(HashSet<string> elements) => Elements = elements;

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
        var count = Elements.Count;
        if (count == 0) yield break;

        // An exactly-sized array rather than a List: HashSet.CopyTo fills it
        // in one pass with no List wrapper object, and indexing it drops the
        // per-element version check a List enumerator pays on every MoveNext.
        // Order is unchanged - the same ordinal sort of the same base64 keys.
        var keys = new string[count];
        Elements.CopyTo(keys);
        Array.Sort(keys, OrdinalStringOrder.Comparison);
        for (var i = 0; i < count; i++)
        {
            yield return Convert.FromBase64String(keys[i]);
        }
    }

    /// <summary>
    /// The eager counterpart to <see cref="Values"/>: the identical
    /// deterministic projection, materialised once into an exactly-sized
    /// array. <see cref="Count"/> is exact, so the destination is sized up
    /// front and the decoded elements are written straight into it.
    /// <para>
    /// Prefer this over <c>[.. set.Values()]</c> or <c>set.Values().ToArray()</c>
    /// on a read path that materialises the whole set. <see cref="Values"/> is a
    /// <c>yield return</c> iterator, so it hides its element count from the
    /// materialiser: the builder cannot size the destination, and instead fills
    /// a chain of segments and copies the whole projection once more into the
    /// final array - on top of the iterator state machine itself. The segments
    /// are pooled, so the cost is the bookkeeping, the extra copy, and the state
    /// machine rather than heap bytes. Sizing the destination up front pays none
    /// of it.
    /// </para>
    /// </summary>
    internal byte[][] SnapshotValues()
    {
        var count = Elements.Count;
        if (count == 0) return Array.Empty<byte[]>();

        var keys = new string[count];
        Elements.CopyTo(keys);
        Array.Sort(keys, OrdinalStringOrder.Comparison);

        var values = new byte[count][];
        for (var i = 0; i < count; i++)
        {
            values[i] = Convert.FromBase64String(keys[i]);
        }
        return values;
    }

    /// <summary>
    /// Lattice merge: the set union of <paramref name="left"/> and
    /// <paramref name="right"/>. Commutative, associative, idempotent.
    /// </summary>
    public static GSet Merge(GSet left, GSet right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        // Seed the union from the left operand through the HashSet copy
        // constructor under an ordinally-equivalent comparer, so the runtime
        // bulk-copies the backing bucket and entry arrays instead of hashing
        // every element again. The previous form built an empty presized set and
        // filled it with UnionWith(left), which re-hashed all of left's strings
        // on every merge - work that is pure overhead, because the source set
        // has already stored each element's hash code.
        //
        // That copy inherits the source set's capacity rather than sizing to the
        // combined count, so it is only the cheaper shape while the right
        // operand is narrow enough not to force the result to grow past it. The
        // dominant replication case is exactly that: idempotent delivery, where
        // right is a subset of left and nothing is appended at all. A wide
        // right operand takes the verbatim presized arm below, which sizes the
        // result once and allocates no more than it did before. Mirrors the same
        // comparer-preserving copy in MvRegister.Clone.
        var leftElements = left.Elements;
        var rightElements = right.Elements;
        return new GSet(
            rightElements.Count * CopyMergeWidthRatio <= leftElements.Count
                ? MergeByCopyThenUnion(leftElements, rightElements)
                : MergeByPresizedUnion(leftElements, rightElements));
    }

    /// <summary>
    /// How many times wider than the right operand the left operand must be for
    /// the copy-then-union merge arm to be worth its inherited capacity. Below
    /// it the union is built presized, exactly as every merge once was.
    /// </summary>
    private const int CopyMergeWidthRatio = 4;

    /// <summary>
    /// Narrow-right arm: bulk-copy the left operand's backing store, then append
    /// the few elements the right operand contributes.
    /// </summary>
    private static HashSet<string> MergeByCopyThenUnion(HashSet<string> left, HashSet<string> right)
    {
        var union = new HashSet<string>(left, OrdinalEquivalent(left.Comparer));
        if (right.Count > 0) union.UnionWith(right);
        return union;
    }

    /// <summary>
    /// Wide-right arm: size the union to the combined upper bound once and fill
    /// it from both operands.
    /// </summary>
    private static HashSet<string> MergeByPresizedUnion(HashSet<string> left, HashSet<string> right)
    {
        var union = new HashSet<string>(left.Count + right.Count, StringComparer.Ordinal);
        union.UnionWith(left);
        union.UnionWith(right);
        return union;
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
    public GSet Clone() =>
        // Copy through the source set's own comparer when it is already
        // ordinally equivalent, so the HashSet copy constructor bulk-copies the
        // backing store instead of rehashing every element. Passing a
        // reference-distinct comparer - which StringComparer.Ordinal is, against
        // a set built with the default string comparer - defeats that fast path
        // and costs one string hash per element on every clone. Anything that is
        // not ordinally equivalent still normalises to StringComparer.Ordinal,
        // so the clone's membership semantics are unchanged. The direct-assign
        // constructor takes the copy as-is, so the clone allocates exactly one
        // set with no discarded empty-collection shell.
        new(new HashSet<string>(Elements, OrdinalEquivalent(Elements.Comparer)));

    /// <summary>
    /// The comparer to copy <paramref name="sourceComparer"/>'s set under:
    /// itself when it is already ordinal (so the copy constructor's bulk-copy
    /// fast path applies), otherwise the ordinal normalisation every GSet
    /// factory applies. <see cref="EqualityComparer{T}.Default"/> for
    /// <see cref="string"/> is ordinal, so preserving it changes no membership.
    /// </summary>
    private static IEqualityComparer<string> OrdinalEquivalent(IEqualityComparer<string> sourceComparer)
        => ReferenceEquals(sourceComparer, EqualityComparer<string>.Default)
            ? sourceComparer
            : StringComparer.Ordinal;

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
