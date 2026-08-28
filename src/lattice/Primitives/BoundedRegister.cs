namespace Orleans.Lattice;

/// <summary>
/// A monotonic bounded register CRDT: a single totally-ordered value that only
/// ever moves in one direction. The direction is carried durably on the state
/// (<see cref="IsMin"/>): a <c>Max</c> register keeps the greatest value ever
/// seen (a high-water mark, a monotone gauge, a version ceiling); a <c>Min</c>
/// register keeps the smallest (a min-seen latency floor). One core primitive
/// serves both directions; the typed accessors <see cref="MaxRegisterAccessor{T}"/>
/// and <see cref="MinRegisterAccessor{T}"/> pick the direction and supply the
/// order-preserving key.
/// <para>
/// State shape: a single opaque <see cref="Value"/> paired with an explicit
/// total-order <see cref="OrderKey"/>. The order key is carried on the wire
/// alongside the value so a receiver folds the register with a fixed
/// lexicographic byte comparison and never needs the domain comparer. The
/// producer is responsible for authoring an order-preserving key (the typed
/// accessor derives it through an order-key selector).
/// </para>
/// <para>
/// <see cref="Merge(BoundedRegister, BoundedRegister)"/> (and the in-place
/// <see cref="MergeFrom(BoundedRegister)"/> / <see cref="MergeDelta(BoundedRegisterDelta)"/>)
/// is the directional fold: keep the value whose order key is the greatest
/// (<c>Max</c>) or least (<c>Min</c>) under the total order, tie-breaking on the
/// value bytes so the result is deterministic. Because the fold is max/min over a
/// total order it is commutative, associative, and idempotent (a total-order
/// semilattice): backwards writes and duplicate deliveries are no-ops.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.BoundedRegister)]
public sealed class BoundedRegister : ICrdt<BoundedRegister>
{
    /// <summary>
    /// The register's current opaque value bytes, or <see langword="null"/> when
    /// the register has never been written (<see cref="IsBottom"/>). An empty
    /// array is a valid written value distinct from the never-written state
    /// (see <see cref="HasValue"/>).
    /// </summary>
    [Id(0)]
    public byte[]? Value { get; set; }

    /// <summary>
    /// The total-order key for <see cref="Value"/>: an order-preserving byte
    /// string authored by the producer. Two registers fold by comparing this
    /// key lexicographically (unsigned byte order), so the receiver resolves the
    /// merge without the domain comparer. <see langword="null"/> only when the
    /// register has never been written.
    /// </summary>
    [Id(1)]
    public byte[]? OrderKey { get; set; }

    /// <summary>
    /// <see langword="true"/> once the register has been written at least once.
    /// Distinguishes a never-written register from one written with an empty
    /// value, so a legitimately empty value is not mistaken for bottom.
    /// </summary>
    [Id(2)]
    public bool HasValue { get; set; }

    /// <summary>
    /// The fold direction. <see langword="false"/> keeps the greatest value
    /// (a <c>Max</c> register); <see langword="true"/> keeps the smallest (a
    /// <c>Min</c> register). Carried on the state so the direction is durable on
    /// the wire and a receiver folds without a separate mode lookup.
    /// <para>
    /// This field is a <em>cache of</em>, not the authority for, the direction.
    /// The authority is the registered <see cref="LatticeMergeMode"/> for the key
    /// (<see cref="LatticeMergeMode.MaxRegister"/> or
    /// <see cref="LatticeMergeMode.MinRegister"/>), which is what the tree
    /// dispatches on. Every decode seam re-stamps this field from that mode via
    /// <see cref="WithDirection(bool)"/>, so a payload persisted or supplied with
    /// the wrong direction - a raw <c>SetAsync</c> of hand-written state, or a
    /// foreign writer - self-heals on read instead of silently folding backwards
    /// forever. Keep the setter: the stamp is an in-place write on a
    /// just-decoded instance, which costs no allocation.
    /// </para>
    /// </summary>
    [Id(3)]
    public bool IsMin { get; set; }

    /// <summary>
    /// Initialises an empty (never-written) <c>Max</c> register. Present for the
    /// serializer and the empty-slot constructor; author a directional empty
    /// register with <see cref="CreateEmpty(bool)"/>.
    /// </summary>
    public BoundedRegister()
    {
    }

    /// <summary>Initialises an empty (never-written) register in the given direction.</summary>
    /// <param name="isMin"><see langword="true"/> for a <c>Min</c> register; <see langword="false"/> for <c>Max</c>.</param>
    public BoundedRegister(bool isMin) => IsMin = isMin;

    /// <summary>Creates an empty register in the given direction.</summary>
    /// <param name="isMin"><see langword="true"/> for a <c>Min</c> register; <see langword="false"/> for <c>Max</c>.</param>
    public static BoundedRegister CreateEmpty(bool isMin) => new(isMin);

    /// <summary>
    /// Stamps <see cref="IsMin"/> from the authoritative registered merge mode
    /// and returns this same instance, so a decode seam can stamp inline without
    /// allocating a second register.
    /// <para>
    /// Direction is authored in exactly one place - the
    /// <see cref="LatticeMergeMode"/> registered for the key - and
    /// <see cref="IsMin"/> on the state is only a wire-carried cache of it. The
    /// two can disagree whenever state reaches the store without going through a
    /// directional accessor (a raw byte write of hand-authored JSON, a payload
    /// from a foreign or older writer, or a key whose mode was re-registered).
    /// Left unstamped, such a payload folds under the wrong direction
    /// indefinitely and silently: <see cref="MergeFrom(BoundedRegister)"/>
    /// resolves under the <em>receiver's</em> direction and never inspects the
    /// other side's, so nothing detects the disagreement. Re-stamping on decode
    /// makes the registered mode win, which is what the rest of the tree already
    /// treats as authoritative.
    /// </para>
    /// </summary>
    /// <param name="isMin">The direction carried by the registered merge mode.</param>
    /// <returns>This instance, with <see cref="IsMin"/> set to <paramref name="isMin"/>.</returns>
    internal BoundedRegister WithDirection(bool isMin)
    {
        IsMin = isMin;
        return this;
    }

    /// <inheritdoc />
    /// <remarks>
    /// A <see cref="BoundedRegister"/> is bottom when it has never been written
    /// (<see cref="HasValue"/> is <see langword="false"/>), so a containing
    /// composite treats the slot as absent.
    /// </remarks>
    public bool IsBottom => !HasValue;

    /// <summary>
    /// Advances the register to <paramref name="value"/> with total-order key
    /// <paramref name="orderKey"/> only when the candidate beats the current
    /// value under the configured direction (a strictly greater key for a
    /// <c>Max</c> register, a strictly lesser key for a <c>Min</c> register,
    /// tie-broken on the value bytes). A write that would move the register
    /// backwards is a no-op. Returns <see langword="true"/> when the register
    /// advanced.
    /// </summary>
    /// <param name="value">
    /// The candidate value bytes. Must not be <see langword="null"/>. Stored by
    /// reference: <c>Set</c> is a hand-off, so the register takes ownership of
    /// the array and the caller must not mutate it afterwards. (The merge and
    /// delta-apply paths, which read an array the other side still owns, copy
    /// instead - see <see cref="FoldCandidate"/>.)
    /// </param>
    /// <param name="orderKey">
    /// The candidate's order-preserving total-order key. Must not be
    /// <see langword="null"/>. Stored by reference on the same hand-off basis as
    /// <paramref name="value"/>.
    /// </param>
    public bool Set(byte[] value, byte[] orderKey)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(orderKey);

        if (!HasValue || Beats(orderKey, value, OrderKey!, Value!))
        {
            Value = value;
            OrderKey = orderKey;
            HasValue = true;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Lattice merge: keeps the value whose order key wins under the direction of
    /// <paramref name="left"/>. Commutative, associative, idempotent.
    /// </summary>
    public static BoundedRegister Merge(BoundedRegister left, BoundedRegister right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: folds <paramref name="other"/>'s value into this
    /// register under this register's direction. Equivalent to
    /// <see cref="Merge(BoundedRegister, BoundedRegister)"/> followed by replacing
    /// the receiver, but avoids the intermediate clone. Commutative, associative,
    /// and idempotent.
    /// </summary>
    public void MergeFrom(BoundedRegister other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (!other.HasValue) return;
        FoldCandidate(other.Value!, other.OrderKey!);
    }

    /// <summary>
    /// Creates a deep, independent copy of this register, per the
    /// <see cref="ICrdt{TSelf}.Clone"/> contract: the value and order-key byte
    /// arrays are copied, not shared.
    /// <para>
    /// The arrays are treated as immutable <em>inside</em> this type, but that
    /// invariant stops at its boundary. A caller that reads a register out of an
    /// <c>OrMap&lt;string, BoundedRegister&gt;</c> gets it through
    /// <c>OrMap.Get</c>, which hands back <c>Clone()</c>; sharing the arrays
    /// there gives the caller a live handle on the map's durable state, so a
    /// write through the returned <c>Value</c> corrupts the stored CRDT without
    /// going through any mutation API. The two array copies are the price of
    /// the contract - the same trade
    /// <see cref="OrMap{TKey, TValue}.Clone"/> already pays for its nested value
    /// snapshots. They are expressed as span copies rather than
    /// <see cref="Array.Clone"/>, which allocates identically but goes through
    /// the non-generic <see cref="Array"/> path and measured roughly 3-4x
    /// slower on the <c>ordedup</c> microbench suite.
    /// </para>
    /// </summary>
    public BoundedRegister Clone() => new()
    {
        Value = Value?.AsSpan().ToArray(),
        OrderKey = OrderKey?.AsSpan().ToArray(),
        HasValue = HasValue,
        IsMin = IsMin,
    };

    /// <summary>
    /// Folds a <see cref="BoundedRegisterDelta"/> into this register: the delta's
    /// candidate value advances the register only when it beats the current value
    /// under the configured direction. The fold is commutative, associative, and
    /// idempotent against arrival order and duplicate delivery - applying the same
    /// delta twice yields the same state because a candidate that has already won
    /// (or lost) does not move the register again.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. A delta with no
    /// candidate (<see cref="BoundedRegisterDelta.HasValue"/> is
    /// <see langword="false"/>) is a no-op.
    /// </param>
    public void MergeDelta(BoundedRegisterDelta delta)
    {
        if (!delta.HasValue) return;
        FoldCandidate(delta.Value ?? Array.Empty<byte>(), delta.OrderKey ?? Array.Empty<byte>());
    }

    /// <summary>
    /// Folds a candidate authored by another register or by a delta. The
    /// candidate advances this register only when it beats the current value
    /// under the configured direction.
    /// <para>
    /// A winning candidate is copied rather than adopted: the array belongs to
    /// the other side (a peer register that keeps using it, or a delta the
    /// producer may still hold for retry or fan-out), so retaining it would
    /// leave this register's durable state aliased to somebody else's buffer.
    /// A losing candidate allocates nothing.
    /// </para>
    /// </summary>
    private void FoldCandidate(byte[] candidateValue, byte[] candidateOrderKey)
    {
        if (!HasValue || Beats(candidateOrderKey, candidateValue, OrderKey!, Value!))
        {
            Value = candidateValue.AsSpan().ToArray();
            OrderKey = candidateOrderKey.AsSpan().ToArray();
            HasValue = true;
        }
    }

    private bool Beats(byte[] candidateOrderKey, byte[] candidateValue, byte[] currentOrderKey, byte[] currentValue)
    {
        var cmp = CompareTotal(candidateOrderKey, candidateValue, currentOrderKey, currentValue);
        return IsMin ? cmp < 0 : cmp > 0;
    }

    private static int CompareTotal(byte[] leftKey, byte[] leftValue, byte[] rightKey, byte[] rightValue)
    {
        var byKey = ((ReadOnlySpan<byte>)leftKey).SequenceCompareTo(rightKey);
        return byKey != 0 ? byKey : ((ReadOnlySpan<byte>)leftValue).SequenceCompareTo(rightValue);
    }
}
