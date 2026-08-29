namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// The deterministic split of an ordered strip of items into the ones rendered
/// inline and the ones that move into an overflow menu, used by both the
/// adaptive tab strip and the compact navigation bar so the two behave
/// identically.
/// </summary>
/// <remarks>
/// <para>
/// The rule is: keep the first <c>capacity</c> items inline, except that the
/// active item is always inline. When the active item would fall outside the
/// inline window it is <em>promoted</em> into the last inline slot, displacing
/// the item that would otherwise have sat there. This is what makes a collapsed
/// strip usable: the caller can always see where they are, and the strip never
/// scrolls the active item off-screen.
/// </para>
/// <para>
/// The split is a <see langword="readonly" /> <see langword="record" />
/// <see langword="struct" /> of four <see langword="int" />-sized fields and
/// every query on it is branch-only arithmetic, so a render loop can ask
/// <see cref="IsInline"/> once per item without allocating.
/// </para>
/// </remarks>
public readonly record struct LatticeOverflowLayout
{
    private LatticeOverflowLayout(int totalCount, int inlineCapacity, int activeIndex, bool promotesActive)
    {
        TotalCount = totalCount;
        InlineCapacity = inlineCapacity;
        ActiveIndex = activeIndex;
        PromotesActive = promotesActive;
    }

    /// <summary>The number of items in the strip.</summary>
    public int TotalCount { get; }

    /// <summary>
    /// The number of inline slots available. Never less than one for a
    /// non-empty strip, and never more than <see cref="TotalCount"/>.
    /// </summary>
    public int InlineCapacity { get; }

    /// <summary>
    /// The index of the active item, or <c>-1</c> when nothing is active (in
    /// which case no item is promoted).
    /// </summary>
    public int ActiveIndex { get; }

    /// <summary>
    /// Whether the active item was promoted into the last inline slot because
    /// it sat beyond the inline window.
    /// </summary>
    public bool PromotesActive { get; }

    /// <summary>
    /// The number of items rendered inline: every item when the strip fits, and
    /// <see cref="InlineCapacity"/> otherwise.
    /// </summary>
    public int InlineCount => HasOverflow ? InlineCapacity : TotalCount;

    /// <summary>
    /// Whether any item was displaced into the overflow menu. False when the
    /// whole strip fits inline, in which case no overflow control is rendered.
    /// </summary>
    public bool HasOverflow => TotalCount > InlineCapacity;

    /// <summary>
    /// An empty layout: no items, no overflow. This is the value of
    /// <see langword="default" /> and the result of resolving an empty strip.
    /// </summary>
    public static LatticeOverflowLayout Empty => default;

    /// <summary>
    /// Splits a strip of <paramref name="totalCount"/> items into the inline
    /// window and the overflow remainder.
    /// </summary>
    /// <param name="totalCount">
    /// The number of items in the strip. A negative count is treated as empty.
    /// </param>
    /// <param name="activeIndex">
    /// The index of the active item, or any out-of-range value (for example
    /// <c>-1</c>) when nothing is active.
    /// </param>
    /// <param name="inlineCapacity">
    /// The number of inline slots. Clamped to at least one for a non-empty
    /// strip, so a caller can pass a computed capacity without guarding it, and
    /// clamped down to <paramref name="totalCount"/> so the layout never claims
    /// more inline slots than there are items.
    /// </param>
    /// <returns>The resolved layout.</returns>
    public static LatticeOverflowLayout Resolve(int totalCount, int activeIndex, int inlineCapacity)
    {
        if (totalCount <= 0)
        {
            return Empty;
        }

        var capacity = inlineCapacity < 1 ? 1 : inlineCapacity;
        if (capacity > totalCount)
        {
            capacity = totalCount;
        }

        var active = activeIndex >= 0 && activeIndex < totalCount ? activeIndex : -1;

        // The active item is promoted only when the strip actually overflows
        // and the active item sits beyond the inline window.
        var promotes = totalCount > capacity && active >= capacity;

        return new LatticeOverflowLayout(totalCount, capacity, active, promotes);
    }

    /// <summary>
    /// Whether the item at <paramref name="index"/> is rendered inline. An
    /// index outside the strip is never inline.
    /// </summary>
    /// <param name="index">The zero-based index of the item.</param>
    /// <returns><see langword="true"/> when the item is rendered inline.</returns>
    public bool IsInline(int index)
    {
        if (index < 0 || index >= TotalCount)
        {
            return false;
        }

        if (!HasOverflow)
        {
            return true;
        }

        // With the active item promoted, the last inline slot belongs to it and
        // the leading slots hold the first (capacity - 1) items.
        return PromotesActive
            ? index == ActiveIndex || index < InlineCapacity - 1
            : index < InlineCapacity;
    }

    /// <summary>
    /// Whether the item at <paramref name="index"/> was displaced into the
    /// overflow menu. Exactly the negation of <see cref="IsInline"/> for an
    /// in-range index.
    /// </summary>
    /// <param name="index">The zero-based index of the item.</param>
    /// <returns><see langword="true"/> when the item is only reachable through the overflow menu.</returns>
    public bool IsOverflowed(int index) => index >= 0 && index < TotalCount && !IsInline(index);
}
