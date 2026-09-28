using System.Runtime.InteropServices;

namespace Orleans.Lattice;

/// <summary>
/// Resolves a contiguous <see cref="ReadOnlySpan{T}"/> over a CRDT delta's
/// <see cref="IReadOnlyList{T}"/>-typed dot and entry collections, so the hot
/// walks over them are ordinary array walks rather than interface calls.
/// <para>
/// The delta DTOs declare their collections as <see cref="IReadOnlyList{T}"/>
/// because they are part of the serialised public surface and cannot name a
/// concrete container. Every element type they carry is a <b>struct</b>, so a
/// walk through the interface indexer costs an interface dispatch per element
/// that returns the whole element by value - strictly dearer than the
/// <c>List&lt;T&gt;</c> indexer the dot scans elsewhere in this assembly have
/// already been moved off. <see cref="CollectionsMarshal.AsSpan{T}"/> cannot
/// help on its own because it demands a concrete <see cref="List{T}"/>.
/// </para>
/// <para>
/// In practice the collections are never an exotic implementation: the delta
/// emitters mint <c>T[]</c> (including <see cref="Array.Empty{T}"/> for the
/// no-op shapes), and the coalescing fold mints <see cref="List{T}"/>. Both are
/// spannable, so both shapes are recognised here - an array-only or
/// <see cref="List{T}"/>-only test would silently leave the more common half of
/// the call sites on the interface walk. Anything else - a caller-supplied
/// collection, a deserialiser that chooses another container - is reported as
/// unspannable so the caller can keep its interface walk, which stays correct
/// and is the shape that shipped before.
/// </para>
/// <para>
/// <b>The precondition is the same one the list-span walks carry:</b> no loop
/// may change the underlying collection's length while the span is alive. Every
/// caller here appends to a different collection than the one it scans.
/// </para>
/// </summary>
internal static class CrdtDeltaListSpan
{
    /// <summary>
    /// Resolves <paramref name="list"/> to a span when its runtime type is
    /// contiguous. A <see langword="null"/> list resolves to an empty span and
    /// reports success, because an absent collection and an empty one are the
    /// same walk.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="list">The collection to span; may be <see langword="null"/>.</param>
    /// <param name="span">The resolved span, or an empty span when the list is unspannable.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="span"/> covers the whole
    /// list and the caller may walk it; <see langword="false"/> when the caller
    /// must fall back to the <see cref="IReadOnlyList{T}"/> indexer.
    /// </returns>
    internal static bool TryGetSpan<T>(IReadOnlyList<T>? list, out ReadOnlySpan<T> span)
    {
        switch (list)
        {
            case null:
                span = default;
                return true;
            case T[] array:
                span = array;
                return true;
            case List<T> concrete:
                span = CollectionsMarshal.AsSpan(concrete);
                return true;
            default:
                span = default;
                return false;
        }
    }
}
