using System.Runtime.CompilerServices;

namespace Orleans.Lattice;

/// <summary>
/// Typed extension methods for reading from an <see cref="ILatticeView"/> that
/// deserialize materialised values via an <see cref="ILatticeSerializer{T}"/>,
/// eliminating per-caller <c>byte[]</c> boilerplate on the view read path - the
/// read-side counterpart to the typed projection factories
/// (<see cref="PredicateLatticeViewProjection.Create{T}"/> /
/// <see cref="AggregationLatticeViewProjection.Create{T}"/>) and to
/// <c>TypedLatticeExtensions</c> for <see cref="ILattice"/>. Each typed method
/// has two overloads: one accepting an explicit serializer and one that defaults
/// to <see cref="JsonLatticeSerializer{T}"/>.
/// <para>
/// For <b>aggregation</b> views the materialised value is a fixed-width numeric
/// (not a serialized POCO), so use <see cref="GetAggregateDoubleAsync"/> /
/// <see cref="GetAggregateInt64Async"/>, which decode through
/// <see cref="LatticeAggregationValue"/> for the view's
/// <see cref="AggregationKind"/>.
/// </para>
/// </summary>
public static class TypedLatticeViewExtensions
{
    /// <summary>
    /// Gets the deserialized view value for <paramref name="key"/>, or
    /// <c>default</c> when the key is absent from the view.
    /// </summary>
    public static async Task<T?> GetAsync<T>(this ILatticeView view, string key, ILatticeSerializer<T> serializer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(view);
        ArgumentNullException.ThrowIfNull(serializer);
        var bytes = await view.GetAsync(key, cancellationToken);
        return bytes is null ? default : serializer.Deserialize(bytes);
    }

    /// <inheritdoc cref="GetAsync{T}(ILatticeView, string, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task<T?> GetAsync<T>(this ILatticeView view, string key, CancellationToken cancellationToken = default) =>
        view.GetAsync(key, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Streams the view's live entries in lexicographic key order over the
    /// optional range, deserializing each value to <typeparamref name="T"/>.
    /// </summary>
    public static async IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILatticeView view,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(view);
        ArgumentNullException.ThrowIfNull(serializer);
        await foreach (var entry in view.EntriesAsync(startInclusive, endExclusive, cancellationToken))
        {
            yield return new KeyValuePair<string, T>(entry.Key, serializer.Deserialize(entry.Value));
        }
    }

    /// <inheritdoc cref="EntriesAsync{T}(ILatticeView, ILatticeSerializer{T}, string, string, CancellationToken)"/>
    public static IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILatticeView view,
        string? startInclusive = null,
        string? endExclusive = null,
        CancellationToken cancellationToken = default) =>
        view.EntriesAsync(JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, cancellationToken);

    /// <summary>
    /// Resilient typed entry scan over a view. Composes
    /// <see cref="LatticeViewExtensions.ScanEntriesAsync(ILatticeView, string?, string?, int?, CancellationToken)"/>
    /// with <paramref name="serializer"/>, so typed view exports automatically
    /// recover from <c>Orleans.Runtime.EnumerationAbortedException</c> without
    /// duplicates or gaps. This is the recommended client API for long-running
    /// typed view scans; the raw <see cref="EntriesAsync{T}(ILatticeView, ILatticeSerializer{T}, string, string, CancellationToken)"/>
    /// primitive is retained for deliberate low-level use.
    /// </summary>
    /// <param name="view">The view to scan.</param>
    /// <param name="serializer">Value deserializer.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the view's lowest (non-reserved) key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the view's end.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget; defaults to <see cref="LatticeExtensions.DefaultScanReconnectAttempts"/>.</param>
    /// <param name="cancellationToken">Cancellation token; honoured between reconnects and during backoff.</param>
    public static async IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(
        this ILatticeView view,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        int? maxAttempts = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(view);
        ArgumentNullException.ThrowIfNull(serializer);
        await foreach (var entry in view.ScanEntriesAsync(startInclusive, endExclusive, maxAttempts, cancellationToken).ConfigureAwait(false))
        {
            yield return new KeyValuePair<string, T>(entry.Key, serializer.Deserialize(entry.Value));
        }
    }

    /// <inheritdoc cref="ScanEntriesAsync{T}(ILatticeView, ILatticeSerializer{T}, string?, string?, int?, CancellationToken)"/>
    public static IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(
        this ILatticeView view,
        string? startInclusive = null,
        string? endExclusive = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default) =>
        view.ScanEntriesAsync(JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, maxAttempts, cancellationToken);

    /// <summary>
    /// Reads a <see cref="double"/> aggregate (a <see cref="AggregationKind.Sum"/>,
    /// <see cref="AggregationKind.Min"/>, or <see cref="AggregationKind.Max"/>
    /// view's materialised group value) for <paramref name="groupKey"/>, decoded
    /// through <see cref="LatticeAggregationValue.DecodeDouble"/>. Returns
    /// <see langword="null"/> when the group has no live members.
    /// </summary>
    public static async Task<double?> GetAggregateDoubleAsync(this ILatticeView view, string groupKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(view);
        var bytes = await view.GetAsync(groupKey, cancellationToken);
        return bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);
    }

    /// <summary>
    /// Reads an <see cref="long"/> aggregate (a <see cref="AggregationKind.Count"/>
    /// or <see cref="AggregationKind.SetUnion"/> view's materialised group value)
    /// for <paramref name="groupKey"/>, decoded through
    /// <see cref="LatticeAggregationValue.DecodeInt64"/>. Returns
    /// <see langword="null"/> when the group has no live members.
    /// </summary>
    public static async Task<long?> GetAggregateInt64Async(this ILatticeView view, string groupKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(view);
        var bytes = await view.GetAsync(groupKey, cancellationToken);
        return bytes is null ? null : LatticeAggregationValue.DecodeInt64(bytes);
    }
}
