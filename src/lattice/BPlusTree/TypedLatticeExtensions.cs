using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed extension methods for <see cref="ILattice"/> that serialize and
/// deserialize values via an <see cref="ILatticeSerializer{T}"/>, eliminating
/// per-caller <c>byte[]</c> boilerplate. Each method has two overloads: one
/// accepting an explicit serializer and one that defaults to
/// <see cref="JsonLatticeSerializer{T}"/>.
/// </summary>
public static class TypedLatticeExtensions
{
    // ── Single-Key ──────────────────────────────────────────────

    /// <summary>Gets the deserialized value for <paramref name="key"/>, or <c>default</c> if not found.</summary>
    public static async Task<T?> GetAsync<T>(this ILattice lattice, string key, ILatticeSerializer<T> serializer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var bytes = await lattice.GetAsync(key, cancellationToken);
        return bytes is null ? default : serializer.Deserialize(bytes);
    }

    /// <inheritdoc cref="GetAsync{T}(ILattice, string, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task<T?> GetAsync<T>(this ILattice lattice, string key, CancellationToken cancellationToken = default) =>
        lattice.GetAsync(key, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Gets the deserialized value and its <see cref="HybridLogicalClock"/> version for
    /// <paramref name="key"/>. Returns a <see cref="Versioned{T}"/> with <c>default</c>
    /// value and <see cref="HybridLogicalClock.Zero"/> version when the key is absent
    /// or tombstoned.
    /// </summary>
    public static async Task<Versioned<T>> GetWithVersionAsync<T>(this ILattice lattice, string key, ILatticeSerializer<T> serializer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var result = await lattice.GetWithVersionAsync(key, cancellationToken);
        return new Versioned<T>
        {
            Value = result.Value is null ? default : serializer.Deserialize(result.Value),
            Version = result.Version
        };
    }

    /// <inheritdoc cref="GetWithVersionAsync{T}(ILattice, string, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task<Versioned<T>> GetWithVersionAsync<T>(this ILattice lattice, string key, CancellationToken cancellationToken = default) =>
        lattice.GetWithVersionAsync(key, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns the existing deserialized value when the
    /// key is already live, or <c>default</c> when the value was newly written.
    /// </summary>
    public static async Task<T?> GetOrSetAsync<T>(this ILattice lattice, string key, T value, ILatticeSerializer<T> serializer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var bytes = await lattice.GetOrSetAsync(key, serializer.Serialize(value), cancellationToken);
        return bytes is null ? default : serializer.Deserialize(bytes);
    }

    /// <inheritdoc cref="GetOrSetAsync{T}(ILattice, string, T, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task<T?> GetOrSetAsync<T>(this ILattice lattice, string key, T value, CancellationToken cancellationToken = default) =>
        lattice.GetOrSetAsync(key, value, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>Serializes <paramref name="value"/> and stores it under <paramref name="key"/>.</summary>
    public static Task SetAsync<T>(this ILattice lattice, string key, T value, ILatticeSerializer<T> serializer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return lattice.SetAsync(key, serializer.Serialize(value), cancellationToken);
    }

    /// <inheritdoc cref="SetAsync{T}(ILattice, string, T, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task SetAsync<T>(this ILattice lattice, string key, T value, CancellationToken cancellationToken = default) =>
        lattice.SetAsync(key, value, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Serializes <paramref name="value"/> and stores it under <paramref name="key"/>
    /// with a time-to-live. See <see cref="ILattice.SetAsync(string, byte[], TimeSpan, CancellationToken)"/>
    /// for expiry semantics.
    /// </summary>
    public static Task SetAsync<T>(this ILattice lattice, string key, T value, TimeSpan ttl, ILatticeSerializer<T> serializer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return lattice.SetAsync(key, serializer.Serialize(value), ttl, cancellationToken);
    }

    /// <inheritdoc cref="SetAsync{T}(ILattice, string, T, TimeSpan, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task SetAsync<T>(this ILattice lattice, string key, T value, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        lattice.SetAsync(key, value, ttl, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the entry's
    /// current <see cref="HybridLogicalClock"/> matches <paramref name="expectedVersion"/>.
    /// Returns <c>true</c> if the write was applied. See <see cref="ILattice.SetIfVersionAsync"/>
    /// for full semantics.
    /// </summary>
    public static Task<bool> SetIfVersionAsync<T>(this ILattice lattice, string key, T value, HybridLogicalClock expectedVersion, ILatticeSerializer<T> serializer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return lattice.SetIfVersionAsync(key, serializer.Serialize(value), expectedVersion, cancellationToken);
    }

    /// <inheritdoc cref="SetIfVersionAsync{T}(ILattice, string, T, HybridLogicalClock, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task<bool> SetIfVersionAsync<T>(this ILattice lattice, string key, T value, HybridLogicalClock expectedVersion, CancellationToken cancellationToken = default) =>
        lattice.SetIfVersionAsync(key, value, expectedVersion, JsonLatticeSerializer<T>.Default, cancellationToken);

    // ── Batch ───────────────────────────────────────────────────

    /// <summary>
    /// Fetches multiple keys and deserializes their values.
    /// Missing/tombstoned keys are omitted from the result.
    /// </summary>
    public static async Task<Dictionary<string, T>> GetManyAsync<T>(
        this ILattice lattice,
        List<string> keys,
        ILatticeSerializer<T> serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = await lattice.GetManyAsync(keys, cancellationToken);
        var result = new Dictionary<string, T>(raw.Count);
        foreach (var (k, v) in raw)
            result[k] = serializer.Deserialize(v);
        return result;
    }

    /// <inheritdoc cref="GetManyAsync{T}(ILattice, List{string}, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task<Dictionary<string, T>> GetManyAsync<T>(
        this ILattice lattice,
        List<string> keys,
        CancellationToken cancellationToken = default) =>
        lattice.GetManyAsync(keys, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Serializes and inserts/updates multiple key-value pairs in parallel across shards.
    /// </summary>
    public static Task SetManyAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        ILatticeSerializer<T> serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = new List<KeyValuePair<string, byte[]>>(entries.Count);
        foreach (var (k, v) in entries)
            raw.Add(new KeyValuePair<string, byte[]>(k, serializer.Serialize(v)));
        return lattice.SetManyAsync(raw, cancellationToken);
    }

    /// <inheritdoc cref="SetManyAsync{T}(ILattice, List{KeyValuePair{string, T}}, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task SetManyAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        CancellationToken cancellationToken = default) =>
        lattice.SetManyAsync(entries, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Serializes and atomically writes multiple key-value pairs via the
    /// saga. See <see cref="ILattice.SetManyAtomicAsync"/> for full semantics
    /// (all-or-nothing commit, partial-visibility window, compensation on failure).
    /// </summary>
    public static Task SetManyAtomicAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        ILatticeSerializer<T> serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = new List<KeyValuePair<string, byte[]>>(entries.Count);
        foreach (var (k, v) in entries)
            raw.Add(new KeyValuePair<string, byte[]>(k, serializer.Serialize(v)));
        return lattice.SetManyAtomicAsync(raw, cancellationToken);
    }

    /// <inheritdoc cref="SetManyAtomicAsync{T}(ILattice, List{KeyValuePair{string, T}}, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task SetManyAtomicAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return lattice.SetManyAtomicAsync(entries, JsonLatticeSerializer<T>.Default, cancellationToken);
    }

    /// <summary>
    /// Caller-supplied idempotency-key overload: serializes and atomically
    /// writes multiple key-value pairs via the saga, keyed by
    /// <paramref name="operationId"/>. See
    /// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, string, CancellationToken)"/>
    /// for the idempotency contract (key-set fingerprint, retention window,
    /// re-attach semantics on retry).
    /// </summary>
    public static Task SetManyAtomicAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        string operationId,
        ILatticeSerializer<T> serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = new List<KeyValuePair<string, byte[]>>(entries.Count);
        foreach (var (k, v) in entries)
            raw.Add(new KeyValuePair<string, byte[]>(k, serializer.Serialize(v)));
        return lattice.SetManyAtomicAsync(raw, operationId, cancellationToken);
    }

    /// <inheritdoc cref="SetManyAtomicAsync{T}(ILattice, List{KeyValuePair{string, T}}, string, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task SetManyAtomicAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return lattice.SetManyAtomicAsync(entries, operationId, JsonLatticeSerializer<T>.Default, cancellationToken);
    }

    // ── Bulk Loading ────────────────────────────────────────────

    /// <summary>
    /// Serializes and bulk-loads key-value pairs into an empty tree.
    /// </summary>
    public static Task BulkLoadAsync<T>(
        this ILattice lattice,
        IReadOnlyList<KeyValuePair<string, T>> entries,
        ILatticeSerializer<T> serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = new List<KeyValuePair<string, byte[]>>(entries.Count);
        foreach (var (k, v) in entries)
            raw.Add(new KeyValuePair<string, byte[]>(k, serializer.Serialize(v)));
        return lattice.BulkLoadAsync(raw, cancellationToken);
    }

    /// <inheritdoc cref="BulkLoadAsync{T}(ILattice, IReadOnlyList{KeyValuePair{string, T}}, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task BulkLoadAsync<T>(
        this ILattice lattice,
        IReadOnlyList<KeyValuePair<string, T>> entries,
        CancellationToken cancellationToken = default) =>
        lattice.BulkLoadAsync(entries, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Low-level typed streaming primitive. Streams deserialized entries
    /// in lexicographic key order. Prefer
    /// <see cref="ScanEntriesAsync{T}(ILattice, ILatticeSerializer{T}, string?, string?, bool, bool?, int?, CancellationToken)"/>, 
    /// which adds transparent reconnect on
    /// <c>Orleans.Runtime.EnumerationAbortedException</c>. Hidden from
    /// IntelliSense to steer callers toward the resilient wrapper.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static async IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILattice lattice,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        await foreach (var entry in lattice.EntriesAsync(startInclusive, endExclusive, reverse, prefetch, cancellationToken))
        {
            yield return new KeyValuePair<string, T>(entry.Key, serializer.Deserialize(entry.Value));
        }
    }

    /// <inheritdoc cref="EntriesAsync{T}(ILattice, ILatticeSerializer{T}, string?, string?, bool, bool?, CancellationToken)"/>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default) =>
        lattice.EntriesAsync(JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, prefetch, cancellationToken);

    /// <summary>
    /// Resilient typed entry scan. Composes <see cref="LatticeExtensions.ScanEntriesAsync(ILattice, string?, string?, bool, bool?, int?, CancellationToken)"/>
    /// with <paramref name="serializer"/>, so typed exports automatically recover
    /// from <c>Orleans.Runtime.EnumerationAbortedException</c> without duplicates
    /// or gaps. This is the recommended client API for long-running typed scans.
    /// </summary>
    /// <param name="lattice">The tree to scan.</param>
    /// <param name="serializer">Value deserializer.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c>.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c>.</param>
    /// <param name="reverse">If <c>true</c>, yields entries in descending key order.</param>
    /// <param name="prefetch">Optional per-call override for shard prefetch.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(
        this ILattice lattice,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        await foreach (var entry in lattice.ScanEntriesAsync(startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken).ConfigureAwait(false))
        {
            yield return new KeyValuePair<string, T>(entry.Key, serializer.Deserialize(entry.Value));
        }
    }

    /// <inheritdoc cref="ScanEntriesAsync{T}(ILattice, ILatticeSerializer{T}, string?, string?, bool, bool?, int?, CancellationToken)"/>
    public static IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default) =>
        lattice.ScanEntriesAsync(JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken);
}
