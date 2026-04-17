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
    public static async Task<T?> GetAsync<T>(this ILattice lattice, string key, ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var bytes = await lattice.GetAsync(key);
        return bytes is null ? default : serializer.Deserialize(bytes);
    }

    /// <inheritdoc cref="GetAsync{T}(ILattice, string, ILatticeSerializer{T})"/>
    public static Task<T?> GetAsync<T>(this ILattice lattice, string key) =>
        lattice.GetAsync(key, JsonLatticeSerializer<T>.Default);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns the existing deserialized value when the
    /// key is already live, or <c>default</c> when the value was newly written.
    /// </summary>
    public static async Task<T?> GetOrSetAsync<T>(this ILattice lattice, string key, T value, ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var bytes = await lattice.GetOrSetAsync(key, serializer.Serialize(value));
        return bytes is null ? default : serializer.Deserialize(bytes);
    }

    /// <inheritdoc cref="GetOrSetAsync{T}(ILattice, string, T, ILatticeSerializer{T})"/>
    public static Task<T?> GetOrSetAsync<T>(this ILattice lattice, string key, T value) =>
        lattice.GetOrSetAsync(key, value, JsonLatticeSerializer<T>.Default);

    /// <summary>Serializes <paramref name="value"/> and stores it under <paramref name="key"/>.</summary>
    public static Task SetAsync<T>(this ILattice lattice, string key, T value, ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return lattice.SetAsync(key, serializer.Serialize(value));
    }

    /// <inheritdoc cref="SetAsync{T}(ILattice, string, T, ILatticeSerializer{T})"/>
    public static Task SetAsync<T>(this ILattice lattice, string key, T value) =>
        lattice.SetAsync(key, value, JsonLatticeSerializer<T>.Default);

    // ── Batch ───────────────────────────────────────────────────

    /// <summary>
    /// Fetches multiple keys and deserializes their values.
    /// Missing/tombstoned keys are omitted from the result.
    /// </summary>
    public static async Task<Dictionary<string, T>> GetManyAsync<T>(
        this ILattice lattice,
        List<string> keys,
        ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = await lattice.GetManyAsync(keys);
        var result = new Dictionary<string, T>(raw.Count);
        foreach (var (k, v) in raw)
            result[k] = serializer.Deserialize(v);
        return result;
    }

    /// <inheritdoc cref="GetManyAsync{T}(ILattice, List{string}, ILatticeSerializer{T})"/>
    public static Task<Dictionary<string, T>> GetManyAsync<T>(
        this ILattice lattice,
        List<string> keys) =>
        lattice.GetManyAsync(keys, JsonLatticeSerializer<T>.Default);

    /// <summary>
    /// Serializes and inserts/updates multiple key-value pairs in parallel across shards.
    /// </summary>
    public static Task SetManyAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = new List<KeyValuePair<string, byte[]>>(entries.Count);
        foreach (var (k, v) in entries)
            raw.Add(new KeyValuePair<string, byte[]>(k, serializer.Serialize(v)));
        return lattice.SetManyAsync(raw);
    }

    /// <inheritdoc cref="SetManyAsync{T}(ILattice, List{KeyValuePair{string, T}}, ILatticeSerializer{T})"/>
    public static Task SetManyAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries) =>
        lattice.SetManyAsync(entries, JsonLatticeSerializer<T>.Default);

    // ── Bulk Loading ────────────────────────────────────────────

    /// <summary>
    /// Serializes and bulk-loads key-value pairs into an empty tree.
    /// </summary>
    public static Task BulkLoadAsync<T>(
        this ILattice lattice,
        IReadOnlyList<KeyValuePair<string, T>> entries,
        ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var raw = new List<KeyValuePair<string, byte[]>>(entries.Count);
        foreach (var (k, v) in entries)
            raw.Add(new KeyValuePair<string, byte[]>(k, serializer.Serialize(v)));
        return lattice.BulkLoadAsync(raw);
    }

    /// <inheritdoc cref="BulkLoadAsync{T}(ILattice, IReadOnlyList{KeyValuePair{string, T}}, ILatticeSerializer{T})"/>
    public static Task BulkLoadAsync<T>(
        this ILattice lattice,
        IReadOnlyList<KeyValuePair<string, T>> entries) =>
        lattice.BulkLoadAsync(entries, JsonLatticeSerializer<T>.Default);

    // ── Enumeration ─────────────────────────────────────────────

    /// <summary>
    /// Streams live key-value entries in sorted key order, deserializing values via
    /// the provided <paramref name="serializer"/>.
    /// </summary>
    public static async IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILattice lattice,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        await foreach (var entry in lattice.EntriesAsync(startInclusive, endExclusive, reverse))
        {
            yield return new KeyValuePair<string, T>(entry.Key, serializer.Deserialize(entry.Value));
        }
    }

    /// <inheritdoc cref="EntriesAsync{T}(ILattice, ILatticeSerializer{T}, string?, string?, bool)"/>
    public static IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false) =>
        lattice.EntriesAsync(JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse);
}
