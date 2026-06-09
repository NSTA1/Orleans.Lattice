using System.Linq.Expressions;
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
    /// Fetches multiple keys and returns only those whose deserialized value
    /// satisfies <paramref name="predicate"/>. The predicate is lowered to a
    /// serializable IR and evaluated <b>server-side</b> against each value's
    /// JSON document view, so values that do not match are dropped on the
    /// owning leaf and never cross the wire. Missing/tombstoned keys are
    /// omitted as usual.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or <paramref name="predicate"/> contains an unsupported construct.
    /// </exception>
    public static async Task<Dictionary<string, T>> GetManyAsync<T>(
        this ILattice lattice,
        List<string> keys,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        using (LatticePredicateContext.With(ir))
        {
            var raw = await lattice.GetManyAsync(keys, cancellationToken);
            var result = new Dictionary<string, T>(raw.Count);
            foreach (var (k, v) in raw)
                result[k] = serializer.Deserialize(v);
            return result;
        }
    }

    /// <inheritdoc cref="GetManyAsync{T}(ILattice, List{string}, Expression{Func{T, bool}}, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task<Dictionary<string, T>> GetManyAsync<T>(
        this ILattice lattice,
        List<string> keys,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default) =>
        lattice.GetManyAsync(keys, predicate, JsonLatticeSerializer<T>.Default, cancellationToken);

    /// <summary>
    /// Serializes and inserts/updates multiple key-value pairs in parallel across shards.
    /// </summary>
    public static Task SetManyAsync<T>(
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
        return lattice.SetManyAsync(raw, cancellationToken);
    }

    /// <inheritdoc cref="SetManyAsync{T}(ILattice, List{KeyValuePair{string, T}}, ILatticeSerializer{T}, CancellationToken)"/>
    public static Task SetManyAsync<T>(
        this ILattice lattice,
        List<KeyValuePair<string, T>> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return lattice.SetManyAsync(entries, JsonLatticeSerializer<T>.Default, cancellationToken);
    }

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
        ArgumentNullException.ThrowIfNull(entries);
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
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return lattice.BulkLoadAsync(entries, JsonLatticeSerializer<T>.Default, cancellationToken);
    }

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

    // ── Predicate push-down (streaming scans) ───────────────────
    //
    // The predicate is lowered to the serializable LatticePredicateNode IR
    // (gated on the serializer's ILatticePredicateSerializer capability) and
    // carried to every shard's leaf-scan via the ambient
    // LatticePredicateContext scope. The leaf evaluates the IR against each
    // candidate value's JSON document view and drops non-matching keys before
    // they are paged, so the filter is applied consistently across the k-way
    // merge and reconciliation drains, and filtered keys never materialize
    // client-side. The scope stays open for the whole enumeration (including
    // transparent reconnects in the resilient Scan* wrappers), so the
    // predicate survives an EnumerationAbortedException intact.

    /// <summary>
    /// Resilient typed key scan whose keys are filtered server-side by a value
    /// <paramref name="predicate"/>. The leaf reads each candidate's value to
    /// evaluate the predicate but only the matching <b>keys</b> stream back -
    /// no values cross the wire. Recovers from
    /// <c>Orleans.Runtime.EnumerationAbortedException</c> with the predicate
    /// intact.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or <paramref name="predicate"/> contains an unsupported construct.
    /// </exception>
    public static async IAsyncEnumerable<string> ScanKeysAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        await foreach (var key in lattice.ScanKeysWhereAsync(ir, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken).ConfigureAwait(false))
            yield return key;
    }

    /// <inheritdoc cref="ScanKeysAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool?, int?, CancellationToken)"/>
    public static IAsyncEnumerable<string> ScanKeysAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default) =>
        lattice.ScanKeysAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken);

    /// <summary>
    /// Low-level single-page typed key scan filtered server-side by a value
    /// <paramref name="predicate"/>. Prefer the resilient
    /// <see cref="ScanKeysAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool?, int?, CancellationToken)"/>.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static async IAsyncEnumerable<string> KeysAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        await foreach (var key in lattice.KeysWherePredicateAsync(ir, startInclusive, endExclusive, reverse, prefetch, cancellationToken).ConfigureAwait(false))
            yield return key;
    }

    /// <inheritdoc cref="KeysAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool?, CancellationToken)"/>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static IAsyncEnumerable<string> KeysAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default) =>
        lattice.KeysAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, prefetch, cancellationToken);

    /// <summary>
    /// Resilient typed entry scan filtered server-side by
    /// <paramref name="predicate"/>. Non-matching entries are dropped on the
    /// owning leaf and never cross the wire. Recovers from
    /// <c>Orleans.Runtime.EnumerationAbortedException</c> with the predicate
    /// intact.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or <paramref name="predicate"/> contains an unsupported construct.
    /// </exception>
    public static async IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        await foreach (var entry in lattice.ScanEntriesWhereAsync(ir, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken).ConfigureAwait(false))
            yield return new KeyValuePair<string, T>(entry.Key, serializer.Deserialize(entry.Value));
    }

    /// <inheritdoc cref="ScanEntriesAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool?, int?, CancellationToken)"/>
    public static IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default) =>
        lattice.ScanEntriesAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken);

    /// <summary>
    /// Low-level single-page typed entry scan filtered server-side by
    /// <paramref name="predicate"/>. Prefer the resilient
    /// <see cref="ScanEntriesAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool?, int?, CancellationToken)"/>.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static async IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        await foreach (var entry in lattice.EntriesWherePredicateAsync(ir, startInclusive, endExclusive, reverse, prefetch, cancellationToken).ConfigureAwait(false))
            yield return new KeyValuePair<string, T>(entry.Key, serializer.Deserialize(entry.Value));
    }

    /// <inheritdoc cref="EntriesAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool?, CancellationToken)"/>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default) =>
        lattice.EntriesAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, prefetch, cancellationToken);

    /// <summary>
    /// Resilient typed value projection over the entry scan, optionally
    /// filtered server-side by <paramref name="predicate"/>. Yields only the
    /// deserialized values in key order; when a predicate is supplied,
    /// non-matching values are dropped on the owning leaf and never cross the
    /// wire. Recovers from <c>Orleans.Runtime.EnumerationAbortedException</c>
    /// with the predicate intact.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// A non-null <paramref name="predicate"/> is supplied and the serializer
    /// does not implement <see cref="ILatticePredicateSerializer"/>, or the
    /// predicate contains an unsupported construct.
    /// </exception>
    public static async IAsyncEnumerable<T> ScanValuesAsync<T>(
        this ILattice lattice,
        ILatticeSerializer<T> serializer,
        Expression<Func<T, bool>>? predicate = null,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = predicate is null ? (LatticePredicateNode?)null : LatticePredicatePushdown.Compile(predicate, serializer);
        var entries = ir is null
            ? lattice.ScanEntriesAsync(startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken)
            : lattice.ScanEntriesWhereAsync(ir.Value, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken);
        await foreach (var entry in entries.ConfigureAwait(false))
            yield return serializer.Deserialize(entry.Value);
    }

    /// <inheritdoc cref="ScanValuesAsync{T}(ILattice, ILatticeSerializer{T}, Expression{Func{T, bool}}, string?, string?, bool, bool?, int?, CancellationToken)"/>
    public static IAsyncEnumerable<T> ScanValuesAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>>? predicate = null,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default) =>
        lattice.ScanValuesAsync(JsonLatticeSerializer<T>.Default, predicate, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken);

    /// <summary>
    /// Low-level single-page typed value projection, optionally filtered
    /// server-side by <paramref name="predicate"/>. Prefer the resilient
    /// <see cref="ScanValuesAsync{T}(ILattice, ILatticeSerializer{T}, Expression{Func{T, bool}}, string?, string?, bool, bool?, int?, CancellationToken)"/>.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static async IAsyncEnumerable<T> ValuesAsync<T>(
        this ILattice lattice,
        ILatticeSerializer<T> serializer,
        Expression<Func<T, bool>>? predicate = null,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = predicate is null ? (LatticePredicateNode?)null : LatticePredicatePushdown.Compile(predicate, serializer);
        var entries = ir is null
            ? lattice.EntriesAsync(startInclusive, endExclusive, reverse, prefetch, cancellationToken)
            : lattice.EntriesWherePredicateAsync(ir.Value, startInclusive, endExclusive, reverse, prefetch, cancellationToken);
        await foreach (var entry in entries.ConfigureAwait(false))
            yield return serializer.Deserialize(entry.Value);
    }

    /// <inheritdoc cref="ValuesAsync{T}(ILattice, ILatticeSerializer{T}, Expression{Func{T, bool}}, string?, string?, bool, bool?, CancellationToken)"/>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static IAsyncEnumerable<T> ValuesAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>>? predicate = null,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default) =>
        lattice.ValuesAsync(JsonLatticeSerializer<T>.Default, predicate, startInclusive, endExclusive, reverse, prefetch, cancellationToken);

    // ── Predicate-filtered cursors ──────────────────────────────

    /// <summary>
    /// Opens a stateful key cursor whose every page is filtered server-side by
    /// <paramref name="predicate"/>. The compiled IR is persisted on the cursor
    /// spec, so a durable cursor that reactivates after a silo failover
    /// re-applies the identical filter. Composes with point-in-time mode.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or <paramref name="predicate"/> contains an unsupported construct.
    /// </exception>
    public static Task<string> OpenKeyCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        return lattice.OpenKeyCursorWherePredicateAsync(ir, startInclusive, endExclusive, reverse, pointInTime, cancellationToken);
    }

    /// <inheritdoc cref="OpenKeyCursorAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool, CancellationToken)"/>
    public static Task<string> OpenKeyCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default) =>
        lattice.OpenKeyCursorAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, pointInTime, cancellationToken);

    /// <summary>
    /// Opens a stateful entry cursor whose every page is filtered server-side
    /// by <paramref name="predicate"/>. See
    /// <see cref="OpenKeyCursorAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool, CancellationToken)"/>
    /// for the durability and composition contract.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or <paramref name="predicate"/> contains an unsupported construct.
    /// </exception>
    public static Task<string> OpenEntryCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        return lattice.OpenEntryCursorWherePredicateAsync(ir, startInclusive, endExclusive, reverse, pointInTime, cancellationToken);
    }

    /// <inheritdoc cref="OpenEntryCursorAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, bool, CancellationToken)"/>
    public static Task<string> OpenEntryCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default) =>
        lattice.OpenEntryCursorAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, pointInTime, cancellationToken);

    /// <summary>
    /// Opens a zero-observable-writes snapshot key cursor whose every page is
    /// filtered server-side by <paramref name="predicate"/>. The filter
    /// composes with the WAL-coordinate replay and frozen saga-decision
    /// snapshot, and is persisted so a reactivated cursor re-applies it.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or <paramref name="predicate"/> contains an unsupported construct.
    /// </exception>
    public static Task<string> OpenSnapshotKeyCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        return lattice.OpenSnapshotKeyCursorWherePredicateAsync(ir, startInclusive, endExclusive, reverse, cancellationToken);
    }

    /// <inheritdoc cref="OpenSnapshotKeyCursorAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, CancellationToken)"/>
    public static Task<string> OpenSnapshotKeyCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default) =>
        lattice.OpenSnapshotKeyCursorAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, cancellationToken);

    /// <summary>
    /// Opens a zero-observable-writes snapshot entry cursor whose every page is
    /// filtered server-side by <paramref name="predicate"/>. See
    /// <see cref="OpenSnapshotKeyCursorAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, CancellationToken)"/>.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or <paramref name="predicate"/> contains an unsupported construct.
    /// </exception>
    public static Task<string> OpenSnapshotEntryCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        ILatticeSerializer<T> serializer,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);
        var ir = LatticePredicatePushdown.Compile(predicate, serializer);
        return lattice.OpenSnapshotEntryCursorWherePredicateAsync(ir, startInclusive, endExclusive, reverse, cancellationToken);
    }

    /// <inheritdoc cref="OpenSnapshotEntryCursorAsync{T}(ILattice, Expression{Func{T, bool}}, ILatticeSerializer{T}, string?, string?, bool, CancellationToken)"/>
    public static Task<string> OpenSnapshotEntryCursorAsync<T>(
        this ILattice lattice,
        Expression<Func<T, bool>> predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default) =>
        lattice.OpenSnapshotEntryCursorAsync(predicate, JsonLatticeSerializer<T>.Default, startInclusive, endExclusive, reverse, cancellationToken);
}
