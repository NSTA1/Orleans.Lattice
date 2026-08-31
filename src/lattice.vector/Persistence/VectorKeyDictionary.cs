using System.Buffers.Binary;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The durable, collision-free mapping between the string identifiers a store of
/// record uses and the <see cref="long"/> keys a <see cref="VectorIndex"/> is
/// keyed by.
/// <para>
/// <b>It is not a hash.</b> Identifiers are assigned from a strictly increasing
/// counter, so two distinct identifiers can never share a key - not with
/// vanishing probability, but by construction. A hash of any width would make a
/// collision <i>silently return the wrong document</i>, which is precisely the
/// failure a retrieval index must not have, and no amount of width makes that
/// failure diagnosable after the fact.
/// </para>
/// <para>
/// <b>Crash safety.</b> The counter is reserved ahead in blocks and the
/// watermark is made durable <i>before</i> any identifier in the block is handed
/// out, so a process that dies mid-block resumes past the whole block rather
/// than re-issuing a key it may already have used. Unused identifiers in an
/// abandoned block are simply burned; the space is 64 bits wide and the
/// alternative is a reuse hazard.
/// </para>
/// <para>
/// <b>Stability.</b> A key is never recycled, and an identifier that is
/// re-embedded keeps the key it already had, so re-embedding is an in-place
/// update of one cell rather than a delete and an insert. Only the forward
/// direction is persisted; the reverse map is rebuilt in memory from the same
/// scan, so the two can never disagree on disk.
/// </para>
/// </summary>
public sealed class VectorKeyDictionary
{
    private readonly Dictionary<string, long> _forward = new(StringComparer.Ordinal);
    private readonly Dictionary<long, string> _reverse = [];
    private readonly IVectorIndexStore _store;
    private readonly string _prefix;
    private readonly int _reservationBlock;
    private long _next;
    private long _reservedTo;

    /// <summary>
    /// Creates a dictionary over a store. Call <see cref="LoadAsync"/> before use
    /// so it adopts whatever a previous process already assigned.
    /// </summary>
    /// <param name="store">The durable store the mapping lives on.</param>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="reservationBlock">How many identifiers one durable reservation covers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="store"/> or <paramref name="prefix"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="reservationBlock"/> is not positive.</exception>
    public VectorKeyDictionary(IVectorIndexStore store, string prefix, int reservationBlock)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(reservationBlock);

        _store = store;
        _prefix = prefix;
        _reservationBlock = reservationBlock;
    }

    /// <summary>The number of identifiers currently mapped.</summary>
    public int Count => _forward.Count;

    /// <summary>
    /// Every mapped identifier, in no particular order. Materialise before
    /// mutating: this is a live view of the mapping.
    /// </summary>
    public IReadOnlyCollection<string> Ids => _forward.Keys;

    /// <summary>
    /// The next identifier that would be assigned. Never decreases, and never
    /// revisits a value a previous process may have handed out.
    /// </summary>
    public long NextKey => _next;

    /// <summary>
    /// Loads the persisted mapping, replacing anything held in memory. Safe to
    /// call on an empty store, where it simply yields an empty dictionary.
    /// </summary>
    /// <param name="cancellationToken">Cancels the load.</param>
    public async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        _forward.Clear();
        _reverse.Clear();
        _next = 0;
        _reservedTo = 0;

        var highest = -1L;
        var mapPrefix = VectorIndexStorageKeys.KeyMapPrefix(_prefix);
        var scan = _store.ScanAsync(mapPrefix, cancellationToken);
        await foreach (var entry in scan.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (!VectorIndexStorageKeys.TryReadKeyMapId(_prefix, entry.Key, out var id) ||
                id.Length == 0 ||
                !TryReadKey(entry.Value, out var key))
            {
                // A mapping record that cannot be decoded is dropped rather than
                // guessed at. The identifier it named is simply unmapped, so the
                // next write assigns it a fresh key; nothing downstream can
                // observe a half-decoded identifier.
                continue;
            }

            _forward[id] = key;
            _reverse[key] = id;
            if (key > highest)
            {
                highest = key;
            }
        }

        var watermark = await _store.ReadAsync(
            VectorIndexStorageKeys.KeyWatermark(_prefix), cancellationToken).ConfigureAwait(false);

        // The watermark is the authority, but a mapping record that survived a
        // lost watermark still pins the floor: resuming below an identifier that
        // is demonstrably already in use would reissue it.
        _reservedTo = TryReadKey(watermark, out var reserved) ? reserved : 0;
        _next = Math.Max(_reservedTo, highest + 1);
        _reservedTo = Math.Max(_reservedTo, _next);
    }

    /// <summary>
    /// Returns the key for an identifier, assigning and persisting a fresh one
    /// when it has never been seen.
    /// </summary>
    /// <param name="id">The source identifier.</param>
    /// <param name="cancellationToken">Cancels the assignment.</param>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="id"/> is empty.</exception>
    public async Task<long> GetOrAddAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (id.Length == 0)
        {
            throw new ArgumentException("A vector identifier must not be empty.", nameof(id));
        }

        if (_forward.TryGetValue(id, out var existing))
        {
            return existing;
        }

        if (_next >= _reservedTo)
        {
            await ReserveAsync(cancellationToken).ConfigureAwait(false);
        }

        var key = _next++;
        await _store.WriteAsync(
            [new KeyValuePair<string, byte[]>(VectorIndexStorageKeys.KeyMap(_prefix, id), WriteKey(key))],
            cancellationToken).ConfigureAwait(false);

        _forward[id] = key;
        _reverse[key] = id;
        return key;
    }

    /// <summary>Looks up the key an identifier is mapped to, without assigning one.</summary>
    /// <param name="id">The source identifier.</param>
    /// <param name="key">The index key when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the identifier is mapped.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is null.</exception>
    public bool TryGetKey(string id, out long key)
    {
        ArgumentNullException.ThrowIfNull(id);
        return _forward.TryGetValue(id, out key);
    }

    /// <summary>
    /// Resolves an index key back to the identifier it was assigned for. This is
    /// how a result set of <see cref="VectorSearchResult"/> becomes a result set
    /// the caller can act on, and it never touches the store.
    /// </summary>
    /// <param name="key">The index key.</param>
    /// <param name="id">The source identifier when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the key is mapped.</returns>
    public bool TryGetId(long key, out string id) => _reverse.TryGetValue(key, out id!);

    /// <summary>
    /// Drops an identifier's mapping and its persisted record. The key it held is
    /// retired with it and is never reassigned.
    /// </summary>
    /// <param name="id">The source identifier.</param>
    /// <param name="cancellationToken">Cancels the removal.</param>
    /// <returns>The key that was mapped, or <see langword="null"/> when the identifier was unmapped.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is null.</exception>
    public async Task<long?> RemoveAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (!_forward.Remove(id, out var key))
        {
            return null;
        }

        _reverse.Remove(key);
        await _store.DeleteAsync(
            [VectorIndexStorageKeys.KeyMap(_prefix, id)], cancellationToken).ConfigureAwait(false);
        return key;
    }

    /// <summary>
    /// Forgets every mapping, in memory and on the store. Used only when the
    /// whole index is being rebuilt from source, where the keys it assigned have
    /// no surviving meaning.
    /// </summary>
    /// <param name="cancellationToken">Cancels the reset.</param>
    public async Task ClearAsync(CancellationToken cancellationToken = default)
    {
        await _store.DeletePrefixAsync(
            VectorIndexStorageKeys.KeyMapPrefix(_prefix), cancellationToken).ConfigureAwait(false);

        _forward.Clear();
        _reverse.Clear();

        // The counter deliberately does not rewind. A rebuild discards the
        // mapping, not the history of which keys have been in circulation, and a
        // durable record elsewhere may still name one of them.
        await PersistWatermarkAsync(Math.Max(_reservedTo, _next), cancellationToken).ConfigureAwait(false);
    }

    private async Task ReserveAsync(CancellationToken cancellationToken)
    {
        var target = _next + _reservationBlock;
        await PersistWatermarkAsync(target, cancellationToken).ConfigureAwait(false);
        _reservedTo = target;
    }

    private Task PersistWatermarkAsync(long value, CancellationToken cancellationToken)
    {
        _reservedTo = Math.Max(_reservedTo, value);
        return _store.WriteAsync(
            [new KeyValuePair<string, byte[]>(VectorIndexStorageKeys.KeyWatermark(_prefix), WriteKey(value))],
            cancellationToken);
    }

    private static byte[] WriteKey(long value)
    {
        Span<byte> payload = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(payload, value);
        return VectorIndexRecord.Wrap(payload);
    }

    private static bool TryReadKey(byte[]? record, out long value)
    {
        value = 0;
        if (record is null || !VectorIndexRecord.TryUnwrap(record, out var payload) || payload.Length != sizeof(long))
        {
            return false;
        }

        value = BinaryPrimitives.ReadInt64LittleEndian(payload);
        return value >= 0;
    }
}
