using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// An <see cref="IPersistentState{TState}"/> double that enforces optimistic
/// concurrency against a shared <see cref="DurableStateRow{T}"/> and can inject
/// the issue #3572 fault: a write that <b>lands</b> and is then reported as an
/// <see cref="InconsistentStateException"/>. That is what an Azure Table 412/409
/// looks like after a storage-SDK transport retry: the first attempt applied the
/// write, the retry saw the row's new ETag, and the activation's cached ETag is
/// stale from then on, so every later write conflicts too.
/// </summary>
/// <remarks>
/// State is deep-copied through the Orleans copier on every write and read, so
/// the durable row can never alias the activation's in-memory object: an
/// in-memory mutation after a failed write is not silently "persisted".
/// </remarks>
/// <typeparam name="T">The grain state type.</typeparam>
internal sealed class LandedConflictPersistentState<T> : IPersistentState<T> where T : new()
{
    private static readonly Lazy<DeepCopier<T>> Copier = new(static () =>
        new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<DeepCopier<T>>());

    private readonly DurableStateRow<T> _row;
    private int _etag;

    /// <summary>
    /// Creates an activation's view over <paramref name="row"/>, loaded as the
    /// Orleans runtime loads state before activating a grain.
    /// </summary>
    public LandedConflictPersistentState(DurableStateRow<T> row)
    {
        _row = row;
        Load();
    }

    /// <inheritdoc />
    public T State { get; set; } = new();

    /// <inheritdoc />
    public string Etag => _etag.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <inheritdoc />
    public bool RecordExists { get; private set; }

    /// <summary>
    /// When set, the next write whose state satisfies this predicate lands and
    /// then throws <see cref="InconsistentStateException"/>, leaving this
    /// activation's cached ETag stale. Cleared once it fires.
    /// </summary>
    public Func<T, bool>? LandThenConflictWhen { get; set; }

    /// <summary>
    /// When <see langword="true"/>, the next clear lands and then throws
    /// <see cref="InconsistentStateException"/>. Cleared once it fires.
    /// </summary>
    public bool LandThenConflictOnNextClear { get; set; }

    /// <summary>Number of write attempts this activation issued.</summary>
    public int WriteAttempts { get; private set; }

    /// <summary>Number of writes this activation issued that the row rejected on a stale ETag.</summary>
    public int StaleEtagRejections { get; private set; }

    /// <summary>Number of reads this activation issued after it was loaded.</summary>
    public int Reads { get; private set; }

    /// <summary>Arms <see cref="LandThenConflictWhen"/> for the very next write.</summary>
    public void LandThenConflictOnNextWrite() => LandThenConflictWhen = static _ => true;

    /// <inheritdoc />
    public Task ReadStateAsync()
    {
        Reads++;
        Load();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task WriteStateAsync()
    {
        WriteAttempts++;
        if (_etag != _row.Etag)
        {
            StaleEtagRejections++;
            return Task.FromException(StaleEtag());
        }

        _row.Value = Copier.Value.Copy(State);
        _row.Etag++;
        _row.LandedWrites++;

        if (LandThenConflictWhen is { } when && when(State))
        {
            LandThenConflictWhen = null;
            return Task.FromException(StaleEtag());
        }

        _etag = _row.Etag;
        RecordExists = true;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearStateAsync()
    {
        if (_etag != _row.Etag)
        {
            StaleEtagRejections++;
            return Task.FromException(StaleEtag());
        }

        _row.Value = default;
        _row.Etag++;

        if (LandThenConflictOnNextClear)
        {
            LandThenConflictOnNextClear = false;
            return Task.FromException(StaleEtag());
        }

        _etag = _row.Etag;
        State = new();
        RecordExists = false;
        return Task.CompletedTask;
    }

    private void Load()
    {
        State = _row.Value is { } value ? Copier.Value.Copy(value) : new T();
        RecordExists = _row.Exists;
        _etag = _row.Etag;
    }

    private InconsistentStateException StaleEtag() =>
        new("ETag mismatch (simulated Azure Table 412 after a transport retry).",
            _row.Etag.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Etag);
}
