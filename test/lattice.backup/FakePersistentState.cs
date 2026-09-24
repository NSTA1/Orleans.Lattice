namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// In-memory <see cref="IPersistentState{TState}"/> for unit-testing backup grains
/// without a storage provider. Mirrors the core library's test-side fake.
/// </summary>
internal sealed class FakePersistentState<T> : IPersistentState<T> where T : new()
{
    public T State { get; set; } = new();
    public string Etag => string.Empty;
    public bool RecordExists => true;

    /// <summary>Number of times <see cref="WriteStateAsync"/> has completed successfully.</summary>
    public int WriteCount { get; private set; }

    /// <summary>
    /// When set, the next <see cref="WriteStateAsync"/> call throws this
    /// exception instead of incrementing <see cref="WriteCount"/>. Cleared
    /// after one throw, so a retry succeeds.
    /// </summary>
    public Exception? ThrowOnWrite { get; set; }

    public Task ClearStateAsync()
    {
        State = new();
        return Task.CompletedTask;
    }

    public Task ReadStateAsync() => Task.CompletedTask;

    public Task WriteStateAsync()
    {
        if (ThrowOnWrite is { } ex)
        {
            ThrowOnWrite = null;
            return Task.FromException(ex);
        }

        WriteCount++;
        return Task.CompletedTask;
    }
}
