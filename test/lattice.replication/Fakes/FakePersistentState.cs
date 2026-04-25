namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// In-memory <see cref="IPersistentState{TState}"/> for unit-testing
/// replication grains without a storage provider. Mirrors the core
/// library's test-side fake.
/// </summary>
internal sealed class FakePersistentState<T> : IPersistentState<T> where T : new()
{
    public T State { get; set; } = new();
    public string Etag => string.Empty;
    public bool RecordExists => true;

    /// <summary>Number of times <see cref="WriteStateAsync"/> has been called.</summary>
    public int WriteCount { get; private set; }

    /// <summary>
    /// When set, the next call to <see cref="WriteStateAsync"/> throws this
    /// exception instead of incrementing <see cref="WriteCount"/>. Cleared
    /// after it fires.
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
            throw ex;
        }
        WriteCount++;
        return Task.CompletedTask;
    }
}
