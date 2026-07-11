using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Schema.Tests.Fakes;

/// <summary>
/// In-memory <see cref="IPersistentState{TState}"/> for unit-testing POCO grains
/// without a storage provider. Tracks the write count and can be primed to throw
/// on the next write to exercise the coordinator's rollback-on-write-failure path.
/// </summary>
internal sealed class FakePersistentState<T> : IPersistentState<T> where T : new()
{
    public T State { get; set; } = new();
    public string Etag => string.Empty;
    public bool RecordExists { get; private set; } = true;

    /// <summary>Number of successful <see cref="WriteStateAsync"/> calls.</summary>
    public int WriteCount { get; private set; }

    /// <summary>
    /// When set, the next <see cref="WriteStateAsync"/> throws this exception
    /// instead of persisting, then clears itself so the following write succeeds.
    /// </summary>
    public Exception? ThrowOnWrite { get; set; }

    public Task ClearStateAsync()
    {
        State = new();
        RecordExists = false;
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
        RecordExists = true;
        return Task.CompletedTask;
    }
}
