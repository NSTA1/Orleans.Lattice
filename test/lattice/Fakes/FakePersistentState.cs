namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// In-memory implementation of <see cref="IPersistentState{TState}"/> for unit
/// testing POCO grains without a storage provider.
/// </summary>
internal sealed class FakePersistentState<T> : IPersistentState<T> where T : new()
{
    public T State { get; set; } = new();
    public string Etag => string.Empty;
    public bool RecordExists => true;

    public Task ClearStateAsync()
    {
        State = new();
        return Task.CompletedTask;
    }

    public Task ReadStateAsync() => Task.CompletedTask;
    public Task WriteStateAsync() => Task.CompletedTask;
}
