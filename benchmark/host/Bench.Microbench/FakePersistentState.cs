namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// In-memory implementation of <see cref="IPersistentState{TState}"/> used by
/// the BenchmarkDotNet harness to exercise <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>
/// without an Orleans grain runtime. Mirrors the unit-test fake under
/// <c>test/lattice/Fakes/</c> so the benchmark behaves identically to the
/// existing leaf-grain tests on the persistent-state seam.
/// </summary>
internal sealed class FakePersistentState<T> : IPersistentState<T> where T : new()
{
    /// <summary>The in-memory state. Mutated directly by grain code.</summary>
    public T State { get; set; } = new();

    /// <summary>Always returns the empty etag - no concurrency control in the fake.</summary>
    public string Etag => string.Empty;

    /// <summary>Always reports the record as existing once first written.</summary>
    public bool RecordExists => true;

    /// <summary>Resets the state to a fresh instance.</summary>
    public Task ClearStateAsync()
    {
        State = new();
        return Task.CompletedTask;
    }

    /// <summary>No-op - the in-memory state is always live.</summary>
    public Task ReadStateAsync() => Task.CompletedTask;

    /// <summary>No-op - the persistent-state contract is satisfied synchronously.</summary>
    public Task WriteStateAsync() => Task.CompletedTask;
}