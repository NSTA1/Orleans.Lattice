namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// The durable storage row shared by every <see cref="LandedConflictPersistentState{T}"/>
/// bound to one grain. Each activation of the grain gets its own
/// <see cref="LandedConflictPersistentState{T}"/> over the same row, which is how
/// these tests model a fresh activation reloading what is actually durable.
/// </summary>
/// <typeparam name="T">The grain state type.</typeparam>
internal sealed class DurableStateRow<T> where T : new()
{
    /// <summary>A deep copy of the last state that landed, or <see langword="null"/> when the row is absent.</summary>
    public T? Value { get; set; }

    /// <summary>The row's current ETag. Bumped by every landed write and clear.</summary>
    public int Etag { get; set; }

    /// <summary>Whether the row exists.</summary>
    public bool Exists => Value is not null;

    /// <summary>Number of writes (from any activation) that actually landed.</summary>
    public int LandedWrites { get; set; }
}
