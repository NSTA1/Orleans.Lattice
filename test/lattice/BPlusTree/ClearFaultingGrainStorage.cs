using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Delegates to an inner <see cref="IGrainStorage"/>, failing the next
/// <see cref="FailClears"/> deletes as a crash between a write and a delete would, so a
/// test can prove what a sequence leaves durable before its first delete.
/// </summary>
internal sealed class ClearFaultingGrainStorage(IGrainStorage inner) : IGrainStorage
{
    private int _failClears;

    /// <summary>How many of the next deletes fail with an <see cref="IOException"/>.</summary>
    public int FailClears
    {
        get => Volatile.Read(ref _failClears);
        set => Volatile.Write(ref _failClears, value);
    }

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.ReadStateAsync(stateName, grainId, grainState);

    /// <inheritdoc />
    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.WriteStateAsync(stateName, grainId, grainState);

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        if (Interlocked.Decrement(ref _failClears) >= 0)
        {
            throw new IOException("injected crash before the delete");
        }

        Interlocked.Exchange(ref _failClears, 0);
        return inner.ClearStateAsync(stateName, grainId, grainState);
    }
}
