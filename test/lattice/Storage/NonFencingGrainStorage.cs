using System.Collections.Concurrent;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// A minimal in-memory <see cref="IGrainStorage"/> that issues a fresh ETag on
/// every write but never checks the one it is given, so a write carrying a stale
/// ETag succeeds. It is the provider shape <see cref="GrainStorageFencingProbe"/>
/// exists to catch.
/// </summary>
internal sealed class NonFencingGrainStorage : IGrainStorage
{
    private readonly ConcurrentDictionary<string, (string ETag, object? State)> _store = new();

    /// <summary>How many writes the store has accepted.</summary>
    internal int Writes;

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        if (_store.TryGetValue($"{stateName}/{grainId}", out var entry))
        {
            grainState.State = (T)entry.State!;
            grainState.ETag = entry.ETag;
            grainState.RecordExists = true;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var etag = Guid.NewGuid().ToString("N");
        _store[$"{stateName}/{grainId}"] = (etag, grainState.State);
        grainState.ETag = etag;
        grainState.RecordExists = true;
        Interlocked.Increment(ref Writes);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        _store.TryRemove($"{stateName}/{grainId}", out _);
        grainState.ETag = null;
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }
}
