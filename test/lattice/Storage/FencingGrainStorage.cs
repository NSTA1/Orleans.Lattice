using System.Collections.Concurrent;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// A scriptable in-memory <see cref="IGrainStorage"/> that enforces ETags the
/// way Orleans' providers do: a write whose ETag is not the stored row's current
/// one throws <see cref="InconsistentStateException"/>. Hooks let a test inject a
/// fault or a delay ahead of the Nth write.
/// </summary>
internal sealed class FencingGrainStorage : IGrainStorage
{
    private readonly ConcurrentDictionary<string, (string ETag, object? State)> _store = new();
    private int _writes;

    /// <summary>
    /// Called before each write with its 1-based ordinal; may throw or delay.
    /// </summary>
    internal Func<int, Task>? BeforeWrite { get; set; }

    /// <summary>How many writes have been attempted.</summary>
    internal int Writes => Volatile.Read(ref _writes);

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
    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var ordinal = Interlocked.Increment(ref _writes);
        if (BeforeWrite is { } hook)
        {
            await hook(ordinal);
        }

        var key = $"{stateName}/{grainId}";
        var current = _store.TryGetValue(key, out var entry) ? entry.ETag : null;
        if (!string.Equals(current, grainState.ETag, StringComparison.Ordinal))
        {
            throw new InconsistentStateException("ETag mismatch.", current ?? "<none>", grainState.ETag ?? "<none>");
        }

        var etag = Guid.NewGuid().ToString("N");
        _store[key] = (etag, grainState.State);
        grainState.ETag = etag;
        grainState.RecordExists = true;
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
