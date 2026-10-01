using System.Collections.Concurrent;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Schema.Tests.Chaos;

/// <summary>
/// Process-scope in-memory <see cref="IGrainStorage"/> for the schema chaos suite:
/// state lives in a static dictionary every silo shares, so a killed silo loses
/// only its activations and in-process work, exactly as it would over a durable
/// provider, never the grain state other silos read back.
/// </summary>
internal sealed class ProcessScopeSchemaGrainStorage : IGrainStorage
{
    private static readonly ConcurrentDictionary<string, (string ETag, object State)> Store = new();

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        if (Store.TryGetValue(Key(stateName, grainId), out var entry))
        {
            grainState.State = (T)entry.State;
            grainState.ETag = entry.ETag;
            grainState.RecordExists = true;
        }
        else
        {
            grainState.RecordExists = false;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var etag = Guid.NewGuid().ToString("N");
        Store[Key(stateName, grainId)] = (etag, grainState.State!);
        grainState.ETag = etag;
        grainState.RecordExists = true;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        Store.TryRemove(Key(stateName, grainId), out _);
        grainState.ETag = null!;
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }

    private static string Key(string stateName, GrainId grainId) => $"{stateName}/{grainId}";
}
