using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// An in-memory <see cref="IGrainStorage"/> whose stored rows can be enumerated by
/// state name, so a test can assert on what a removal actually left in storage
/// rather than on what a grain reports about it (issue #4383).
/// <para>
/// That distinction is the point. A snapshot manifest that has been tombstoned
/// reads back as "no snapshot" while its row still exists, so a load-based
/// assertion passes on exactly the leak it is meant to catch. Only the row set
/// can say a row is gone.
/// </para>
/// <para>
/// Instance-scoped, so one fixture's rows never mix with another's. State is
/// deep-copied on the way in and out, as a serialising provider would copy it.
/// ETags are issued but not enforced: these fixtures run one silo.
/// </para>
/// </summary>
internal sealed class EnumerableMemoryGrainStorage : IGrainStorage
{
    private readonly ConcurrentDictionary<(string StateName, GrainId GrainId), object> _rows = new();

    private static readonly Lazy<DeepCopier> Copier = new(static () =>
        new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<DeepCopier>());

    /// <summary>Every grain id holding a row under <paramref name="stateName"/>.</summary>
    /// <param name="stateName">The persistent-state name, for example <c>leaf-snapshot</c>.</param>
    /// <returns>The grain ids, in no particular order.</returns>
    public IReadOnlyList<GrainId> GrainIds(string stateName)
        => _rows.Keys.Where(k => k.StateName == stateName).Select(k => k.GrainId).ToArray();

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        if (_rows.TryGetValue((stateName, grainId), out var stored))
        {
            grainState.State = Copier.Value.Copy((T)stored);
            grainState.ETag = Guid.NewGuid().ToString("N");
            grainState.RecordExists = true;
        }
        else
        {
            grainState.RecordExists = false;
            grainState.ETag = null!;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        _rows[(stateName, grainId)] = Copier.Value.Copy(grainState.State)!;
        grainState.ETag = Guid.NewGuid().ToString("N");
        grainState.RecordExists = true;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        _rows.TryRemove((stateName, grainId), out _);
        grainState.ETag = null!;
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }
}
