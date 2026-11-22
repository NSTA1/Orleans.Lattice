using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Delegates to an inner <see cref="IGrainStorage"/> and, for a selected fraction of
/// writes, lets the write land and then reports it as an ETag conflict, restoring the
/// caller's pre-write ETag. That is what a storage SDK's transparent retry of a write
/// that already succeeded looks like to the grain (issue #5095): the row is durable,
/// but the activation is told it lost the race and keeps a stale ETag.
/// </summary>
/// <remarks>
/// When <c>shouldRejectUnlanded</c> selects a write instead, it is refused with the same
/// conflict <em>without</em> reaching the inner storage: a genuine conflict that must
/// still surface.
/// <para>
/// When <c>divergeAfterLanding</c> selects a write, it lands and is then reported as a
/// conflict after the selector has moved the caller's in-memory state on, as an
/// interleaved turn does during a slow write. The stored row no longer matches the
/// state, so the conflict cannot be adopted and the activation's ETag is left stale.
/// </para>
/// </remarks>
internal sealed class LandedThenConflictGrainStorage(
    IGrainStorage inner,
    Func<string, GrainId, bool> shouldFault,
    Func<string, GrainId, bool>? shouldRejectUnlanded = null,
    Func<string, GrainId, object?, bool>? divergeAfterLanding = null) : IGrainStorage
{
    private long _injected;
    private long _rejected;

    /// <summary>How many writes have landed and then been reported as a conflict.</summary>
    public long Injected => Interlocked.Read(ref _injected);

    /// <summary>How many writes were refused with a conflict without landing.</summary>
    public long Rejected => Interlocked.Read(ref _rejected);

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.ReadStateAsync(stateName, grainId, grainState);

    /// <inheritdoc />
    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        if (shouldRejectUnlanded?.Invoke(stateName, grainId) == true)
        {
            Interlocked.Increment(ref _rejected);
            throw new InconsistentStateException(
                $"Injected unlanded conflict on '{stateName}' for {grainId}.",
                "injected-other-writer",
                grainState.ETag ?? "Unknown");
        }

        var priorETag = grainState.ETag;
        var priorExists = grainState.RecordExists;
        await inner.WriteStateAsync(stateName, grainId, grainState);
        var diverged = divergeAfterLanding?.Invoke(stateName, grainId, grainState.State) == true;
        if (!diverged && !shouldFault(stateName, grainId))
        {
            return;
        }

        var landedETag = grainState.ETag;
        grainState.ETag = priorETag;
        grainState.RecordExists = priorExists;
        Interlocked.Increment(ref _injected);
        throw new InconsistentStateException(
            $"Injected landed-then-conflict on '{stateName}' for {grainId}.",
            landedETag,
            priorETag ?? "Unknown");
    }

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.ClearStateAsync(stateName, grainId, grainState);
}
