using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Shared classification and translation of a failed grain-state write for the
/// saga grains that persist through <see cref="IPersistentState{TState}"/>
/// (<see cref="AtomicWriteGrain"/>, <see cref="LatticeCrossTreeTxGrain"/> and
/// <see cref="WalMaterialiserPinGrain"/>), issue #3572.
/// <para>
/// A storage SDK transport retry can land a write and still report an
/// optimistic-concurrency failure: the retry sees the row's new ETag and the
/// provider throws an <see cref="InconsistentStateException"/> (Azure Table
/// 412/409). From then on the activation's cached ETag is stale and every later
/// write fails identically. The only way out is to reload the row, so these
/// grains stop writing and deactivate, and the next call resumes from what is
/// actually durable. The exception type alone is the discriminator, never the
/// ETag text (an empty ETag is legitimate on some providers).
/// </para>
/// </summary>
internal static class GrainStateWriteFaults
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="failure"/> (or an
    /// exception it wraps) is an <see cref="InconsistentStateException"/>.
    /// </summary>
    internal static bool IsConflict(Exception failure)
    {
        for (var e = failure; e is not null; e = e.InnerException)
        {
            if (e is InconsistentStateException)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="failure"/> (or an
    /// exception it wraps) is a <see cref="LatticeStateWriteFailedException"/>
    /// reporting a conflict: a saga grain lost an ETag check, deactivated, and
    /// the operation is safe to retry against a fresh activation.
    /// </summary>
    internal static bool IsTranslatedConflict(Exception failure)
    {
        for (var e = failure; e is not null; e = e.InnerException)
        {
            if (e is LatticeStateWriteFailedException { Conflict: true })
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Translates a failed state write into the exception a caller should
    /// observe, or returns <see langword="null"/> when the original exception
    /// may propagate unchanged.
    /// <para>
    /// A conflict always translates, with
    /// <see cref="LatticeStateWriteFailedException.Conflict"/> set. Any other
    /// fault translates only when some exception in its chain comes from an
    /// assembly a client may not be able to load (a storage provider), because
    /// such a type surfaces there as a <see cref="TypeLoadException"/>. A BCL or
    /// Lattice exception passes through unchanged, preserving the contracts
    /// callers already rely on.
    /// </para>
    /// </summary>
    /// <param name="grainType">A short name for the grain whose write failed.</param>
    /// <param name="grainKey">The key of the grain whose write failed.</param>
    /// <param name="failure">The exception the write threw.</param>
    internal static LatticeStateWriteFailedException? Translate(string grainType, string grainKey, Exception failure)
    {
        if (failure is LatticeStateWriteFailedException)
        {
            return null;
        }

        if (IsConflict(failure))
        {
            return new LatticeStateWriteFailedException(grainType, grainKey, failure, conflict: true);
        }

        for (var e = failure; e is not null; e = e.InnerException)
        {
            if (!IsClientLoadable(e.GetType()))
            {
                return new LatticeStateWriteFailedException(grainType, grainKey, e, conflict: false);
            }
        }

        return null;
    }

    /// <summary>
    /// The exception a conflicted activation throws for every later call while
    /// its deactivation is pending, without touching storage again.
    /// </summary>
    /// <param name="grainType">A short name for the grain.</param>
    /// <param name="grainKey">The key of the grain.</param>
    internal static LatticeStateWriteFailedException ConflictedActivation(string grainType, string grainKey) =>
        new($"Lattice {grainType} grain '{grainKey}' lost an optimistic-concurrency check on an earlier state write and is reloading its durable state; retry the operation with the same operation id.")
        {
            GrainType = grainType,
            GrainKey = grainKey,
            FaultType = typeof(InconsistentStateException).FullName!,
            Conflict = true,
        };

    /// <summary>
    /// Clears <paramref name="state"/>, recovering from a conflict by re-reading
    /// the row and clearing again only when the re-read state is still
    /// clearable. A clear that landed but reported a conflict re-reads as absent
    /// and needs nothing more; without the re-read the retention clear would be
    /// abandoned and the row left behind forever, because the retention reminder
    /// is unregistered whether or not cleanup succeeds.
    /// </summary>
    /// <typeparam name="T">The grain state type.</typeparam>
    /// <param name="state">The persistent state to clear.</param>
    /// <param name="stillClearable">
    /// Decides whether the freshly re-read state may still be cleared (for
    /// example, it is still terminal).
    /// </param>
    internal static async Task ClearRecoveringConflictAsync<T>(IPersistentState<T> state, Func<T, bool> stillClearable)
    {
        try
        {
            await state.ClearStateAsync();
            return;
        }
        catch (Exception ex) when (IsConflict(ex))
        {
        }

        await state.ReadStateAsync();
        if (state.RecordExists && stillClearable(state.State))
        {
            await state.ClearStateAsync();
        }
    }

    /// <summary>
    /// Whether an exception type lives in an assembly every Lattice client
    /// references: the BCL, the Orleans core and serialization assemblies, or
    /// the core Lattice library itself. A storage provider's assembly
    /// (including a Lattice storage package) is not on the list.
    /// </summary>
    private static bool IsClientLoadable(Type type)
    {
        var name = type.Assembly.GetName().Name;
        if (name is null)
        {
            return false;
        }

        return name is "System" or "mscorlib" or "netstandard" or "Orleans.Lattice"
            || name.StartsWith("System.", StringComparison.Ordinal)
            || name.StartsWith("Microsoft.Extensions.", StringComparison.Ordinal)
            || name.StartsWith("Orleans.Core", StringComparison.Ordinal)
            || name.StartsWith("Orleans.Serialization", StringComparison.Ordinal);
    }
}
