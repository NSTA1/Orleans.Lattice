using System.Runtime.CompilerServices;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Process-local side channel by which a WAL storage provider reports that an
/// append it surfaced as faulted may still land (issue #6116). A faulted call is
/// not proof that nothing was written: a request the client abandoned on a
/// per-attempt network timeout, or one answered by a transient fault, can still
/// commit on the service after the call returns. The WAL shard treats such a
/// fault as an abandoned window, exactly like a flush whose deadline gave up on
/// a call still in motion (issues #4621, #5799), until <see cref="Record.Settled"/>
/// completes.
/// </summary>
/// <remarks>
/// The record is keyed weakly on the surfaced exception instance, which the
/// provider rethrows unchanged, so every caller keeps seeing the exception type it
/// saw before, and nothing new crosses a grain boundary or a serializer. Only the
/// fault path allocates.
/// </remarks>
internal static class WalAppendOutcomeUnknown
{
    private static readonly ConditionalWeakTable<Exception, Record> Records = new();

    /// <summary>
    /// Marks <paramref name="fault"/> as surfacing an append whose outcome is unknown.
    /// <paramref name="settled"/> completes once no write of the batch can still land,
    /// and must never fault. <paramref name="attempts"/> is the number of submits the
    /// provider made and <paramref name="clientAbandonedAttempts"/> how many of them
    /// the client gave up on while the request could still be in motion.
    /// </summary>
    internal static void Mark(Exception fault, Task settled, int attempts, int clientAbandonedAttempts)
    {
        ArgumentNullException.ThrowIfNull(fault);
        ArgumentNullException.ThrowIfNull(settled);
        Records.AddOrUpdate(fault, new Record(settled, attempts, clientAbandonedAttempts));
    }

    /// <summary>
    /// Returns the record of <paramref name="fault"/> or of any exception it wraps,
    /// or <see langword="null"/> when the fault proves nothing of the batch can land.
    /// </summary>
    internal static Record? Find(Exception? fault)
    {
        for (var current = fault; current is not null; current = current.InnerException)
        {
            if (Records.TryGetValue(current, out var record))
            {
                return record;
            }
        }

        return null;
    }

    /// <summary>The unknown outcome of one surfaced append fault.</summary>
    /// <param name="Settled">Completes once no write of the batch can still land.</param>
    /// <param name="Attempts">Submits the provider made before surfacing the fault.</param>
    /// <param name="ClientAbandonedAttempts">Submits the client gave up on while the request could still be in motion.</param>
    internal sealed record Record(Task Settled, int Attempts, int ClientAbandonedAttempts);
}
