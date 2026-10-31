using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The journey host's hold on writes to the secondary WAL provider, per physical tree (A12,
/// issue #5382): while a tree is held, every write and tail read a WAL move makes against its
/// target waits, as it would against a target storage account that has stopped answering. A
/// journey holds the target, starts a move, sees it waiting in its copy phase and stops it
/// before its flip; then the harness releases the hold. Reachable only when the fault seams
/// are enabled, and process-wide, so the harness sets it on every silo of the region.
/// </summary>
internal static class JourneyWalTargetGate
{
    /// <summary>The longest a hold stays in force, so a forgotten one cannot wedge a silo.</summary>
    public static readonly TimeSpan MaximumHold = TimeSpan.FromMinutes(5);

    private static readonly ConcurrentDictionary<string, Hold> Holds = new(StringComparer.Ordinal);

    /// <summary>Whether <paramref name="physicalTreeId"/> is held now.</summary>
    /// <param name="physicalTreeId">The physical tree id.</param>
    public static bool IsHeld(string physicalTreeId) => Holds.TryGetValue(physicalTreeId, out var hold) && !hold.Released.Task.IsCompleted;

    /// <summary>Holds writes to the secondary provider for <paramref name="physicalTreeId"/>.</summary>
    /// <param name="physicalTreeId">The physical tree id.</param>
    public static void HoldTree(string physicalTreeId)
    {
        var hold = new Hold();
        if (Holds.TryGetValue(physicalTreeId, out var previous))
        {
            previous.Released.TrySetResult();
        }

        Holds[physicalTreeId] = hold;
        _ = ExpireAsync(physicalTreeId, hold);
    }

    /// <summary>Releases <paramref name="physicalTreeId"/>, letting every waiting call through.</summary>
    /// <param name="physicalTreeId">The physical tree id.</param>
    public static void Release(string physicalTreeId)
    {
        if (Holds.TryRemove(physicalTreeId, out var hold))
        {
            hold.Released.TrySetResult();
        }
    }

    /// <summary>Waits while <paramref name="physicalTreeId"/> is held; returns at once when it is not.</summary>
    /// <param name="physicalTreeId">The physical tree id.</param>
    /// <param name="cancellationToken">The caller's token: a cancelled move stops waiting at once.</param>
    public static Task WaitAsync(string physicalTreeId, CancellationToken cancellationToken) =>
        Holds.TryGetValue(physicalTreeId, out var hold) && !hold.Released.Task.IsCompleted
            ? hold.Released.Task.WaitAsync(cancellationToken)
            : Task.CompletedTask;

    private static async Task ExpireAsync(string physicalTreeId, Hold hold)
    {
        await Task.WhenAny(hold.Released.Task, Task.Delay(MaximumHold)).ConfigureAwait(false);
        if (hold.Released.TrySetResult())
        {
            Holds.TryRemove(new KeyValuePair<string, Hold>(physicalTreeId, hold));
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
