using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Holds the source-version and coherent logical-row reads against an armed
/// source tree, so a test can act on the view while a range-delete pass is
/// between listing a page of view keys and deleting it (issue #4885).
/// </summary>
internal sealed class RawVersionReadHoldFilter : IIncomingGrainCallFilter
{
    private static readonly ConcurrentDictionary<string, Gate> Gates = new(StringComparer.Ordinal);

    /// <summary>
    /// Arms a hold for <paramref name="physicalTreeId"/>: every source admission read
    /// against it that asks for <paramref name="triggerKey"/> waits for
    /// <see cref="Gate.Release"/>, and every read against it is delayed by
    /// <paramref name="delay"/>.
    /// </summary>
    public static Gate Arm(string physicalTreeId, string triggerKey, TimeSpan delay)
    {
        var gate = new Gate(triggerKey, delay);
        Gates[physicalTreeId] = gate;
        return gate;
    }

    /// <summary>Removes the hold for <paramref name="physicalTreeId"/>.</summary>
    public static void Disarm(string physicalTreeId)
    {
        if (Gates.TryRemove(physicalTreeId, out var gate))
        {
            gate.Release();
        }
    }

    public async Task Invoke(IIncomingGrainCallContext context)
    {
        if (context.InterfaceMethod.DeclaringType == typeof(IShardRootGrain)
            && (context.InterfaceMethod.Name == nameof(IShardRootGrain.GetRawVersionsAsync)
                || context.InterfaceMethod.Name == nameof(IShardRootGrain.GetVisibleRawEntriesAsync)))
        {
            var key = context.TargetId.Key.ToString()!;
            var slash = key.LastIndexOf('/');
            if (slash > 0 && Gates.TryGetValue(key[..slash], out var gate))
            {
                Interlocked.Increment(ref gate.Reads);
                gate.Enter();
                try
                {
                    if (context.Request.GetArgument(0) is List<string> keys && keys.Contains(gate.TriggerKey))
                    {
                        gate.Entered.TrySetResult();
                        await gate.Released.Task;
                    }

                    await Task.Delay(gate.Delay);
                    await context.Invoke();
                }
                finally
                {
                    gate.Exit();
                }

                return;
            }
        }

        await context.Invoke();
    }

    /// <summary>A hold on one source tree's raw version reads.</summary>
    internal sealed class Gate(string triggerKey, TimeSpan delay)
    {
        public int Reads;

        private int _inFlight;

        private int _maxInFlight;

        /// <summary>The most raw version reads observed in flight at once.</summary>
        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        public void Enter()
        {
            var now = Interlocked.Increment(ref _inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref _maxInFlight))
                && Interlocked.CompareExchange(ref _maxInFlight, now, seen) != seen)
            {
            }
        }

        public void Exit() => Interlocked.Decrement(ref _inFlight);

        public string TriggerKey { get; } = triggerKey;

        public TimeSpan Delay { get; } = delay;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => Released.TrySetResult();
    }
}