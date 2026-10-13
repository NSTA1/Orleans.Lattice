using System.Collections.Concurrent;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Holds a history batch at its actual outgoing atomic publication seam.</summary>
internal sealed class HistoryPublicationHoldFilter(IGrainFactory grainFactory) : IOutgoingGrainCallFilter
{
    private static readonly ConcurrentDictionary<string, Gate> Gates = new(StringComparer.Ordinal);

    public static Gate Arm(
        string treeId,
        IReadOnlyList<string> keys,
        bool splitPublication,
        bool loseResponse = false)
    {
        var gate = new Gate(keys, splitPublication, loseResponse);
        if (!Gates.TryAdd(treeId, gate))
        {
            throw new InvalidOperationException($"History publication for '{treeId}' is already held.");
        }
        return gate;
    }

    public static void Disarm(string treeId)
    {
        if (Gates.TryRemove(treeId, out var gate))
        {
            gate.Release();
        }
    }

    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        var treeId = context.TargetId.Key.ToString()!;
        if (context.InterfaceMethod.DeclaringType == typeof(ILattice)
            && context.InterfaceMethod.Name == nameof(ILattice.SetManyAtomicAsync)
            && Gates.TryGetValue(treeId, out var gate)
            && context.Request.GetArgument(0) is List<KeyValuePair<string, byte[]>> writes
            // The publication slice may include unrelated groups beside the snapshot under test.
            && gate.Keys.All(key => writes.Any(write => StringComparer.Ordinal.Equals(write.Key, key)))
            && Gates.TryRemove(treeId, out _))
        {
            if (gate.SplitPublication)
            {
                // Replace only this captured batch with point publications; the
                // reader is held between them, not between two separate reads.
                var tree = grainFactory.GetGrain<ILattice>(treeId);
                var splitKey = gate.Keys[0];
                var splitWrite = writes.Single(write => StringComparer.Ordinal.Equals(write.Key, splitKey));
                await tree.SetAsync(splitWrite.Key, splitWrite.Value);
                gate.Entered.TrySetResult();
                await gate.Released.Task;
                foreach (var write in writes)
                {
                    if (!StringComparer.Ordinal.Equals(write.Key, splitKey))
                    {
                        await tree.SetAsync(write.Key, write.Value);
                    }
                }
                context.Result = null;
                return;
            }

            if (gate.LoseResponse)
            {
                await context.Invoke();
                gate.Committed = true;
                gate.Entered.TrySetResult();
                await gate.Released.Task;
                throw new TimeoutException("Lost history publication response after commit.");
            }

            gate.Entered.TrySetResult();
            await gate.Released.Task;
        }
        await context.Invoke();
    }

    internal sealed class Gate(IReadOnlyList<string> keys, bool splitPublication, bool loseResponse)
    {
        public IReadOnlyList<string> Keys { get; } = keys;
        public bool SplitPublication { get; } = splitPublication;
        public bool LoseResponse { get; } = loseResponse;
        public bool Committed { get; set; }
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Release() => Released.TrySetResult();
    }
}
