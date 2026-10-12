using System.Collections.Concurrent;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Holds a history batch at its actual outgoing atomic publication seam.</summary>
internal sealed class HistoryPublicationHoldFilter(IGrainFactory grainFactory) : IOutgoingGrainCallFilter
{
    private static readonly ConcurrentDictionary<string, Gate> Gates = new(StringComparer.Ordinal);

    public static Gate Arm(string treeId, IReadOnlyList<string> keys, bool splitPublication)
    {
        var gate = new Gate(keys, splitPublication);
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
            && writes.Count == gate.Keys.Count
            && writes.All(write => gate.Keys.Contains(write.Key, StringComparer.Ordinal))
            && Gates.TryRemove(treeId, out _))
        {
            if (gate.SplitPublication)
            {
                // Replace only this captured batch with point publications; the
                // reader is held between them, not between two separate reads.
                var tree = grainFactory.GetGrain<ILattice>(treeId);
                await tree.SetAsync(writes[0].Key, writes[0].Value);
                gate.Entered.TrySetResult();
                await gate.Released.Task;
                for (var index = 1; index < writes.Count; index++)
                {
                    await tree.SetAsync(writes[index].Key, writes[index].Value);
                }
                context.Result = null;
                return;
            }

            gate.Entered.TrySetResult();
            await gate.Released.Task;
        }
        await context.Invoke();
    }

    internal sealed class Gate(IReadOnlyList<string> keys, bool splitPublication)
    {
        public IReadOnlyList<string> Keys { get; } = keys;
        public bool SplitPublication { get; } = splitPublication;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Release() => Released.TrySetResult();
    }
}
