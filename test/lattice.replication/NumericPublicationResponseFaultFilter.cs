using System.Collections.Concurrent;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Loses a response only after the numeric membership and intent flip actually commits.</summary>
internal sealed class NumericPublicationResponseFaultFilter : IOutgoingGrainCallFilter
{
    private static readonly ConcurrentDictionary<string, Fault> Faults = new(StringComparer.Ordinal);

    public static Fault Arm(string treeId, string intentKey)
    {
        var fault = new Fault(intentKey);
        if (!Faults.TryAdd(treeId, fault))
        {
            throw new InvalidOperationException($"Numeric publication for '{treeId}' is already armed.");
        }
        return fault;
    }

    public static void Disarm(string treeId) => Faults.TryRemove(treeId, out _);

    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        var treeId = context.TargetId.Key.ToString()!;
        if (context.InterfaceMethod.DeclaringType == typeof(ILattice)
            && context.InterfaceMethod.Name == nameof(ILattice.SetManyAtomicAsync)
            && Faults.TryGetValue(treeId, out var fault)
            && context.Request.GetArgument(0) is List<KeyValuePair<string, byte[]>> writes
            && writes.Any(write => string.Equals(write.Key, fault.IntentKey, StringComparison.Ordinal))
            && Faults.TryRemove(treeId, out _))
        {
            await context.Invoke();
            fault.Committed = true;
            throw new TimeoutException("Lost numeric membership and intent publication response.");
        }

        await context.Invoke();
    }

    internal sealed class Fault(string intentKey)
    {
        public string IntentKey { get; } = intentKey;
        public bool Committed { get; set; }
    }
}
