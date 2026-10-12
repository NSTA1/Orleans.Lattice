using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Fails every leaf terminal apply for an armed transaction, so a test can hold a
/// committed transaction in the state where its terminal is already in the commit
/// log but no leaf has drained its prepared entries.
/// </summary>
internal sealed class LeafTerminalFaultFilter : IIncomingGrainCallFilter
{
    private static readonly ConcurrentDictionary<Guid, int> Armed = new();

    /// <summary>Fails leaf terminal applies for <paramref name="transactionId"/>.</summary>
    public static void Arm(Guid transactionId) => Armed[transactionId] = 0;

    /// <summary>Stops failing leaf terminal applies for <paramref name="transactionId"/>.</summary>
    public static void Disarm(Guid transactionId) => Armed.TryRemove(transactionId, out _);

    /// <summary>How many leaf terminal applies were failed for <paramref name="transactionId"/>.</summary>
    public static int Failures(Guid transactionId) => Armed.TryGetValue(transactionId, out var count) ? count : 0;

    public Task Invoke(IIncomingGrainCallContext context)
    {
        if (context.InterfaceMethod.DeclaringType == typeof(IBPlusLeafGrain)
            && context.InterfaceMethod.Name == nameof(IBPlusLeafGrain.ApplyTxTerminalAsync)
            && context.Request.GetArgument(0) is Guid transactionId
            && Armed.ContainsKey(transactionId))
        {
            Armed.AddOrUpdate(transactionId, 1, static (_, count) => count + 1);
            throw new InvalidOperationException($"Leaf terminal apply for '{transactionId}' failed by the test.");
        }

        return context.Invoke();
    }
}