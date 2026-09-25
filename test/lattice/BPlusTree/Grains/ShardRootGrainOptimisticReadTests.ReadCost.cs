using System.Reflection;
using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    [Test]
    public void Raw_read_state_machine_does_not_capture_leaf_proof()
    {
        var readMethod = typeof(ShardRootGrain).GetMethod(nameof(ShardRootGrain.TryGetOptimisticAsync));
        Assert.That(readMethod, Is.Not.Null, "Optimistic read entry point changed; update this structural guard.");
        var stateMachineAttribute = readMethod!.GetCustomAttribute<AsyncStateMachineAttribute>();
        Assert.That(stateMachineAttribute, Is.Not.Null, "Optimistic read no longer has an async state machine; update this guard.");
        var stateMachine = stateMachineAttribute!.StateMachineType;
        var fields = stateMachine.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        Assert.That(fields.Select(f => f.FieldType), Does.Not.Contain(typeof((Guid, long))));
        Assert.That(fields.Select(f => f.FieldType), Does.Not.Contain(typeof(TaskAwaiter<VersionedValue>)));
        var sizeMethod = typeof(ShardRootGrainOptimisticReadTests)
            .GetMethod(nameof(ReadStateMachineSize), BindingFlags.Static | BindingFlags.NonPublic);
        Assert.That(sizeMethod, Is.Not.Null, "Test-only size helper changed; update this guard.");
        var size = sizeMethod!.MakeGenericMethod(stateMachine).Invoke(null, null);
        TestContext.Out.WriteLine($"COST raw-read-state-machine bytes={size}");
    }

    private static int ReadStateMachineSize<T>() => Unsafe.SizeOf<T>();
}
