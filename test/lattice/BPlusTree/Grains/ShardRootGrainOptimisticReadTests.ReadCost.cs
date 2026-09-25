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
        var stateMachine = typeof(ShardRootGrain).GetMethod(nameof(ShardRootGrain.TryGetOptimisticAsync))!
            .GetCustomAttribute<AsyncStateMachineAttribute>()!.StateMachineType;
        var fields = stateMachine.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        Assert.That(fields.Select(f => f.FieldType), Does.Not.Contain(typeof((Guid, long))));
        Assert.That(fields.Select(f => f.FieldType), Does.Not.Contain(typeof(TaskAwaiter<VersionedValue>)));
        var size = typeof(ShardRootGrainOptimisticReadTests)
            .GetMethod(nameof(ReadStateMachineSize), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(stateMachine).Invoke(null, null);
        TestContext.Out.WriteLine($"COST raw-read-state-machine bytes={size}");
    }

    private static int ReadStateMachineSize<T>() => Unsafe.SizeOf<T>();
}
