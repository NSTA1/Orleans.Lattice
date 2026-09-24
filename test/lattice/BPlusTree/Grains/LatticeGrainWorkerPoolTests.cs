using NUnit.Framework;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the <see cref="LatticeGrain"/> stateless-worker pool size (issue #812):
/// the attribute Orleans reads must carry <see cref="LatticeGrain.MaxLocalWorkers"/>,
/// and that value must stay above the per-caller fan-out windows sized to the
/// original 32-worker pool.
/// </summary>
[TestFixture]
public sealed class LatticeGrainWorkerPoolTests
{
    [Test]
    public void MaxLocalWorkers_is_256()
    {
        Assert.That(LatticeGrain.MaxLocalWorkers, Is.EqualTo(256));
    }

    [Test]
    public void StatelessWorker_attribute_uses_MaxLocalWorkers()
    {
        var attribute = typeof(LatticeGrain).CustomAttributes
            .SingleOrDefault(a => a.AttributeType == typeof(StatelessWorkerAttribute));

        Assert.That(attribute, Is.Not.Null);
        Assert.That(attribute!.ConstructorArguments, Has.Count.EqualTo(1));
        Assert.That(attribute.ConstructorArguments[0].Value, Is.EqualTo(LatticeGrain.MaxLocalWorkers));
    }

    [Test]
    public void Per_caller_fan_out_windows_stay_below_the_pool()
    {
        Assert.That(BoundedFanOut.DefaultWidth, Is.LessThan(LatticeGrain.MaxLocalWorkers));
    }
}
