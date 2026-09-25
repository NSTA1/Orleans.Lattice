using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins <see cref="BenchWorkloadDispatcher.SliceIntoFlushUnits"/>, which decides
/// the unit the ingest engine flushes, retries and accounts independently
/// (#3581): one saga per unit in the atomic modes, the whole batch otherwise.
/// </summary>
[TestFixture]
public class BenchWorkloadDispatcherFlushUnitTests
{
    private static List<KeyValuePair<string, byte[]>> BuildBatch(int count)
    {
        var batch = new List<KeyValuePair<string, byte[]>>(count);
        for (var i = 0; i < count; i++)
        {
            batch.Add(new KeyValuePair<string, byte[]>($"k{i:D5}", new byte[] { (byte)(i & 0xFF) }));
        }

        return batch;
    }

    [TestCase(BenchWorkloadMode.SetMany)]
    [TestCase(BenchWorkloadMode.SetPoint)]
    [TestCase(BenchWorkloadMode.SetPointMv)]
    [TestCase(BenchWorkloadMode.GetPoint)]
    [TestCase(BenchWorkloadMode.GetMany)]
    public void Non_atomic_modes_keep_the_batch_as_one_unit(BenchWorkloadMode mode)
    {
        var batch = BuildBatch(300);

        var units = BenchWorkloadDispatcher.SliceIntoFlushUnits(mode, batch, atomicBatchSize: 64);

        Assert.That(units, Has.Count.EqualTo(1));
        Assert.That(units[0], Is.SameAs(batch));
    }

    [TestCase(BenchWorkloadMode.SetManyAtomic2, 64, 2)]
    [TestCase(BenchWorkloadMode.CrossTreeAtomic2, 64, 2)]
    [TestCase(BenchWorkloadMode.CrossTreeAtomic64, 8, 64)]
    [TestCase(BenchWorkloadMode.SetManyAtomic, 50, 50)]
    [TestCase(BenchWorkloadMode.SetManyAtomic, 0, 1)]
    public void Atomic_modes_slice_into_one_unit_per_saga(BenchWorkloadMode mode, int atomicBatchSize, int expectedSagaSize)
    {
        var batch = BuildBatch(301);

        var units = BenchWorkloadDispatcher.SliceIntoFlushUnits(mode, batch, atomicBatchSize);

        var expectedUnits = (batch.Count + expectedSagaSize - 1) / expectedSagaSize;
        Assert.Multiple(() =>
        {
            Assert.That(units, Has.Count.EqualTo(expectedUnits));
            Assert.That(units.Take(units.Count - 1).Select(u => u.Count), Has.All.EqualTo(expectedSagaSize));
            Assert.That(units[^1].Count, Is.EqualTo(batch.Count - (expectedUnits - 1) * expectedSagaSize),
                "the final unit carries the remainder");
            Assert.That(units.SelectMany(u => u).Select(e => e.Key), Is.EqualTo(batch.Select(e => e.Key)),
                "every entry lands in exactly one unit, in batch order");
        });
    }

    [Test]
    public void A_batch_no_larger_than_one_saga_is_one_unit()
    {
        var batch = BuildBatch(2);

        var units = BenchWorkloadDispatcher.SliceIntoFlushUnits(BenchWorkloadMode.SetManyAtomic2, batch, atomicBatchSize: 64);

        Assert.That(units, Has.Count.EqualTo(1));
        Assert.That(units[0], Is.SameAs(batch));
    }

    [Test]
    public void An_empty_batch_has_no_units()
    {
        var units = BenchWorkloadDispatcher.SliceIntoFlushUnits(BenchWorkloadMode.SetManyAtomic2, new List<KeyValuePair<string, byte[]>>(), atomicBatchSize: 64);

        Assert.That(units, Is.Empty);
    }

    [Test]
    public void A_null_batch_is_rejected()
    {
        Assert.Throws<ArgumentNullException>(() =>
            BenchWorkloadDispatcher.SliceIntoFlushUnits(BenchWorkloadMode.SetManyAtomic2, null!, atomicBatchSize: 64));
    }
}
