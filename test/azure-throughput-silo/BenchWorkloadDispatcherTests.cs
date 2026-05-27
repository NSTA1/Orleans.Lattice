using NSubstitute;
using Orleans.Lattice;
using VehicleFleetSimulator.AzureThroughput.Silo;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the per-mode dispatch surface of <see cref="BenchWorkloadDispatcher"/>.
/// Every <c>ILattice</c> op the silo dispatches lands here, and the
/// throughput report depends on each row touching the right method
/// with the right call count and slice shape. See
/// throughput-capture-plan.md step 7.
/// </summary>
[TestFixture]
public class BenchWorkloadDispatcherTests
{
    private const int BatchCount = 256;

    private static List<KeyValuePair<string, byte[]>> BuildBatch(int count = BatchCount)
    {
        var batch = new List<KeyValuePair<string, byte[]>>(count);
        for (var i = 0; i < count; i++)
        {
            batch.Add(new KeyValuePair<string, byte[]>($"k{i:D5}", new byte[] { (byte)(i & 0xFF) }));
        }
        return batch;
    }

    [Test]
    public async Task SetMany_dispatches_one_batched_call()
    {
        var lattice = Substitute.For<ILattice>();
        var batch = BuildBatch();

        var ops = await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.SetMany, lattice, batch,
            atomicBatchSize: 64, parallelism: 8, ct: default);

        Assert.That(ops, Is.EqualTo(BatchCount));
        await lattice.Received(1).SetManyAsync(batch, Arg.Any<CancellationToken>());
        await lattice.DidNotReceiveWithAnyArgs().SetManyAtomicAsync(default!, default(CancellationToken));
        await lattice.DidNotReceiveWithAnyArgs().SetAsync(default!, default!, default(CancellationToken));
        await lattice.DidNotReceiveWithAnyArgs().GetAsync(default!, default(CancellationToken));
        await lattice.DidNotReceiveWithAnyArgs().GetManyAsync(default!, default(CancellationToken));
    }

    [Test]
    public async Task SetManyAtomic_slices_batch_to_atomicBatchSize()
    {
        var lattice = Substitute.For<ILattice>();
        var batch = BuildBatch();
        const int atomicBatchSize = 64;

        var ops = await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.SetManyAtomic, lattice, batch,
            atomicBatchSize: atomicBatchSize, parallelism: 8, ct: default);

        Assert.That(ops, Is.EqualTo(BatchCount));
        // 256 / 64 = 4 atomic sagas, each carrying exactly 64 keys.
        var expectedSagas = BatchCount / atomicBatchSize;
        await lattice.Received(expectedSagas).SetManyAtomicAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(s => s.Count == atomicBatchSize),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetManyAtomic_remainder_slice_carries_residual_keys()
    {
        var lattice = Substitute.For<ILattice>();
        // 150 keys split by 64 -> [64, 64, 22] (last slice is the remainder)
        var batch = BuildBatch(150);

        var ops = await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.SetManyAtomic, lattice, batch,
            atomicBatchSize: 64, parallelism: 8, ct: default);

        Assert.That(ops, Is.EqualTo(150));
        await lattice.Received(3).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<CancellationToken>());
        // The remainder saga must carry exactly 22 keys, not silently grow
        // or shrink. Pin it explicitly because off-by-one errors at the
        // end of a slice loop are the most likely regression site.
        await lattice.Received(1).SetManyAtomicAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(s => s.Count == 22),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetPoint_dispatches_one_SetAsync_per_entry()
    {
        var lattice = Substitute.For<ILattice>();
        var batch = BuildBatch();

        var ops = await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.SetPoint, lattice, batch,
            atomicBatchSize: 64, parallelism: 8, ct: default);

        Assert.That(ops, Is.EqualTo(BatchCount));
        await lattice.Received(BatchCount).SetAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
        // Verify the key/value pairs are passed through unchanged for the
        // first and last entries (covers off-by-one and the parallel
        // fan-out's argument capture).
        var firstKey = batch[0].Key;
        var firstVal = batch[0].Value;
        var lastKey = batch[batch.Count - 1].Key;
        var lastVal = batch[batch.Count - 1].Value;
        await lattice.Received(1).SetAsync(firstKey, firstVal, Arg.Any<CancellationToken>());
        await lattice.Received(1).SetAsync(lastKey, lastVal, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetPoint_dispatches_one_GetAsync_per_entry()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<byte[]?>(null));
        var batch = BuildBatch();

        var ops = await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.GetPoint, lattice, batch,
            atomicBatchSize: 64, parallelism: 8, ct: default);

        Assert.That(ops, Is.EqualTo(BatchCount));
        await lattice.Received(BatchCount).GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await lattice.DidNotReceiveWithAnyArgs().SetAsync(default!, default!, default(CancellationToken));
    }

    [Test]
    public async Task GetMany_dispatches_one_batched_call_with_full_key_list()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new Dictionary<string, byte[]>()));
        var batch = BuildBatch();

        var ops = await BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.GetMany, lattice, batch,
            atomicBatchSize: 64, parallelism: 8, ct: default);

        Assert.That(ops, Is.EqualTo(BatchCount));
        var firstKey = batch[0].Key;
        var lastKey = batch[batch.Count - 1].Key;
        await lattice.Received(1).GetManyAsync(
            Arg.Is<List<string>>(keys => keys.Count == BatchCount && keys[0] == firstKey && keys[keys.Count - 1] == lastKey),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetPoint_respects_parallelism_cap()
    {
        // Throughput-capture step 7 done-when: point modes must respect
        // the parallelism cap. We instrument the lattice with a
        // controllable gate that records in-flight depth on each SetAsync
        // call and only releases when explicitly signalled; the test
        // then asserts the observed in-flight peak never exceeded the
        // configured cap.
        const int Parallelism = 4;
        var inFlight = 0;
        var peak = 0;
        var lockObj = new object();
        var releaseGate = new TaskCompletionSource();

        var lattice = Substitute.For<ILattice>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                lock (lockObj)
                {
                    inFlight++;
                    if (inFlight > peak) peak = inFlight;
                }
                // Yield so the fan-out gets a chance to dispatch more
                // SetAsync calls before any in-flight one completes; the
                // gate then releases all of them together once we've
                // had time to observe the steady-state in-flight depth.
                await releaseGate.Task.ConfigureAwait(false);
                lock (lockObj)
                {
                    inFlight--;
                }
            });

        var batch = BuildBatch(64);
        var dispatchTask = BenchWorkloadDispatcher.DispatchAsync(
            BenchWorkloadMode.SetPoint, lattice, batch,
            atomicBatchSize: 64, parallelism: Parallelism, ct: default);

        // Let the fan-out reach steady state.
        await Task.Delay(75).ConfigureAwait(false);
        // Snapshot the peak under the lock, then release the gate so the
        // dispatcher completes.
        int peakObserved;
        lock (lockObj) peakObserved = peak;
        releaseGate.SetResult();

        await dispatchTask.ConfigureAwait(false);

        Assert.That(peakObserved, Is.LessThanOrEqualTo(Parallelism),
            $"SetPoint fan-out must respect parallelism cap; observed peak={peakObserved} cap={Parallelism}");
        Assert.That(peakObserved, Is.GreaterThan(1),
            "SetPoint fan-out should actually fan out under the cap (not serialise); observed peak=" + peakObserved);
    }

    [Test]
    public async Task Empty_batch_is_a_noop_in_every_mode()
    {
        var lattice = Substitute.For<ILattice>();
        var empty = new List<KeyValuePair<string, byte[]>>();

        foreach (var mode in (BenchWorkloadMode[])Enum.GetValues(typeof(BenchWorkloadMode)))
        {
            var ops = await BenchWorkloadDispatcher.DispatchAsync(
                mode, lattice, empty, atomicBatchSize: 64, parallelism: 8, ct: default);
            Assert.That(ops, Is.EqualTo(0), $"mode={mode} expected 0 ops on empty batch");
        }

        await lattice.DidNotReceiveWithAnyArgs().SetManyAsync(default!, default(CancellationToken));
        await lattice.DidNotReceiveWithAnyArgs().SetManyAtomicAsync(default!, default(CancellationToken));
        await lattice.DidNotReceiveWithAnyArgs().SetAsync(default!, default!, default(CancellationToken));
        await lattice.DidNotReceiveWithAnyArgs().GetAsync(default!, default(CancellationToken));
        await lattice.DidNotReceiveWithAnyArgs().GetManyAsync(default!, default(CancellationToken));
    }

    [Test]
    public void FormatWorkloadMode_renders_kebab_case_for_every_enum_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BenchWorkloadMetadata.FormatWorkloadMode(BenchWorkloadMode.SetMany), Is.EqualTo("set-many"));
            Assert.That(BenchWorkloadMetadata.FormatWorkloadMode(BenchWorkloadMode.SetManyAtomic), Is.EqualTo("set-many-atomic"));
            Assert.That(BenchWorkloadMetadata.FormatWorkloadMode(BenchWorkloadMode.SetPoint), Is.EqualTo("set-point"));
            Assert.That(BenchWorkloadMetadata.FormatWorkloadMode(BenchWorkloadMode.GetPoint), Is.EqualTo("get-point"));
            Assert.That(BenchWorkloadMetadata.FormatWorkloadMode(BenchWorkloadMode.GetMany), Is.EqualTo("get-many"));
        });
    }
}
