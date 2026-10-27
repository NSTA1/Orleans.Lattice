using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private sealed class BlockedLeafStateStorage(
        int blockedReadCount,
        bool initialReadCompletes,
        bool synchronousRelease = false) : IGrainStorage
    {
        // Some runner tests need Release to drain the provider-read continuations
        // before immediately retrying admission, not just enqueue them.
        private readonly TaskCompletionSource _release = new(synchronousRelease
            ? TaskCreationOptions.None : TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _firstBatch = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _reads;
        private int _outstanding;
        private int _maximumOutstanding;

        public Task FirstBatch => _firstBatch.Task;
        public int Reads => Volatile.Read(ref _reads);
        public int Outstanding => Volatile.Read(ref _outstanding);
        public int MaximumOutstanding => Volatile.Read(ref _maximumOutstanding);
        public bool CompleteNewReads { get; set; }

        public void Release() => _release.TrySetResult();

        public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var read = Interlocked.Increment(ref _reads);
            if (CompleteNewReads || (initialReadCompletes && read == 1))
            {
                grainState.RecordExists = false;
                return;
            }

            var outstanding = Interlocked.Increment(ref _outstanding);
            int maximum;
            do
            {
                maximum = Volatile.Read(ref _maximumOutstanding);
                if (outstanding <= maximum)
                    break;
            }
            while (Interlocked.CompareExchange(ref _maximumOutstanding, outstanding, maximum) != maximum);

            if (read == blockedReadCount + (initialReadCompletes ? 1 : 0))
                _firstBatch.TrySetResult();

            try
            {
                await _release.Task;
                grainState.RecordExists = false;
            }
            finally
            {
                Interlocked.Decrement(ref _outstanding);
            }
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            throw new NotSupportedException();

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            throw new NotSupportedException();
    }

    [TestCase(false, 1)]
    public async Task Orphan_sweep_blocked_provider_reads_do_not_accumulate_across_collection_budgets(
        bool initialReadCompletes,
        int blockedReadCount)
    {
        const int ReadConcurrency = 16;
        const int Pins = 40;
        var time = new VirtualTimeProvider();
        var storage = new BlockedLeafStateStorage(blockedReadCount, initialReadCompletes);
        var pins = new FakePinStore();
        for (var i = 0; i < Pins; i++)
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i));

        using var scheduler = SchedulerSweeping(pins, storage, time);
        await StartArmedAsync(scheduler, time);
        time.Advance(time.LastScheduledDelay);

        try
        {
            await Parked(storage.FirstBatch);
            var timersBefore = time.TimersCreated;

            // Six collection budgets and many sweep intervals pass while the
            // provider ignores cancellation and leaves every read outstanding.
            time.Advance(TimeSpan.FromHours(1));

            Assert.Multiple(() =>
            {
                Assert.That(storage.Reads, Is.EqualTo(blockedReadCount + (initialReadCompletes ? 1 : 0)));
                Assert.That(storage.Outstanding, Is.EqualTo(blockedReadCount));
                Assert.That(storage.MaximumOutstanding, Is.EqualTo(blockedReadCount));
                Assert.That(time.TimersCreated, Is.EqualTo(timersBefore),
                    "The direct-read sweep has no abandonment timer; the next pass cannot overlap it.");
                Assert.That(pins.RemainingPins, Is.EqualTo(Pins),
                    "An incomplete read is not evidence that its publisher is gone.");
            });

            var nextPass = time.NextTimerAsync();
            storage.Release();
            await Parked(nextPass);

            Assert.Multiple(() =>
            {
                Assert.That(storage.Reads, Is.EqualTo(Pins + 1));
                Assert.That(storage.Outstanding, Is.Zero);
                Assert.That(storage.MaximumOutstanding, Is.LessThanOrEqualTo(ReadConcurrency));
                Assert.That(pins.RemainingPins, Is.Zero,
                    "Once reads complete as missing, subsequent batches must drain normally.");
            });
        }
        finally
        {
            storage.Release();
            await scheduler.StopAsync(CancellationToken.None);
        }
    }
}
