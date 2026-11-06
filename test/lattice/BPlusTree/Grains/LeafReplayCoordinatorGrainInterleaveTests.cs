using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for issue #5877: the per-partition
/// <see cref="ILeafReplayCoordinatorGrain"/> must serve concurrent reads
/// concurrently.
/// <para>
/// One coordinator activation serves every leaf replaying a WAL partition, and a
/// slice read spans many storage round trips. While the coordinator was
/// non-reentrant a silo served at most one slice read per partition at a time:
/// on the Layer 3 rig, raising the replay permits from 4 to 64 bought 4.7 times
/// the storage reads and made every replay about 2.5 times slower.
/// </para>
/// <para>
/// The reader under the coordinator is replaced with one that parks every call
/// until the test releases it, so "two calls were inside the reader at once" is
/// observed directly rather than inferred from timing.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class LeafReplayCoordinatorGrainInterleaveTests
{
    private static readonly TimeSpan OverlapBudget = TimeSpan.FromSeconds(10);
    private TestCluster _cluster = null!;
    private ParkingCommitLogReader _reader = null!;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _reader = (ParkingCommitLogReader)((InProcessSiloHandle)_cluster.Primary).SiloHost.Services
            .GetRequiredService<ICommitLogReader>();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        _reader?.ReleaseAll();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [SetUp]
    public void SetUp() => _reader.Reset();

    public enum CoordinatorCall
    {
        UnfilteredSlice,
        FilteredSlice,
        Head,
        Tail,
    }

    /// <summary>
    /// A second call reaches the reader while a slice read on the same
    /// coordinator is still parked inside it. Fails on a non-reentrant
    /// coordinator, where the second call waits for the first to return.
    /// </summary>
    [Test]
    public async Task A_call_overlaps_a_slice_read_parked_on_the_same_coordinator(
        [Values] CoordinatorCall second)
    {
        var coordinator = _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"interleave-{second}-{Guid.NewGuid():N}/0");
        var first = coordinator.ReadSliceAsync(-1, 100, 16, new WalKeyFilter("a", "m"), CancellationToken.None);
        await _reader.WaitForInFlightAsync(1, OverlapBudget);

        var other = Issue(coordinator, second);
        var overlapped = await _reader.TryWaitForInFlightAsync(2, OverlapBudget);
        _reader.ReleaseAll();
        await Task.WhenAll(first, other);

        Assert.That(overlapped, Is.True,
            $"a {second} call must run while a slice read on the same coordinator is in flight (issue #5877); "
            + $"peak calls inside the reader: {_reader.PeakInFlight}");
    }

    /// <summary>
    /// Interleaved reads of different windows on one coordinator each return
    /// exactly their own window: its slice cache is never served across windows
    /// or torn by a concurrent read.
    /// </summary>
    [Test]
    public async Task Interleaved_reads_of_different_windows_each_return_their_own_window()
    {
        _reader.ReleaseAll();
        _reader.Jitter = true;
        var coordinator = _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"interleave-windows-{Guid.NewGuid():N}/0");

        // Arrivals are staggered so that new reads land while earlier ones are
        // between their cache check and their cache write: the window in which
        // a cache written across an await would serve one window's entries
        // under another's key.
        var random = new Random(5877);
        var reads = new List<(long From, long To, Task<IReadOnlyList<CommitLogSliceEntry>> Read)>();
        for (var i = 0; i < 160; i++)
        {
            var from = (long)random.Next(0, 6) * 8 - 1;
            var to = from + 1 + random.Next(0, 4) * 2;
            reads.Add((from, to, coordinator.ReadSliceAsync(from, to, 1024, CancellationToken.None)));
            if (random.Next(3) == 0)
                await Task.Delay(1);
        }

        await Task.WhenAll(reads.Select(r => r.Read));

        Assert.Multiple(() =>
        {
            foreach (var (from, to, read) in reads)
            {
                Assert.That(read.Result.Select(e => e.Offset),
                    Is.EqualTo(Enumerable.Range((int)from + 1, (int)(to - from)).Select(o => (long)o)),
                    $"window ({from}, {to}]");
            }
        });
        Assert.That(_reader.PeakInFlight, Is.GreaterThan(1),
            "instrument validation: the reads must actually have interleaved");
    }

    private static Task Issue(ILeafReplayCoordinatorGrain coordinator, CoordinatorCall call) => call switch
    {
        CoordinatorCall.UnfilteredSlice => coordinator.ReadSliceAsync(-1, 50, 16, CancellationToken.None),
        CoordinatorCall.FilteredSlice => coordinator.ReadSliceAsync(-1, 100, 16, new WalKeyFilter("m", "z"), CancellationToken.None),
        CoordinatorCall.Head => coordinator.GetHeadOffsetAsync(CancellationToken.None),
        CoordinatorCall.Tail => coordinator.GetTailOffsetAsync(CancellationToken.None),
        _ => throw new ArgumentOutOfRangeException(nameof(call)),
    };

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Registered before AddLattice, whose TryAddSingleton then keeps it.
            siloBuilder.Services.AddSingleton<ICommitLogReader, ParkingCommitLogReader>();
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        }
    }

    /// <summary>
    /// A commit log of offsets 0..999 whose every call parks until released, and
    /// which counts how many calls are inside it at once.
    /// </summary>
    private sealed class ParkingCommitLogReader : ICommitLogReader
    {
        private readonly object _gate = new();
        private TaskCompletionSource _release = NewRelease();
        private int _inFlight;
        private readonly List<(int Count, TaskCompletionSource Reached)> _waiters = [];

        public int PeakInFlight { get; private set; }

        public bool Jitter { get; set; }

        public void Reset()
        {
            lock (_gate)
            {
                _release.TrySetResult();
                _release = NewRelease();
                PeakInFlight = 0;
                Jitter = false;
                _waiters.Clear();
            }
        }

        public void ReleaseAll()
        {
            lock (_gate)
            {
                _release.TrySetResult();
            }
        }

        public async Task WaitForInFlightAsync(int count, TimeSpan budget)
            => Assert.That(await TryWaitForInFlightAsync(count, budget), Is.True,
                $"instrument validation: {count} call(s) must reach the reader");

        public async Task<bool> TryWaitForInFlightAsync(int count, TimeSpan budget)
        {
            TaskCompletionSource reached;
            lock (_gate)
            {
                if (_inFlight >= count)
                    return true;
                reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add((count, reached));
            }

            return await Task.WhenAny(reached.Task, Task.Delay(budget)) == reached.Task;
        }

        public async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadAsync(
            string treeId,
            int shardIndex,
            long fromOffsetExclusive,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await ParkAsync();
            for (var offset = Math.Max(0, fromOffsetExclusive + 1); offset < 1000; offset++)
            {
                yield return (offset, new LatticeMutation
                {
                    TreeId = treeId,
                    Kind = MutationKind.Set,
                    Key = ((char)('a' + (offset % 26))).ToString(),
                    Value = [(byte)offset],
                });
            }
        }

        public async Task<long> GetHeadOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
        {
            await ParkAsync();
            return 1000;
        }

        public async Task<long> GetTailOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
        {
            await ParkAsync();
            return 0;
        }

        private async Task ParkAsync()
        {
            Task release;
            lock (_gate)
            {
                _inFlight++;
                PeakInFlight = Math.Max(PeakInFlight, _inFlight);
                foreach (var (count, reached) in _waiters)
                {
                    if (_inFlight >= count)
                        reached.TrySetResult();
                }

                release = _release.Task;
            }

            try
            {
                await release;
                if (Jitter)
                    await Task.Delay(Random.Shared.Next(0, 3));
                else
                    await Task.Yield();
            }
            finally
            {
                lock (_gate)
                {
                    _inFlight--;
                }
            }
        }

        private static TaskCompletionSource NewRelease()
            => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
