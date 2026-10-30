using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Seeding a change-observation cursor reads every WAL partition concurrently,
/// so opening or resuming a subscription costs one WAL round trip rather than
/// one per partition.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    private const int SeedPartitions = 4;

    [Test]
    public async Task Fresh_subscription_reads_every_partition_tail_concurrently()
    {
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (grainFactory, tails) = SeedTopology(gate, liveCount: 0);

        await AssertSeedIsConcurrentAsync(grainFactory, continuationToken: null, gate, tails);
    }

    [Test]
    public async Task Resumed_subscription_checks_every_partition_retention_concurrently()
    {
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (grainFactory, tails) = SeedTopology(gate, liveCount: 5);
        var token = Convert.ToBase64String(Encoding.ASCII.GetBytes("1|0|0|0|0"));

        await AssertSeedIsConcurrentAsync(grainFactory, token, gate, tails);
    }

    [Test]
    public void Resumed_subscription_still_expires_when_any_partition_trimmed_past_its_offset()
    {
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        gate.SetResult(5);
        var (grainFactory, _) = SeedTopology(gate, liveCount: 5, trimmedPartition: 2);
        var token = Convert.ToBase64String(Encoding.ASCII.GetBytes("1|0|0|0|0"));

        Assert.ThrowsAsync<LatticeStateCursorExpiredException>(async () =>
        {
            await using var enumerator = CreateSeedObserver(grainFactory)
                .ObserveAsync(new StateObserveRequest { TreeId = "tree", ContinuationToken = token }, CancellationToken.None)
                .GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
        });
    }

    private static async Task AssertSeedIsConcurrentAsync(
        IGrainFactory grainFactory,
        string? continuationToken,
        TaskCompletionSource<long> gate,
        int[] tails)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var enumerator = CreateSeedObserver(grainFactory)
            .ObserveAsync(new StateObserveRequest { TreeId = "tree", ContinuationToken = continuationToken }, cts.Token)
            .GetAsyncEnumerator(cts.Token);
        try
        {
            var first = enumerator.MoveNextAsync().AsTask();

            // Every partition's tail read is in flight before any completes: a
            // serial seed would issue the second only after the first returned.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (Volatile.Read(ref tails[0]) < SeedPartitions && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            var inFlight = Volatile.Read(ref tails[0]);
            gate.SetResult(5);

            Assert.That(inFlight, Is.EqualTo(SeedPartitions), "peak concurrent WAL tail reads while seeding the cursor");
            Assert.That(await first, Is.True);
            Assert.That(enumerator.Current.Key, Is.EqualTo("key"));
            for (var p = 0; p < SeedPartitions; p++)
            {
                var wal = grainFactory.GetGrain<IWalShardGrain>($"tree/{p}");
                if (continuationToken is not null)
                {
                    await wal.Received(1).GetResumeFloorAsync(Arg.Any<CancellationToken>());
                    _ = wal.DidNotReceive().GetNextSequenceAsync(Arg.Any<CancellationToken>());
                }
                else
                {
                    _ = wal.Received(1).GetNextSequenceAsync(Arg.Any<CancellationToken>());
                    await wal.DidNotReceive().GetResumeFloorAsync(Arg.Any<CancellationToken>());
                }
            }
        }
        finally
        {
            await cts.CancelAsync();
            await enumerator.DisposeAsync();
        }
    }

    private static (IGrainFactory GrainFactory, int[] Tails) SeedTopology(
        TaskCompletionSource<long> gate,
        long liveCount,
        int trimmedPartition = -1)
    {
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync("tree").Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { WalPartitions = SeedPartitions }));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("tree").Returns(tree);
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var tails = new int[1];
        for (var p = 0; p < SeedPartitions; p++)
        {
            var wal = Substitute.For<IWalShardGrain>();
            wal.GetNextSequenceAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                Interlocked.Increment(ref tails[0]);
                return new ValueTask<long>(gate.Task);
            });
            var retained = p == trimmedPartition ? 1L : liveCount;
            wal.GetResumeFloorAsync(Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                Interlocked.Increment(ref tails[0]);
                return await gate.Task - retained;
            });
            wal.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns(p == 0
                ? new ValueTask<WalShardPage>(WalPage(new WalShardSequencedEntry
                {
                    Sequence = 5,
                    Entry = new WalRecord { Op = MutationKind.Set, Key = "key" },
                }))
                : new ValueTask<WalShardPage>(new WalShardPage { Entries = [], NextSequence = 5 }));
            grainFactory.GetGrain<IWalShardGrain>($"tree/{p}").Returns(wal);
        }

        return (grainFactory, tails);
    }

    private static LatticeStateObserver CreateSeedObserver(IGrainFactory grainFactory) => new(
        grainFactory,
        OptionsMonitor(),
        Options.Create(new LatticeApiStateOptions { ChangeObservationPollInterval = TimeSpan.Zero }),
        new ServiceCollection().BuildServiceProvider(),
        new NullTenantContextResolver());
}
