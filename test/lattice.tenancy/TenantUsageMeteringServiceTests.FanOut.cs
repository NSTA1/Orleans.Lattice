using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tenancy.Tests;

public sealed partial class TenantUsageMeteringServiceTests
{
    [TestCase(4, false)]
    [TestCase(40, false)]
    [TestCase(4, true)]
    public async Task Metering_overlaps_owned_tree_reads_with_a_bound_and_publishes_only_after_sampling(int trees, bool cold)
    {
        var ids = Enumerable.Range(0, trees).Select(i => $"t/acme/tree-{i}").ToArray();
        var factory = GrainFactoryWith(ids);
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, factory);
        var width = Math.Min(trees, BoundedFanOut.DefaultWidth);
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        var refreshed = 0;
        foreach (var id in ids)
        {
            factory.GetGrain<ILatticeStorageUsage>(id)
                .GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
                .Returns(call => ReadAsync(call.Arg<bool>()));
        }

        var cycle = service.MeterOnceAsync(CancellationToken.None);
        try
        {
            await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(entered, Is.EqualTo(width));
            Assert.That(store.Published, Is.Empty);
        }
        finally
        {
            release.TrySetResult();
            await cycle.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.That(entered, Is.EqualTo(trees));
        Assert.That(refreshed, Is.EqualTo(cold ? trees : 0));
        Assert.That(store.Published, Has.Count.EqualTo(1));
        var sample = store.Published[0].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(sample.Bytes, Is.EqualTo(100L * trees));
            Assert.That(sample.Keys, Is.EqualTo(10L * trees));
            Assert.That(sample.MemoryBytes, Is.EqualTo(50L * trees));
            Assert.That(sample.TreeCount, Is.EqualTo(trees));
        });

        async Task<TreeStorageUsageReport> ReadAsync(bool forceRefresh)
        {
            if (forceRefresh)
            {
                Interlocked.Increment(ref refreshed);
                return new TreeStorageUsageReport { TotalBytes = 100, LiveKeys = 10, LeafStateBytes = 50 };
            }
            if (Interlocked.Increment(ref entered) == width) arrived.TrySetResult();
            await release.Task;
            return new TreeStorageUsageReport
            {
                TotalBytes = 100,
                LiveKeys = cold ? 0 : 10,
                LeafStateBytes = cold ? 0 : 50,
            };
        }
    }

    [Test]
    public async Task Metering_cancellation_settles_the_launched_wave_without_publishing()
    {
        var ids = Enumerable.Range(0, 40).Select(i => $"t/acme/tree-{i}").ToArray();
        var factory = GrainFactoryWith(ids);
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, factory);
        using var cancellation = new CancellationTokenSource();
        var release = new TaskCompletionSource<TreeStorageUsageReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        foreach (var id in ids)
        {
            factory.GetGrain<ILatticeStorageUsage>(id)
                .GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
                .Returns(_ => { entered++; return release.Task; });
        }

        var cycle = service.MeterOnceAsync(cancellation.Token);
        Assert.That(entered, Is.EqualTo(BoundedFanOut.DefaultWidth));
        cancellation.Cancel();
        Assert.That(cycle.IsCompleted, Is.False, "Launched reads must settle before cancellation escapes.");
        release.SetResult(new TreeStorageUsageReport { TotalBytes = 100, LiveKeys = 10 });
        Assert.That(async () => await cycle, Throws.InstanceOf<OperationCanceledException>());
        Assert.That(store.Published, Is.Empty);
        Assert.That(entered, Is.EqualTo(BoundedFanOut.DefaultWidth));
    }
}
