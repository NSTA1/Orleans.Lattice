using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for
/// <see cref="AzureTableWalStorageProvider.FlushPhaseTwoAsync"/>, the
/// read-your-writes barrier that drains the per-shard pipelined
/// phase-2 slots without disposing the provider (issue #2509).
/// <para>
/// The tests drive the in-memory slot exchange
/// (<see cref="AzureTableWalStorageProvider.AwaitPreviousPipelinedAsync"/>)
/// with <see cref="TaskCompletionSource"/>-backed stand-ins for the
/// worker's phase-2 tasks, so every wait is resolved by an explicit
/// event rather than by elapsed time and no Azurite endpoint is
/// required. The end-to-end visibility behaviour these semantics buy
/// is pinned separately by
/// <c>AzureTableWalStorageProviderPipelinedVisibilityIntegrationTests</c>.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderFlushPhaseTwoTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private AzureTableWalStorageProvider CreateProvider(bool pipeline = true)
    {
        // These tests exercise only the in-memory slot exchange, so
        // the connection string just has to parse - no I/O is issued.
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "Tflush" + Guid.NewGuid().ToString("N"),
            PipelinePhaseTwoCommits = pipeline,
            Compression = LatticeCompression.None,
        };
        return new AzureTableWalStorageProvider(Options.Create(options), _serializer);
    }

    private static TaskCompletionSource NewPhaseTwo() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Test]
    public async Task FlushPhaseTwoAsync_completes_immediately_when_no_phase_two_is_outstanding()
    {
        // Nothing has been appended, so no slot is occupied. This is
        // also the shape a provider configured with
        // PipelinePhaseTwoCommits = false is permanently in.
        await using var sut = CreateProvider();

        var flush = sut.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(flush.IsCompletedSuccessfully, Is.True,
            "an empty slot set gives the barrier nothing to await, so it must complete synchronously");
        await flush.ConfigureAwait(false);
    }

    [Test]
    public async Task FlushPhaseTwoAsync_awaits_the_outstanding_phase_two_commit()
    {
        // The whole point of the barrier: the trailing batch's
        // phase-2 task is the one nobody else awaits, and it is the
        // one whose manifest row a post-append read needs.
        await using var sut = CreateProvider();

        var trailing = NewPhaseTwo();
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", trailing.Task).ConfigureAwait(false);

        var flush = sut.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(flush.IsCompleted, Is.False,
            "the barrier must block on the still-in-flight trailing phase-2 commit");

        trailing.SetResult();
        await flush.ConfigureAwait(false);

        Assert.That(flush.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task FlushPhaseTwoAsync_awaits_every_shard_slot()
    {
        // A multi-shard producer (the chaos workload's shape) leaves
        // one un-awaited trailing task per shard; the barrier must
        // cover all of them, not just one.
        await using var sut = CreateProvider();

        var shardZero = NewPhaseTwo();
        var shardOne = NewPhaseTwo();
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", shardZero.Task).ConfigureAwait(false);
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|1", shardOne.Task).ConfigureAwait(false);

        var flush = sut.FlushPhaseTwoAsync(CancellationToken.None);

        shardZero.SetResult();
        Assert.That(flush.IsCompleted, Is.False,
            "resolving one shard must not release a barrier that still has another shard outstanding");

        shardOne.SetResult();
        await flush.ConfigureAwait(false);

        Assert.That(flush.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task FlushPhaseTwoAsync_rethrows_a_faulted_phase_two_commit()
    {
        // DisposeAsync deliberately swallows phase-2 faults because
        // it is the terminal stage. The barrier is not terminal, so
        // a caller asking for read-your-writes must be told the
        // write it is waiting on failed.
        await using var sut = CreateProvider();

        var trailing = NewPhaseTwo();
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", trailing.Task).ConfigureAwait(false);

        var phaseTwoFailure = new InvalidOperationException("phase-2 blew up");
        trailing.SetException(phaseTwoFailure);

        var observed = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await sut.FlushPhaseTwoAsync(CancellationToken.None).ConfigureAwait(false));

        Assert.That(observed, Is.SameAs(phaseTwoFailure),
            "the barrier must surface the phase-2 fault verbatim");
    }

    [Test]
    public async Task FlushPhaseTwoAsync_aggregates_faults_across_shards()
    {
        // Faulting one shard must not hide a fault on another, and
        // must not leave the other task unobserved.
        await using var sut = CreateProvider();

        var shardZero = NewPhaseTwo();
        var shardOne = NewPhaseTwo();
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", shardZero.Task).ConfigureAwait(false);
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|1", shardOne.Task).ConfigureAwait(false);

        var zeroFailure = new InvalidOperationException("shard 0 phase-2 blew up");
        var oneFailure = new InvalidOperationException("shard 1 phase-2 blew up");
        shardZero.SetException(zeroFailure);
        shardOne.SetException(oneFailure);

        var observed = Assert.ThrowsAsync<AggregateException>(
            async () => await sut.FlushPhaseTwoAsync(CancellationToken.None).ConfigureAwait(false));

        Assert.That(observed!.InnerExceptions, Is.EquivalentTo(new Exception[] { zeroFailure, oneFailure }),
            "every faulted shard must be represented so none is silently dropped");
    }

    [Test]
    public async Task FlushPhaseTwoAsync_leaves_the_fault_observable_to_the_next_append()
    {
        // The barrier reads the slots without clearing them, so the
        // existing sticky-failure contract is untouched: the next
        // append on the shard still observes the same fault and still
        // triggers WalShardGrain's resync.
        await using var sut = CreateProvider();

        var trailing = NewPhaseTwo();
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", trailing.Task).ConfigureAwait(false);

        var phaseTwoFailure = new InvalidOperationException("phase-2 blew up");
        trailing.SetException(phaseTwoFailure);

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await sut.FlushPhaseTwoAsync(CancellationToken.None).ConfigureAwait(false));

        var successor = NewPhaseTwo();
        var observed = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", successor.Task).ConfigureAwait(false));

        Assert.That(observed, Is.SameAs(phaseTwoFailure),
            "flushing must not consume the fault the next append is contractually required to see");

        successor.SetResult();
    }

    [Test]
    public async Task FlushPhaseTwoAsync_cancellation_releases_the_caller_without_cancelling_the_commit()
    {
        // Cancellation bounds the caller's wait only. The phase-2
        // task is shared state owned by the per-shard worker, so it
        // must keep running and stay observable to the next append
        // and to DisposeAsync.
        await using var sut = CreateProvider();

        var trailing = NewPhaseTwo();
        await sut.AwaitPreviousPipelinedAsync("_m_|tree|0", trailing.Task).ConfigureAwait(false);

        using var cts = new CancellationTokenSource();
        var flush = sut.FlushPhaseTwoAsync(cts.Token);
        await cts.CancelAsync().ConfigureAwait(false);

        Assert.ThrowsAsync<TaskCanceledException>(async () => await flush.ConfigureAwait(false));
        Assert.That(trailing.Task.IsCompleted, Is.False,
            "the underlying phase-2 commit must be untouched by the caller's cancellation");

        trailing.SetResult();
    }

    [Test]
    public async Task FlushPhaseTwoAsync_throws_after_the_provider_is_disposed()
    {
        // Mirrors every other public entry point: a disposed provider
        // can no longer promise anything about visibility.
        var sut = CreateProvider();
        await sut.DisposeAsync().ConfigureAwait(false);

        Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await sut.FlushPhaseTwoAsync(CancellationToken.None).ConfigureAwait(false));
    }
}
