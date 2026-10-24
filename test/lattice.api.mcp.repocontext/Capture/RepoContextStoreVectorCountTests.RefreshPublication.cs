using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

public sealed partial class RepoContextStoreVectorCountTests
{
    [Test]
    public async Task Competing_count_reads_schedule_only_the_published_refresh()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var scheduler = new CountRefreshScheduler();
        var writer = CreateWriter(harness, harness.GrainFactory, scheduler.Schedule);
        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        // Re-enter while the first scheduler has accepted its work but not yet
        // returned the task: GetOrAdd's eager value factory has not published it.
        scheduler.OnFirstSchedule = () => _ = writer.CountEmbeddedAsync(RepoId, Ct);
        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        var queued = scheduler.Count;
        await scheduler.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(queued, Is.EqualTo(1),
                "Only the published reservation may schedule work, even when callers overlap before scheduling returns.");
            Assert.That(writer.ReadEmbeddedCount(RepoId).Count, Is.EqualTo(1),
                "The controlled scheduler still executes the real membership walk.");
            Assert.That(writer.PendingEmbeddedCountRefresh(RepoId), Is.Null);
        });
    }

    [Test]
    public async Task Completing_a_contender_cannot_remove_another_refresh_guard()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var scheduler = new CountRefreshScheduler();
        var writer = CreateWriter(harness, harness.GrainFactory, scheduler.Schedule);
        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        scheduler.OnFirstSchedule = () => _ = writer.CountEmbeddedAsync(RepoId, Ct);
        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        var published = writer.PendingEmbeddedCountRefresh(RepoId);
        Assert.That(published, Is.Not.Null);

        await scheduler.CompleteAsync(0);
        var afterContender = writer.PendingEmbeddedCountRefresh(RepoId);
        var stillOwned = published!.IsCompleted || ReferenceEquals(afterContender, published);
        await scheduler.DrainAsync();

        Assert.That(stillOwned, Is.True,
            "Completing a discarded contender must not clear the winning refresh's still-running guard.");
    }

    [Test]
    public async Task A_refresh_completing_before_scheduling_returns_does_not_leave_a_completed_guard()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var scheduled = 0;
        var writer = CreateWriter(harness, harness.GrainFactory, work =>
        {
            scheduled++;
            // Model the scheduling caller losing its turn until the off-thread
            // refresh finishes, before Task.Run's result reaches the caller.
            Task.Run(work).GetAwaiter().GetResult();
            return Task.CompletedTask;
        });
        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);
        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        var afterFirst = writer.PendingEmbeddedCountRefresh(RepoId);

        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/B.cs") }, Ct);
        var stale = await writer.CountEmbeddedAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.Null,
                "A scan that already finished must not subsequently be inserted as a permanent guard.");
            Assert.That(stale.Count, Is.EqualTo(1));
            Assert.That(stale.Pending, Is.True,
                "The read still serves the pre-scheduling memo, not an inline-scanned replacement.");
            Assert.That(scheduled, Is.EqualTo(2),
                "A later generation miss must schedule a new scan after the fast first one.");
            Assert.That(writer.ReadEmbeddedCount(RepoId).Count, Is.EqualTo(2));
            Assert.That(writer.PendingEmbeddedCountRefresh(RepoId), Is.Null);
        });
    }

    [Test]
    public async Task A_scheduling_failure_releases_the_reservation_and_is_not_swallowed()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var scheduled = 0;
        var writer = CreateWriter(harness, harness.GrainFactory, work =>
        {
            if (++scheduled == 1)
            {
                throw new InvalidOperationException("Controlled scheduling failure.");
            }

            return Task.Run(work);
        });
        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        Assert.Throws<InvalidOperationException>(() => writer.CountEmbeddedAsync(RepoId, Ct));
        Assert.That(writer.PendingEmbeddedCountRefresh(RepoId), Is.Null);
        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        if (writer.PendingEmbeddedCountRefresh(RepoId) is { } refresh)
        {
            await refresh.WaitAsync(TimeSpan.FromSeconds(30), Ct);
        }

        Assert.Multiple(() =>
        {
            Assert.That(scheduled, Is.EqualTo(2));
            Assert.That(writer.ReadEmbeddedCount(RepoId).Count, Is.EqualTo(1));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task An_unsuccessful_scheduled_task_releases_the_reservation_and_preserves_its_failure(
        bool cancelled)
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        var scheduled = 0;
        var completion = new TaskCompletionSource();
        var writer = CreateWriter(harness, harness.GrainFactory,
            work => ++scheduled == 1 ? completion.Task : Task.Run(work));
        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);
        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        var first = writer.PendingEmbeddedCountRefresh(RepoId);
        Assert.That(first, Is.Not.Null);

        if (cancelled)
        {
            completion.SetCanceled();
            Assert.ThrowsAsync<TaskCanceledException>(async () => await first!);
        }
        else
        {
            completion.SetException(new InvalidOperationException("Controlled scheduled-task failure."));
            Assert.ThrowsAsync<InvalidOperationException>(async () => await first!);
        }

        Assert.That(writer.PendingEmbeddedCountRefresh(RepoId), Is.Null);
        Assert.That(writer.ReadEmbeddedCount(RepoId).Count, Is.Null);
        _ = await writer.CountEmbeddedAsync(RepoId, Ct);
        if (writer.PendingEmbeddedCountRefresh(RepoId) is { } refresh)
        {
            await refresh.WaitAsync(TimeSpan.FromSeconds(30), Ct);
        }

        Assert.Multiple(() =>
        {
            Assert.That(scheduled, Is.EqualTo(2));
            Assert.That(writer.ReadEmbeddedCount(RepoId).Count, Is.EqualTo(1));
        });
    }

    private sealed class CountRefreshScheduler
    {
        private readonly List<(Func<Task> Work, TaskCompletionSource Completion)> _scheduled = new();
        public Action? OnFirstSchedule { get; set; }
        public int Count => _scheduled.Count;

        public Task Schedule(Func<Task> work)
        {
            // Run ExecuteSynchronously cleanup before CompleteAsync returns.
            var completion = new TaskCompletionSource();
            _scheduled.Add((work, completion));
            if (_scheduled.Count == 1)
            {
                OnFirstSchedule?.Invoke();
            }

            return completion.Task;
        }

        public async Task CompleteAsync(int index)
        {
            var (work, completion) = _scheduled[index];
            if (completion.Task.IsCompleted)
            {
                return;
            }

            await work();
            completion.SetResult();
        }

        public async Task DrainAsync()
        {
            for (var index = 0; index < _scheduled.Count; index++)
            {
                await CompleteAsync(index);
            }
        }
    }
}
