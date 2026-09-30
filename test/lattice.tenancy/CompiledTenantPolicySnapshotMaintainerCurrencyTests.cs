using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the cross-silo currency half of
/// <see cref="CompiledTenantPolicySnapshotMaintainer"/> (issue #4030): the lease,
/// the observed cluster epoch, membership invalidation, and publishing - and
/// re-publishing - an advance from the committing silo. Driven on a
/// <see cref="FakeTimeProvider"/> and awaited background tasks; nothing sleeps or
/// polls.
/// </summary>
[TestFixture]
public sealed class CompiledTenantPolicySnapshotMaintainerCurrencyTests
{
    private static readonly LatticeMutation RegistryMutation = new() { TreeId = TenantTreeNames.RegistryTree };
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(10);

    private static CompiledTenantPolicySnapshotMaintainer Create(
        FakeTimeProvider time,
        ITenantPolicyEpochPublisher? publisher = null,
        ITenantRegistry? registry = null)
    {
        var fake = new HoldableTenantRegistry();
        fake.Records.Add(Record("acme", admins: ["alice"]));
        return new CompiledTenantPolicySnapshotMaintainer(
            registry ?? fake,
            publisher ?? Substitute.For<ITenantPolicyEpochPublisher>(),
            time,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
    }

    private static TenantPolicyEpochLease LeaseAt(long version, Guid? incarnation = null) =>
        new(new TenantPolicyEpoch(incarnation ?? IncarnationA, version), Lease);

    private static readonly Guid IncarnationA = Guid.NewGuid();

    private static async Task<CompiledTenantPolicySnapshotMaintainer> LeasedAsync(
        FakeTimeProvider time,
        ITenantPolicyEpochPublisher? publisher = null,
        HoldableTenantRegistry? registry = null)
    {
        registry?.Records.Add(Record("acme", admins: ["alice"]));
        var maintainer = Create(time, publisher, registry);
        maintainer.ApplyLease(LeaseAt(0), time.GetTimestamp());
        await maintainer.BackgroundRebuild;
        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "precondition");
        return maintainer;
    }

    [Test]
    public async Task Built_but_never_leased_snapshot_is_not_authoritative()
    {
        var maintainer = Create(new FakeTimeProvider());

        await maintainer.RebuildNowAsync();

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False, "without a lease the silo cannot know it has seen every write");
    }

    [Test]
    public async Task ApplyLease_schedules_a_build_and_completes_LeaseEstablished()
    {
        var time = new FakeTimeProvider();
        var maintainer = Create(time);
        Assert.That(maintainer.LeaseEstablished.IsCompleted, Is.False);

        maintainer.ApplyLease(LeaseAt(0), time.GetTimestamp());
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.LeaseEstablished.IsCompletedSuccessfully, Is.True);
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1), "the first lease warms the snapshot");
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.True);
        });
    }

    [Test]
    public async Task Lease_lapses_exactly_one_lease_after_the_request_was_sent()
    {
        var time = new FakeTimeProvider();
        var requestedAt = time.GetTimestamp();
        var maintainer = Create(time);
        time.Advance(TimeSpan.FromSeconds(3));
        maintainer.ApplyLease(LeaseAt(0), requestedAt);
        await maintainer.BackgroundRebuild;

        time.Advance(Lease - TimeSpan.FromSeconds(3) - TimeSpan.FromTicks(1));
        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "measured from the send, not the arrival");
        time.Advance(TimeSpan.FromTicks(1));
        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False);
    }

    [Test]
    public async Task System_clock_lease_is_authoritative_within_its_duration()
    {
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            new FakeTenantRegistry(), Substitute.For<ITenantPolicyEpochPublisher>(), TimeProvider.System,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);

        maintainer.ApplyLease(new TenantPolicyEpochLease(new TenantPolicyEpoch(IncarnationA, 0), TimeSpan.FromHours(1)), TimeProvider.System.GetTimestamp());
        await maintainer.BackgroundRebuild;

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task System_clock_lease_shorter_than_the_coarse_clock_allowance_is_never_authoritative()
    {
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            new FakeTenantRegistry(), Substitute.For<ITenantPolicyEpochPublisher>(), TimeProvider.System,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);

        maintainer.ApplyLease(
            new TenantPolicyEpochLease(
                new TenantPolicyEpoch(IncarnationA, 0),
                TimeSpan.FromMilliseconds(CompiledTenantPolicySnapshotMaintainer.CoarseClockAllowanceMilliseconds)),
            TimeProvider.System.GetTimestamp());
        await maintainer.BackgroundRebuild;

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False, "the coarse clock only ever expires a lease early");
    }

    [Test]
    public async Task System_clock_lease_is_measured_from_when_the_request_was_sent()
    {
        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            new FakeTenantRegistry(), Substitute.For<ITenantPolicyEpochPublisher>(), TimeProvider.System,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        var twoHoursAgo = TimeProvider.System.GetTimestamp() - (2 * 3600 * TimeProvider.System.TimestampFrequency);

        maintainer.ApplyLease(new TenantPolicyEpochLease(new TenantPolicyEpoch(IncarnationA, 0), TimeSpan.FromHours(1)), twoHoursAgo);
        await maintainer.BackgroundRebuild;

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False, "a one-hour lease requested two hours ago has lapsed");
    }

    [Test]
    public async Task ApplyLease_never_shortens_the_deadline()
    {
        var time = new FakeTimeProvider();
        var maintainer = await LeasedAsync(time);
        var early = time.GetTimestamp();
        time.Advance(TimeSpan.FromSeconds(5));
        maintainer.ApplyLease(LeaseAt(0), time.GetTimestamp());

        maintainer.ApplyLease(LeaseAt(0), early);
        time.Advance(TimeSpan.FromSeconds(9));

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "an older lease answer does not pull the deadline back");
    }

    [Test]
    public async Task ObserveEpoch_newer_epoch_revokes_authority_until_the_rebuild_lands()
    {
        var time = new FakeTimeProvider();
        var registry = new HoldableTenantRegistry();
        var maintainer = await LeasedAsync(time, registry: registry);
        registry.HoldScans();

        maintainer.ObserveEpoch(new TenantPolicyEpoch(IncarnationA, 1));

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False);
        registry.ReleaseScans();
        await maintainer.BackgroundRebuild;
        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task ObserveEpoch_equal_or_older_epoch_is_ignored()
    {
        var time = new FakeTimeProvider();
        var maintainer = await LeasedAsync(time);
        maintainer.ObserveEpoch(new TenantPolicyEpoch(IncarnationA, 5));
        await maintainer.BackgroundRebuild;
        var epoch = maintainer.CurrentEpoch;

        maintainer.ObserveEpoch(new TenantPolicyEpoch(IncarnationA, 5));
        maintainer.ObserveEpoch(new TenantPolicyEpoch(IncarnationA, 4));

        Assert.Multiple(() =>
        {
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "a late, out-of-order epoch never moves the silo backwards");
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(epoch), "and schedules no rebuild");
        });
    }

    [Test]
    public async Task Epoch_observed_while_a_scan_runs_leaves_the_result_non_authoritative_until_the_follow_up()
    {
        var time = new FakeTimeProvider();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var scans = 0;
        var registry = Substitute.For<ITenantRegistry>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            Interlocked.Increment(ref scans) == 2 ? Held(entered, gate.Task) : Records());
        var maintainer = Create(time, registry: registry);
        maintainer.ApplyLease(LeaseAt(0), time.GetTimestamp());
        await maintainer.BackgroundRebuild;

        maintainer.ObserveEpoch(new TenantPolicyEpoch(IncarnationA, 1));
        await entered.Task;
        maintainer.ObserveEpoch(new TenantPolicyEpoch(IncarnationA, 2));
        gate.SetResult();
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(scans, Is.EqualTo(3), "the epoch observed mid-scan queued a follow-up scan");
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "authority returns only after the follow-up");
        });
    }

    [Test]
    public async Task InvalidateClusterView_revokes_authority_and_rebuilds()
    {
        var time = new FakeTimeProvider();
        var registry = new HoldableTenantRegistry();
        var maintainer = await LeasedAsync(time, registry: registry);
        var epoch = maintainer.CurrentEpoch;
        registry.HoldScans();

        maintainer.InvalidateClusterView();

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False);
        registry.ReleaseScans();
        await maintainer.BackgroundRebuild;
        Assert.Multiple(() =>
        {
            Assert.That(maintainer.CurrentEpoch, Is.GreaterThan(epoch));
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.True);
        });
    }

    [Test]
    public async Task OnMutationAsync_registry_write_publishes_one_advance_and_completes_after_it()
    {
        var time = new FakeTimeProvider();
        var publish = new TaskCompletionSource();
        var publisher = Substitute.For<ITenantPolicyEpochPublisher>();
        publisher.AdvanceAsync(Arg.Any<CancellationToken>()).Returns(publish.Task);
        var maintainer = await LeasedAsync(time, publisher);

        var write = maintainer.OnMutationAsync(RegistryMutation, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(write.IsCompleted, Is.False, "the write waits for the cluster to be told");
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.False, "not authoritative while its advance is in flight");
        });
        publish.SetResult();
        await write;
        await publisher.Received(1).AdvanceAsync(Arg.Any<CancellationToken>());
        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task OnMutationAsync_non_registry_write_publishes_nothing()
    {
        var time = new FakeTimeProvider();
        var publisher = Substitute.For<ITenantPolicyEpochPublisher>();
        var maintainer = await LeasedAsync(time, publisher);

        await maintainer.OnMutationAsync(new LatticeMutation { TreeId = "some-app-tree" }, CancellationToken.None);

        await publisher.DidNotReceive().AdvanceAsync(Arg.Any<CancellationToken>());
        Assert.That(maintainer.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it()
    {
        var time = new TimerSignalingTimeProvider();
        var attempts = 0;
        var publisher = Substitute.For<ITenantPolicyEpochPublisher>();
        publisher.AdvanceAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            ++attempts <= 2 ? Task.FromException(new TimeoutException("epoch grain unreachable")) : Task.CompletedTask);
        var maintainer = await LeasedAsync(time, publisher);

        await maintainer.OnMutationAsync(RegistryMutation, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        Assert.That(maintainer.IsSnapshotAuthoritative, Is.False, "it owes the cluster an advance");
        var retry = maintainer.AdvanceRetry;
        Assert.That(retry.IsCompleted, Is.False);

        // The first retry fires after 250ms and fails; the loop then arms a 500ms
        // back-off, after which the second retry succeeds. Each step waits for the
        // loop to arm its timer before moving the clock, so nothing races.
        await time.NextTimerAsync();
        time.Advance(TimeSpan.FromMilliseconds(250));
        await time.NextTimerAsync();
        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(2));
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.False, "the first retry failed too");
        });
        time.Advance(TimeSpan.FromMilliseconds(500));
        await retry;

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(3));
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "the re-published advance restores authority");
        });
    }

    [Test]
    public async Task Dispose_stops_the_background_retry()
    {
        var time = new FakeTimeProvider();
        var publisher = Substitute.For<ITenantPolicyEpochPublisher>();
        publisher.AdvanceAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        var maintainer = await LeasedAsync(time, publisher);
        await maintainer.OnMutationAsync(RegistryMutation, CancellationToken.None);
        var retry = maintainer.AdvanceRetry;

        maintainer.Dispose();
        maintainer.Dispose();
        await retry;

        Assert.That(retry.IsCompletedSuccessfully, Is.True);
    }

    private static async IAsyncEnumerable<TenantRecord> Records()
    {
        await Task.CompletedTask;
        yield return Record("acme", admins: ["alice"]);
    }

    private static async IAsyncEnumerable<TenantRecord> Held(TaskCompletionSource entered, Task gate)
    {
        entered.TrySetResult();
        await gate;
        yield return Record("acme", admins: ["alice"]);
    }
}
