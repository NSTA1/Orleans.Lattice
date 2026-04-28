using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="LatticeReplicationAdmin"/>. The admin
/// seam gates operator-driven snapshot re-seed requests behind a
/// per-<c>(tree, sourceClusterId)</c> rate limit and forwards
/// honoured requests to
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/>.
/// </summary>
[TestFixture]
public class LatticeReplicationAdminTests
{
    private const string Tree = "orders";
    private const string Source = "site-a";

    /// <summary>Manually-advanced <see cref="TimeProvider"/> for deterministic rate-limit tests.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static (
        LatticeReplicationAdmin Admin,
        ILatticeBootstrapCoordinator Coordinator,
        LatticeReplicationOptions Options,
        ManualTimeProvider Time) Create(TimeSpan? minInterval = null)
    {
        var coordinator = Substitute.For<ILatticeBootstrapCoordinator>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "self",
            OperatorReseedMinInterval = minInterval ?? TimeSpan.FromMinutes(1),
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var time = new ManualTimeProvider();
        var admin = new LatticeReplicationAdmin(
            coordinator, monitor,
            NullLogger<LatticeReplicationAdmin>.Instance,
            time);
        return (admin, coordinator, options, time);
    }

    [Test]
    public void Constructor_throws_when_coordinator_is_null()
    {
        Assert.That(
            () => new LatticeReplicationAdmin(
                null!,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                NullLogger<LatticeReplicationAdmin>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        Assert.That(
            () => new LatticeReplicationAdmin(
                Substitute.For<ILatticeBootstrapCoordinator>(),
                null!,
                NullLogger<LatticeReplicationAdmin>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_logger_is_null()
    {
        Assert.That(
            () => new LatticeReplicationAdmin(
                Substitute.For<ILatticeBootstrapCoordinator>(),
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_accepts_null_time_provider_and_falls_back_to_system()
    {
        Assert.That(
            () => new LatticeReplicationAdmin(
                Substitute.For<ILatticeBootstrapCoordinator>(),
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                NullLogger<LatticeReplicationAdmin>.Instance,
                timeProvider: null),
            Throws.Nothing);
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_tree_name_is_null()
    {
        var (admin, _, _, _) = Create();
        Assert.That(
            async () => await admin.RequestSnapshotAsync(null!, Source),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_tree_name_is_empty()
    {
        var (admin, _, _, _) = Create();
        Assert.That(
            async () => await admin.RequestSnapshotAsync(string.Empty, Source),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_source_cluster_id_is_null()
    {
        var (admin, _, _, _) = Create();
        Assert.That(
            async () => await admin.RequestSnapshotAsync(Tree, null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_source_cluster_id_is_empty()
    {
        var (admin, _, _, _) = Create();
        Assert.That(
            async () => await admin.RequestSnapshotAsync(Tree, string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_observes_cancellation_before_dispatch()
    {
        var (admin, coordinator, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await admin.RequestSnapshotAsync(Tree, Source, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        coordinator.DidNotReceive().BootstrapAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestSnapshotAsync_first_call_is_honoured_and_delegates_to_coordinator()
    {
        var (admin, coordinator, _, time) = Create();

        var decision = await admin.RequestSnapshotAsync(Tree, Source);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Triggered, Is.True);
            Assert.That(decision.LastRequestedAt, Is.EqualTo(time.Now));
            Assert.That(decision.RetryAfter, Is.Null);
        });
        await coordinator.Received(1).BootstrapAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestSnapshotAsync_second_call_within_interval_is_denied_with_retry_after()
    {
        var (admin, coordinator, _, time) = Create(TimeSpan.FromMinutes(1));

        var firstAt = time.Now;
        await admin.RequestSnapshotAsync(Tree, Source);

        // Advance 20s — still within the 60s rate-limit window.
        time.Now = firstAt + TimeSpan.FromSeconds(20);

        var second = await admin.RequestSnapshotAsync(Tree, Source);

        Assert.Multiple(() =>
        {
            Assert.That(second.Triggered, Is.False);
            Assert.That(second.LastRequestedAt, Is.EqualTo(firstAt));
            Assert.That(second.RetryAfter, Is.EqualTo(TimeSpan.FromSeconds(40)));
        });

        // Coordinator was called only on the first request.
        await coordinator.Received(1).BootstrapAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestSnapshotAsync_call_after_interval_elapses_is_honoured()
    {
        var (admin, coordinator, _, time) = Create(TimeSpan.FromMinutes(1));

        await admin.RequestSnapshotAsync(Tree, Source);

        // Advance past the interval.
        var secondAt = time.Now + TimeSpan.FromMinutes(2);
        time.Now = secondAt;

        var second = await admin.RequestSnapshotAsync(Tree, Source);

        Assert.Multiple(() =>
        {
            Assert.That(second.Triggered, Is.True);
            // Pin the contract that a re-honoured call advances the
            // dictionary timestamp to the new now (not the original
            // honoured timestamp). A regression that forgot to update
            // the dictionary on re-honour would slip past every other
            // test in this fixture.
            Assert.That(second.LastRequestedAt, Is.EqualTo(secondAt));
            Assert.That(second.RetryAfter, Is.Null);
        });
        await coordinator.Received(2).BootstrapAsync(Tree, Source, Arg.Any<CancellationToken>());

        // And a third call within the *new* window must be denied
        // against the *new* honoured timestamp, proving the dictionary
        // entry was overwritten and not stale.
        time.Now = secondAt + TimeSpan.FromSeconds(20);
        var third = await admin.RequestSnapshotAsync(Tree, Source);
        Assert.Multiple(() =>
        {
            Assert.That(third.Triggered, Is.False);
            Assert.That(third.LastRequestedAt, Is.EqualTo(secondAt));
            Assert.That(third.RetryAfter, Is.EqualTo(TimeSpan.FromSeconds(40)));
        });
        await coordinator.Received(2).BootstrapAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestSnapshotAsync_zero_interval_disables_rate_limit()
    {
        var (admin, coordinator, _, _) = Create(TimeSpan.Zero);

        await admin.RequestSnapshotAsync(Tree, Source);
        var second = await admin.RequestSnapshotAsync(Tree, Source);
        var third = await admin.RequestSnapshotAsync(Tree, Source);

        Assert.Multiple(() =>
        {
            Assert.That(second.Triggered, Is.True);
            Assert.That(third.Triggered, Is.True);
        });
        await coordinator.Received(3).BootstrapAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestSnapshotAsync_rate_limit_is_per_tree()
    {
        var (admin, coordinator, _, _) = Create(TimeSpan.FromMinutes(1));

        await admin.RequestSnapshotAsync("tree-a", Source);
        var other = await admin.RequestSnapshotAsync("tree-b", Source);

        Assert.That(other.Triggered, Is.True);
        await coordinator.Received(1).BootstrapAsync("tree-a", Source, Arg.Any<CancellationToken>());
        await coordinator.Received(1).BootstrapAsync("tree-b", Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestSnapshotAsync_rate_limit_is_per_source_cluster()
    {
        var (admin, coordinator, _, _) = Create(TimeSpan.FromMinutes(1));

        await admin.RequestSnapshotAsync(Tree, "site-a");
        var other = await admin.RequestSnapshotAsync(Tree, "site-b");

        Assert.That(other.Triggered, Is.True);
        await coordinator.Received(1).BootstrapAsync(Tree, "site-a", Arg.Any<CancellationToken>());
        await coordinator.Received(1).BootstrapAsync(Tree, "site-b", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestSnapshotAsync_coordinator_exception_does_not_consume_rate_limit_budget()
    {
        var (admin, coordinator, _, time) = Create(TimeSpan.FromMinutes(1));
        coordinator.BootstrapAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("conflict")));

        Assert.That(
            async () => await admin.RequestSnapshotAsync(Tree, Source),
            Throws.InstanceOf<InvalidOperationException>());

        // Recover and ensure the next call is still honoured immediately
        // (rate-limit budget was not consumed by the failed attempt).
        coordinator.BootstrapAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        time.Now += TimeSpan.FromSeconds(1);
        var second = await admin.RequestSnapshotAsync(Tree, Source);
        Assert.That(second.Triggered, Is.True);
    }

    [Test]
    public async Task RequestSnapshotAsync_propagates_cancellation_token_to_coordinator()
    {
        var (admin, coordinator, _, _) = Create();

        using var cts = new CancellationTokenSource();
        await admin.RequestSnapshotAsync(Tree, Source, cts.Token);

        await coordinator.Received(1).BootstrapAsync(Tree, Source, cts.Token);
    }

    [Test]
    public async Task RequestSnapshotAsync_picks_up_options_change_via_options_monitor()
    {
        // Per-tree options resolution: ensure the monitor.Get(treeName)
        // path is honoured each call.
        var coordinator = Substitute.For<ILatticeBootstrapCoordinator>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();

        // Initially zero interval (no rate limit) — request should be honoured.
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = "self",
            OperatorReseedMinInterval = TimeSpan.Zero,
        });

        var time = new ManualTimeProvider();
        var admin = new LatticeReplicationAdmin(
            coordinator, monitor,
            NullLogger<LatticeReplicationAdmin>.Instance,
            time);

        await admin.RequestSnapshotAsync(Tree, Source);

        // Reconfigure: tighten the rate limit. Second call should now be denied.
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = "self",
            OperatorReseedMinInterval = TimeSpan.FromMinutes(5),
        });

        var second = await admin.RequestSnapshotAsync(Tree, Source);
        Assert.That(second.Triggered, Is.False);
    }
}
