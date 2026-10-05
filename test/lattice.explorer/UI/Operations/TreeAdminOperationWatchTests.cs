using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Operations;
using static Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>
/// A page's hold on a tree-administration operation (#4124): it starts one under a
/// target-naming id and follows it, finds the one still running for the page's targets
/// and nothing else, asks it to cancel, says once when it finishes, and lets go.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TreeAdminOperationWatchTests
{
    private readonly ManualTimeProvider _time = new();
    private TreeAdminOperationScript _script = null!;

    [SetUp]
    public void Script() => _script = new TreeAdminOperationScript(Substitute.For<ILatticeTreeAdmin, ILatticeTreeAdminOperations>());

    [Test]
    public async Task A_start_follows_the_operation_until_it_finishes_and_says_so_once()
    {
        _script.Script(
            TreeAdminOperationKinds.ViewRebuild,
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Running, TreeAdminOperationPhases.Projecting, 5, 10, TreeAdminOperationUnits.Keys),
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Succeeded, "Completed"));
        using var watch = new TreeAdminOperationWatch(_time);
        var finished = new List<LatticeOperationStatus>();
        watch.Finished += finished.Add;
        string? chosen = null;

        await watch.StartAsync(
            _script.Operations,
            TreeAdminOperationKinds.ViewRebuild,
            "by-status",
            (operationId, ct) =>
            {
                chosen = operationId;
                return _script.Operations.StartViewRebuildAsync("by-status", operationId, ct);
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(TreeAdminOperationIds.Matches(chosen!, TreeAdminOperationKinds.ViewRebuild, "by-status"), Is.True);
            Assert.That(watch.OperationId, Is.EqualTo(chosen));
            Assert.That(watch.IsRunning, Is.True);
            Assert.That(watch.IsFor(TreeAdminOperationKinds.ViewRebuild, "by-status"), Is.True);
            Assert.That(watch.Status!.CompletedUnits, Is.EqualTo(5));
            Assert.That(finished, Is.Empty);
        });

        Advance();
        Assert.That(SpinWait.SpinUntil(() => finished.Count == 1, TimeSpan.FromSeconds(10)), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(watch.IsRunning, Is.False);
            Assert.That(finished.Single().State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(_time.ArmedTimers, Is.Zero, "A finished operation is no longer read.");
        });
    }

    [Test]
    public async Task Resume_follows_the_newest_running_operation_for_a_target_and_ignores_the_rest()
    {
        _script.Running(
            TreeAdminOperationIds.New(TreeAdminOperationKinds.ViewRebuild, "by-status"),
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Succeeded, "Completed"));
        _script.Running(
            TreeAdminOperationIds.New(TreeAdminOperationKinds.ViewRebuild, "elsewhere"),
            Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Running, TreeAdminOperationPhases.Scanning));
        var wanted = TreeAdminOperationIds.New(TreeAdminOperationKinds.ViewReconcile, "by-status");
        _script.Running(wanted, Status(TreeAdminOperationKinds.ViewReconcile, LatticeOperationState.Running, TreeAdminOperationPhases.Digesting));
        _script.Running(
            "0123456789abcdef0123456789abcdef",
            Status(TreeAdminOperationKinds.ViewReconcile, LatticeOperationState.Running, TreeAdminOperationPhases.Digesting));
        using var watch = new TreeAdminOperationWatch(_time);

        var found = await watch.ResumeAsync(
            _script.Operations,
            [(TreeAdminOperationKinds.ViewRebuild, "by-status"), (TreeAdminOperationKinds.ViewReconcile, "by-status")],
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(watch.OperationId, Is.EqualTo(wanted));
            Assert.That(watch.Kind, Is.EqualTo(TreeAdminOperationKinds.ViewReconcile));
            Assert.That(watch.Target, Is.EqualTo("by-status"));
            Assert.That(watch.Status!.Phase, Is.EqualTo(TreeAdminOperationPhases.Digesting));
        });
    }

    [Test]
    public async Task Resume_finds_nothing_when_the_listing_fails_or_is_empty()
    {
        using var watch = new TreeAdminOperationWatch(_time);
        var candidates = new[] { (TreeAdminOperationKinds.WalMove, "orders\n0") };

        var empty = await watch.ResumeAsync(_script.Operations, candidates, CancellationToken.None);
        _script.Operations.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        var failed = await watch.ResumeAsync(_script.Operations, candidates, CancellationToken.None);
        var none = await watch.ResumeAsync(_script.Operations, [], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(empty, Is.False);
            Assert.That(failed, Is.False);
            Assert.That(none, Is.False);
            Assert.That(watch.OperationId, Is.Null);
            Assert.That(watch.Status, Is.Null);
        });
    }

    [Test]
    public async Task Cancel_asks_the_cluster_and_reads_the_status_at_once()
    {
        _script.Script(
            TreeAdminOperationKinds.WalMove,
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying, 4, 20, TreeAdminOperationUnits.Entries),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying, 8, 20, TreeAdminOperationUnits.Entries));
        using var watch = new TreeAdminOperationWatch(_time);
        await watch.CancelAsync(CancellationToken.None);
        Assert.That(_script.Cancelled, Is.Empty, "Nothing is followed, so nothing is cancelled.");

        await watch.StartAsync(
            _script.Operations,
            TreeAdminOperationKinds.WalMove,
            "orders\n1",
            (operationId, ct) => _script.Operations.StartWalMoveAsync("orders", 1, "cold", null, operationId, ct),
            CancellationToken.None);
        await watch.CancelAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_script.Cancelled, Is.EqualTo(new[] { watch.OperationId }));
            Assert.That(watch.Status!.CancelRequested, Is.True);
            Assert.That(watch.Status.CompletedUnits, Is.EqualTo(8));
        });
    }

    [Test]
    public async Task Clear_lets_go_without_touching_the_operation()
    {
        using var watch = new TreeAdminOperationWatch(_time);
        await watch.StartAsync(
            _script.Operations,
            TreeAdminOperationKinds.TagIndexReconcile,
            "by-region",
            (operationId, ct) => _script.Operations.StartTagIndexReconcileAsync("by-region", operationId, ct),
            CancellationToken.None);

        watch.Clear();

        Assert.Multiple(() =>
        {
            Assert.That(watch.OperationId, Is.Null);
            Assert.That(watch.Kind, Is.Null);
            Assert.That(watch.Target, Is.Null);
            Assert.That(watch.Status, Is.Null);
            Assert.That(watch.LastError, Is.Null);
            Assert.That(watch.IsRunning, Is.False);
            Assert.That(_script.Cancelled, Is.Empty);
            Assert.That(_time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public async Task Resume_follows_nothing_when_its_search_was_cancelled_before_the_listing_answered()
    {
        _script.Running(
            TreeAdminOperationIds.New(TreeAdminOperationKinds.WalMove, "orders\n0"),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying));
        using var watch = new TreeAdminOperationWatch(_time);

        // The listing answers despite the cancellation, as a reply already on the wire does:
        // the page that asked has moved on, so another target's operation must not be followed (#4512).
        Assert.That(
            async () => await watch.ResumeAsync(_script.Operations, [(TreeAdminOperationKinds.WalMove, "orders\n0")], new CancellationToken(canceled: true)),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(watch.OperationId, Is.Null);
            Assert.That(watch.Status, Is.Null);
            Assert.That(_time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public void The_arguments_are_checked()
    {
        using var watch = new TreeAdminOperationWatch(_time);

        Assert.Multiple(() =>
        {
            Assert.That(() => new TreeAdminOperationWatch(null!), Throws.ArgumentNullException);
            Assert.That(() => watch.ResumeAsync(null!, [], CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => watch.ResumeAsync(_script.Operations, null!, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => watch.StartAsync(null!, TreeAdminOperationKinds.WalMove, "t", (_, _) => null!, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => watch.StartAsync(_script.Operations, TreeAdminOperationKinds.WalMove, "t", null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    private void Advance()
    {
        Assert.That(SpinWait.SpinUntil(() => _time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "the follower re-arms");
        _time.Advance(ClusterStatusPoller.Interval);
    }
}
