using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4534: the receiver's answer to a sender that took it off the log.
/// The coordinator grain is the only stand-in.
/// </summary>
[TestFixture]
public class ReplicationReseedResponderTests
{
    private const string Tree = "reseed-tree";
    private const string Source = "site-a";

    private static (IGrainFactory Factory, ILatticeBootstrapCoordinatorGrain Coordinator) Create(
        long? completed, string? runningSource = null)
    {
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        coordinator.GetCompletedExportEpochAsync(Source).Returns(Task.FromResult(completed));
        coordinator.GetStatusAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new BootstrapCoordinatorStatus(LatticeBootstrapState.Idle, runningSource)));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree).Returns(coordinator);
        return (factory, coordinator);
    }

    [Test]
    public async Task Echoes_a_completed_reseed_past_the_requested_epoch_without_bootstrapping_again()
    {
        var (factory, coordinator) = Create(completed: 5);

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 3, true, NullLogger.Instance);

        Assert.That(echoed, Is.EqualTo(5));
        await coordinator.DidNotReceiveWithAnyArgs().BootstrapAsync(default!, default);
        await coordinator.DidNotReceiveWithAnyArgs().BootstrapForReseedAsync(default!, default, default, default);
    }

    [Test]
    public async Task Starts_a_bootstrap_when_none_has_completed_past_the_requested_epoch()
    {
        var (factory, coordinator) = Create(completed: 3);

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 3, true, NullLogger.Instance);

        Assert.That(echoed, Is.EqualTo(3));
        await coordinator.Received(1).BootstrapForReseedAsync(Source, 3, true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Records_the_request_on_a_running_bootstrap_without_starting_another()
    {
        // The coordinator's same-source kickoff is idempotent; recording the
        // request lets the running drain clear the stale buckets (#4533).
        var (factory, coordinator) = Create(completed: null, runningSource: Source);

        await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 0, true, NullLogger.Instance);

        await coordinator.DidNotReceiveWithAnyArgs().BootstrapAsync(default!, default);
        await coordinator.Received(1).BootstrapForReseedAsync(Source, 0, true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Records_the_request_but_leaves_the_bootstrap_to_the_operator_when_auto_bootstrap_is_disabled()
    {
        var (factory, coordinator) = Create(completed: null);

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 0, false, NullLogger.Instance);

        Assert.That(echoed, Is.Null);
        await coordinator.DidNotReceiveWithAnyArgs().BootstrapAsync(default!, default);
        await coordinator.Received(1).BootstrapForReseedAsync(Source, 0, false, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_failed_lookup_echoes_nothing_so_the_sender_keeps_withholding()
    {
        var (factory, coordinator) = Create(completed: 9);
        coordinator.GetCompletedExportEpochAsync(Source).ThrowsAsync(new TimeoutException("simulated"));

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 0, true, NullLogger.Instance);

        Assert.That(echoed, Is.Null);
    }

    [Test]
    public async Task With_a_reported_lineage_echoes_only_a_completion_installed_under_it()
    {
        var (factory, coordinator) = Create(completed: 5);
        var lineage = Guid.NewGuid();
        coordinator.GetCompletedExportEpochAsync(Source, lineage).Returns(Task.FromResult<long?>(null));

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 3, true, NullLogger.Instance, lineage);

        Assert.That(echoed, Is.Null, "a completion under another lineage does not vouch for the peer's current contents");
        await coordinator.DidNotReceive().GetCompletedExportEpochAsync(Source);
        await coordinator.Received(1).BootstrapForReseedAsync(Source, 3, true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_degraded_receiver_reporting_the_empty_lineage_echoes_its_unbound_completion()
    {
        var (factory, coordinator) = Create(completed: 5);

        var echoed = await ReplicationReseedResponder.RespondAsync(factory, Tree, Source, 3, true, NullLogger.Instance, Guid.Empty);

        Assert.That(echoed, Is.EqualTo(5), "the sender settles on an empty-lineage echo, so a degraded receiver must still answer");
        await coordinator.DidNotReceiveWithAnyArgs().GetCompletedExportEpochAsync(default!, default(Guid));
    }
}
