using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The bootstrap a receiver's enable requests when the tree is already
/// enrolled locally, because its enrolment converged from a peer through the
/// config tree before the receiver's own enable ran (issue #5594).
/// </summary>
public sealed partial class LatticeReplicationConfigAuthorityTests
{
    private const string Source = "site-b";

    [Test]
    public async Task EnableReplicationAsync_on_an_already_enrolled_receiver_with_a_source_requests_bootstrap()
    {
        // The source enables first; its enrolment converges to this receiver
        // through the shared config OR-Map before the receiver's own enable.
        var store = new InMemoryConfigStore();
        await CreateAuthority(store, localReplicaId: Source)
            .EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);

        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var receiver = CreateAuthority(store, admin: admin);

        var result = await receiver.EnableReplicationAsync(
            Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: Source);

        // The enable is still idempotent for the config, but the named source's
        // pre-enrolment rows reach this receiver only through the snapshot.
        Assert.That(result.AlreadyEnabled, Is.True);
        Assert.That(result.BootstrapRequested, Is.True);
        await admin.Received(1).RequestSnapshotAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnableReplicationAsync_on_an_already_enrolled_receiver_without_a_source_requests_nothing()
    {
        var store = await EnrolledFromSourceAsync();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var coordinator = Substitute.For<ILatticeBootstrapCoordinator>();
        var receiver = CreateAuthority(store, admin: admin, coordinator: coordinator);

        var result = await receiver.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);

        Assert.That(result.AlreadyEnabled, Is.True);
        Assert.That(result.BootstrapRequested, Is.False);
        await admin.DidNotReceiveWithAnyArgs().RequestSnapshotAsync(default!, default!, default);
        await coordinator.DidNotReceiveWithAnyArgs().GetStatusAsync(default!, default);
    }

    [Test]
    public async Task EnableReplicationAsync_on_an_already_enrolled_receiver_with_a_bootstrap_in_flight_from_the_source_starts_no_other()
    {
        var store = await EnrolledFromSourceAsync();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var coordinator = CoordinatorReporting(new BootstrapCoordinatorStatus(LatticeBootstrapState.ApplyingSnapshot, Source));
        var receiver = CreateAuthority(store, admin: admin, coordinator: coordinator);

        var result = await receiver.EnableReplicationAsync(
            Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: Source);

        Assert.That(result.AlreadyEnabled, Is.True);
        Assert.That(result.BootstrapRequested, Is.True, "the bootstrap asked for is under way");
        await admin.DidNotReceiveWithAnyArgs().RequestSnapshotAsync(default!, default!, default);
    }

    [Test]
    public async Task EnableReplicationAsync_on_an_already_enrolled_receiver_bootstrapped_from_the_source_before_requests_again()
    {
        // A completed bootstrap may have exported before the source wrote rows this
        // receiver then dropped while it was not yet enrolled, so it proves nothing
        // about the rows the enable asks for.
        var store = await EnrolledFromSourceAsync();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var coordinator = CoordinatorReporting(new BootstrapCoordinatorStatus(LatticeBootstrapState.LiveIncremental, null)
        {
            CompletedSourceClusterId = Source,
        });
        var receiver = CreateAuthority(store, admin: admin, coordinator: coordinator);

        var result = await receiver.EnableReplicationAsync(
            Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: Source);

        Assert.That(result.AlreadyEnabled, Is.True);
        Assert.That(result.BootstrapRequested, Is.True);
        await admin.Received(1).RequestSnapshotAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnableReplicationAsync_on_an_already_enrolled_receiver_with_a_bootstrap_in_flight_from_another_source_requests_bootstrap()
    {
        // The admin seam surfaces the coordinator's different-source refusal; the
        // authority does not hide it behind a no-op.
        var store = await EnrolledFromSourceAsync();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var coordinator = CoordinatorReporting(new BootstrapCoordinatorStatus(LatticeBootstrapState.ApplyingSnapshot, "site-c"));
        var receiver = CreateAuthority(store, admin: admin, coordinator: coordinator);

        await receiver.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: Source);

        await admin.Received(1).RequestSnapshotAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnableReplicationAsync_repeated_while_the_bootstrap_is_in_flight_starts_one_bootstrap()
    {
        // A second enable naming the same source while the first's bootstrap is still
        // in flight must not start a second export.
        // The admin seam's rate limit is off, so only the authority's in-flight
        // check stands between the two enables and a second kickoff. A truly
        // concurrent pair can both pass that check; the coordinator grain then
        // absorbs the same-source kickoff while the first is in progress
        // (LatticeBootstrapCoordinatorGrainTests.TryInitiateBootstrap_is_idempotent_with_same_source_cluster).
        var store = await EnrolledFromSourceAsync();
        var coordinator = new RecordingCoordinator();
        var options = new LatticeReplicationOptions { ClusterId = "x", OperatorReseedMinInterval = TimeSpan.Zero };
        var monitor = Substitute.For<Microsoft.Extensions.Options.IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var admin = new LatticeReplicationAdmin(
            coordinator,
            monitor,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<LatticeReplicationAdmin>.Instance);
        var receiver = CreateAuthority(store, admin: admin, coordinator: coordinator);

        var first = await receiver.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: Source);
        var second = await receiver.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: Source);

        Assert.That(first.BootstrapRequested, Is.True);
        Assert.That(second.BootstrapRequested, Is.True);
        Assert.That(coordinator.Kickoffs, Is.EqualTo(1));
    }

    /// <summary>A config store in which <see cref="Source"/> has enabled <see cref="Tree"/>, converged to this receiver.</summary>
    private static async Task<InMemoryConfigStore> EnrolledFromSourceAsync()
    {
        var store = new InMemoryConfigStore();
        await CreateAuthority(store, localReplicaId: Source)
            .EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);
        return store;
    }

    private static ILatticeBootstrapCoordinator CoordinatorReporting(BootstrapCoordinatorStatus status)
    {
        var coordinator = Substitute.For<ILatticeBootstrapCoordinator>();
        coordinator.GetStatusAsync(Tree, Arg.Any<CancellationToken>()).Returns(status);
        return coordinator;
    }

    /// <summary>A coordinator that reports a kickoff as in flight from its source until it is told otherwise.</summary>
    private sealed class RecordingCoordinator : ILatticeBootstrapCoordinator
    {
        private string? _inFlight;

        public int Kickoffs { get; private set; }

        public Task<LatticeBootstrapState> GetStateAsync(string treeName, CancellationToken cancellationToken = default)
            => Task.FromResult(_inFlight is null ? LatticeBootstrapState.Idle : LatticeBootstrapState.RequestingSnapshot);

        public Task<BootstrapCoordinatorStatus> GetStatusAsync(string treeName, CancellationToken cancellationToken = default)
            => Task.FromResult(new BootstrapCoordinatorStatus(
                _inFlight is null ? LatticeBootstrapState.Idle : LatticeBootstrapState.RequestingSnapshot,
                _inFlight));

        public Task BootstrapAsync(string treeName, string sourceClusterId, CancellationToken cancellationToken = default)
        {
            Kickoffs++;
            _inFlight = sourceClusterId;
            return Task.CompletedTask;
        }
    }
}
