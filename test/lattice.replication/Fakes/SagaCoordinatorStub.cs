using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// Configures a substitute <see cref="ICrossClusterSagaCoordinatorGrain"/> to
/// behave like the real coordinator on the start / drive / status contract the
/// restore saga dispatcher follows (issue #5506):
/// <see cref="ICrossClusterSagaCoordinatorGrain.BeginAsync"/> records the saga and
/// reports it in flight, <see cref="ICrossClusterSagaCoordinatorGrain.DriveAsync"/>
/// runs the supplied drive and records its outcome, and
/// <see cref="ICrossClusterSagaCoordinatorGrain.GetStatusAsync"/> reports the saga
/// complete with that outcome once a drive has finished.
/// </summary>
internal static class SagaCoordinatorStub
{
    /// <summary>The status of a saga that is preparing and that nothing is driving.</summary>
    public static CrossClusterSagaStatus Preparing { get; } =
        new(CrossClusterSagaPhase.Preparing, null, IsDriving: false, RequiresRepair: false, Detail: null);

    /// <summary>The status of a saga completed with <paramref name="outcome"/>.</summary>
    public static CrossClusterSagaStatus Completed(CrossClusterSagaOutcome outcome) =>
        new(CrossClusterSagaPhase.Completed, outcome, IsDriving: false, RequiresRepair: false, Detail: null);

    /// <summary>
    /// Makes <paramref name="coordinator"/> complete the saga with
    /// <paramref name="outcome"/> when it is driven, first running
    /// <paramref name="drive"/> (for example, this cluster's own participant
    /// prepare, as the real coordinator does) when one is given.
    /// </summary>
    public static void CompletesWith(
        ICrossClusterSagaCoordinatorGrain coordinator,
        CrossClusterSagaOutcome outcome,
        Func<Task>? drive = null)
    {
        CrossClusterSagaOutcome? recorded = null;
        coordinator.BeginAsync(
                Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
                Arg.Any<string?>())
            .Returns(_ => Task.FromResult(recorded is { } done ? Completed(done) : Preparing));
        coordinator.DriveAsync().Returns(async _ =>
        {
            if (drive is not null)
            {
                await drive();
            }

            recorded = outcome;
        });
        coordinator.GetStatusAsync()
            .Returns(_ => Task.FromResult(recorded is { } done ? Completed(done) : Preparing));
    }
}
