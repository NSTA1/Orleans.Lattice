using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The lost set and dependency verdicts on the high-water-mark grain (#4603).
/// </summary>
public partial class ReplicationHighWaterMarkGrainTests
{
    [Test]
    public async Task CheckDependenciesAsync_reports_met_unmet_and_lost()
    {
        var grain = CreateGrain();
        await grain.TryAdvanceAsync(OriginA, Hlc(50), CancellationToken.None);
        await grain.RecordLostAsync(OriginB, Hlc(7), CancellationToken.None);

        var verdicts = await grain.CheckDependenciesAsync(
            [Vector((OriginA, Hlc(40))), Vector((OriginA, Hlc(60))), Vector((OriginA, Hlc(40)), (OriginB, Hlc(7)))],
            CancellationToken.None);

        Assert.That(verdicts, Is.EqualTo(new[]
        {
            CausalDependencyVerdict.Met,
            CausalDependencyVerdict.Unmet,
            CausalDependencyVerdict.Lost,
        }));
    }

    [Test]
    public async Task A_lost_dependency_wins_over_an_unmet_one_whatever_its_position()
    {
        var grain = CreateGrain();
        await grain.RecordLostAsync(OriginB, Hlc(7), CancellationToken.None);

        var verdicts = await grain.CheckDependenciesAsync(
            [Vector((OriginA, Hlc(99)), (OriginB, Hlc(7)))],
            CancellationToken.None);

        Assert.That(verdicts, Is.EqualTo(new[] { CausalDependencyVerdict.Lost }));
    }

    [Test]
    public async Task RecordLostAsync_is_durable_and_idempotent()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);

        await grain.RecordLostAsync(OriginA, Hlc(7), CancellationToken.None);
        await grain.RecordLostAsync(OriginA, Hlc(7), CancellationToken.None);

        var reactivated = CreateGrain(state);
        var verdicts = await reactivated.CheckDependenciesAsync([Vector((OriginA, Hlc(7)))], CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Lost[OriginA], Is.EquivalentTo(new[] { Hlc(7) }));
            Assert.That(verdicts, Is.EqualTo(new[] { CausalDependencyVerdict.Lost }));
        });
    }

    [Test]
    public async Task RecordLostAsync_rolls_back_when_the_write_fails()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>
        {
            ThrowOnWrite = new InvalidOperationException("storage down"),
        };
        var grain = CreateGrain(state);

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.RecordLostAsync(OriginA, Hlc(7), CancellationToken.None));

        Assert.That(state.State.Lost, Is.Empty);
    }

    [Test]
    public void Lost_and_dependency_methods_validate_arguments()
    {
        var grain = CreateGrain();

        Assert.Multiple(() =>
        {
            Assert.That(() => grain.RecordLostAsync(null!, Hlc(1), CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => grain.CheckDependenciesAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }
}
