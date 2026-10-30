using NSubstitute;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The projection-version arm of the aggregation maintainer's activation: a
/// numeric view whose persisted projection version differs from the registered
/// projection's (a changed filter or selector version) is current-state, so it
/// must rebuild from source rather than adopt the new version forward over
/// members the old definition extracted. The matching-version case is the
/// negative control. This is the caller-level detector for the
/// <c>ChangeFilter</c> and <c>Rebind</c> rows of the numeric-projection
/// companion (<c>spec/view-numeric</c>).
/// </summary>
public partial class ViewMaintainerAggregationDrainTests
{
    private static Harness CreateActivatable()
    {
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        var h = Create(reminderRegistry: reminders);
        ArmDrain(h, new WalDrainResult());
        return h;
    }

    [Test]
    public async Task Aggregation_activation_rebuilds_when_the_persisted_projection_version_differs()
    {
        var h = CreateActivatable();
        h.State.State.ProjectionVersion = "agg-v0";

        await h.Grain.EnsureActiveAsync();

        AssertEscalatedToRebuild(h);
        Assert.That(h.State.State.ProjectionVersion, Is.EqualTo("agg-v1"),
            "the rebuilt view records the registered projection version");
    }

    [Test]
    public async Task Aggregation_activation_keeps_the_view_when_the_projection_version_matches()
    {
        var h = CreateActivatable();
        h.State.State.ProjectionVersion = "agg-v1";

        await h.Grain.EnsureActiveAsync();

        AssertNoRebuild(h);
    }
}
