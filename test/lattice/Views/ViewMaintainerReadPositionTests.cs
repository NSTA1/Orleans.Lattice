using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for the read positions a view maintainer answers to WAL GC
/// through <see cref="IWalOffsetConsumer.GetDurableReadPositionsAsync"/>
/// across a restart and a decommission. These are the transitions the
/// <c>MaterialisedViewTail</c> formal model's <c>Crash</c> and <c>Delete</c>
/// actions abstract: a reactivated maintainer must answer for its persisted
/// bound physical log at its persisted resume point (never for a log it no
/// longer reads), and a decommissioned one must hold nothing anywhere.
/// </summary>
[TestFixture]
public class ViewMaintainerReadPositionTests
{
    private const string ViewName = "orders-view";
    private const string LogicalTreeId = "orders";
    private const string BoundPhysicalTreeId = "orders-resized";

    private static (ViewMaintainerGrain Grain, FakePersistentState<ViewCheckpointState> State) Create(
        string boundPhysicalTreeId,
        Dictionary<int, long> appliedOffsets)
    {
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(ViewName).Returns(
            new ViewRegistration(ViewName, LogicalTreeId, Substitute.For<ILatticeViewProjection>()));

        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>([]));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));

        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.BoundPhysicalTreeId = boundPhysicalTreeId;
        state.State.AppliedOffsets = appliedOffsets;

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            Substitute.For<IReminderRegistry>(),
            NullLogger<ViewMaintainerGrain>.Instance,
            catalog,
            commitLogReader: null!,
            subscriber: null!,
            cursorRegistry: Substitute.For<IWalCursorRegistry>(),
            optionsResolver: null!,
            viewOptions: null!,
            latticeOptions: null!,
            replicationContext: null!,
            saturationSignal: null,
            historyRowCodec: null!,
            state);

        return (grain, state);
    }

    private static Task ActivateAsync(ViewMaintainerGrain grain) =>
        ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

    [Test]
    public async Task A_maintainer_answers_for_the_whole_log_before_its_activation_publishes()
    {
        var (grain, _) = Create(BoundPhysicalTreeId, new Dictionary<int, long> { [0] = 4 });

        var positions = await grain.GetDurableReadPositionsAsync(BoundPhysicalTreeId);

        Assert.That(positions, Is.Not.Null.And.Empty,
            "Before the activation hook publishes, the maintainer must hold the whole log rather than nothing.");
    }

    [Test]
    public async Task A_restarted_maintainer_answers_for_its_persisted_bound_log_at_its_persisted_resume_point()
    {
        var (grain, _) = Create(BoundPhysicalTreeId, new Dictionary<int, long> { [0] = 4, [2] = 9 });

        await ActivateAsync(grain);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await grain.GetDurableReadPositionsAsync(BoundPhysicalTreeId), Is.EqualTo(new long[] { 5, 0, 10 }),
                "The first unread offset per partition of the persisted bound log must be held after a restart.");
            Assert.That(await grain.GetDurableReadPositionsAsync(LogicalTreeId), Is.Null,
                "A log other than the bound physical one is not read, so a stale registration there must hold nothing.");
        });
    }

    [Test]
    public async Task A_restarted_maintainer_that_never_bound_a_log_holds_none()
    {
        var (grain, _) = Create(string.Empty, new Dictionary<int, long>());

        await ActivateAsync(grain);

        Assert.That(await grain.GetDurableReadPositionsAsync(LogicalTreeId), Is.Null);
    }

    [Test]
    public async Task A_decommissioned_maintainer_holds_no_log()
    {
        var (grain, _) = Create(BoundPhysicalTreeId, new Dictionary<int, long> { [0] = 4 });
        await ActivateAsync(grain);

        await grain.DecommissionAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await grain.GetDurableReadPositionsAsync(BoundPhysicalTreeId), Is.Null,
                "Decommission must stop the view holding its bound physical log.");
            Assert.That(await grain.GetDurableReadPositionsAsync(LogicalTreeId), Is.Null,
                "Decommission must stop the view holding the logical log.");
        });
    }
}
