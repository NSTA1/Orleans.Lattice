using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Issue #6097: superseded view-tree generations are deleted through the
/// ordinary tree-deletion path rather than merely emptied, oldest first, one
/// deletion per paced pass, and generations an earlier release left registered
/// are retired by the same sweep.
/// </summary>
[TestFixture]
public class ViewMaintainerGenerationReclaimTests
{
    private const string ViewName = "kpi-view";

    private sealed record Harness(
        ViewMaintainerGrain Grain,
        FakePersistentState<ViewCheckpointState> State,
        ManualTimeProvider PacerClock,
        HashSet<string> Registered,
        List<string> Deleted,
        Dictionary<string, ILattice> Trees,
        ILatticeRegistry Registry);

    private static Harness Create(
        long activeGeneration,
        IEnumerable<long> registeredGenerations,
        long? floor = null,
        Func<string, ILattice>? treeFactory = null)
    {
        var registered = new HashSet<string>(registeredGenerations.Select(TreeIdFor), StringComparer.Ordinal);
        var deleted = new List<string>();
        var trees = new Dictionary<string, ILattice>(StringComparer.Ordinal);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            var treeId = call.ArgAt<string>(0);
            if (!trees.TryGetValue(treeId, out var tree))
            {
                if (treeFactory is not null)
                {
                    tree = treeFactory(treeId);
                }
                else
                {
                    tree = Substitute.For<ILattice>();
                    tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).Returns(_ =>
                    {
                        deleted.Add(treeId);
                        return Task.CompletedTask;
                    });
                }

                trees[treeId] = tree;
            }

            return tree;
        });
        factory.GetGrain<ITreeDeletionGrain>(Arg.Any<string>()).Returns(call =>
        {
            var treeId = call.ArgAt<string>(0);
            var deletion = Substitute.For<ITreeDeletionGrain>();
            deletion.HoldsDeletionAsync().Returns(_ => Task.FromResult(deleted.Contains(treeId)));
            return deletion;
        });

        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(call => Task.FromResult(
            registered.Contains(call.ArgAt<string>(0))
                ? new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }
                : null));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));

        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.ActiveGeneration = activeGeneration;
        state.State.LegacyGenerationCeiling = 0;
        state.State.ReclaimedGenerationFloor = floor;

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            Substitute.For<IReminderRegistry>(),
            NullLogger<ViewMaintainerGrain>.Instance,
            Substitute.For<IViewCatalog>(),
            commitLogReader: null!,
            subscriber: null!,
            cursorRegistry: Substitute.For<IWalCursorRegistry>(),
            optionsResolver: TestOptionsResolver.ForFactory(factory),
            viewOptions: null!,
            latticeOptions: null!,
            replicationContext: null!,
            saturationSignal: null,
            historyRowCodec: null!,
            state);

        var pacerClock = new ManualTimeProvider();
        grain.ReclaimPacer = new ViewGenerationReclaimPacer { TimeProvider = pacerClock };
        return new Harness(grain, state, pacerClock, registered, deleted, trees, registry);
    }

    private static string TreeIdFor(long generation) =>
        LatticeViewTrees.ComposeTreeId(ViewName, generation, useLegacySeparator: generation <= 0);

    /// <summary>Runs one pass, then opens the pacer for the next.</summary>
    private static async Task PassAsync(Harness h)
    {
        await h.Grain.TryReclaimSupersededGenerationsAsync(CancellationToken.None);
        h.PacerClock.Advance(ViewGenerationReclaimPacer.DefaultInterval);
    }

    [Test]
    public async Task Leaked_generations_are_deleted_oldest_first_one_per_pass()
    {
        // The shape the issue reports: a view on generation 3 whose three earlier
        // generations an earlier release emptied but left registered.
        var h = Create(activeGeneration: 3, registeredGenerations: [0, 1, 2, 3, 4]);

        await PassAsync(h);
        Assert.That(h.Deleted, Is.EqualTo(new[] { TreeIdFor(0) }));

        await PassAsync(h);
        await PassAsync(h);
        await PassAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(h.Deleted, Is.EqualTo(new[] { TreeIdFor(0), TreeIdFor(1), TreeIdFor(2) }),
                "neither the active generation nor the shadow above it is ever deleted");
            Assert.That(h.State.State.ReclaimedGenerationFloor, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task A_pass_refused_by_the_pacer_does_no_io()
    {
        var h = Create(activeGeneration: 2, registeredGenerations: [0, 1, 2]);
        await h.Grain.TryReclaimSupersededGenerationsAsync(CancellationToken.None);
        h.Registry.ClearReceivedCalls();

        await h.Grain.TryReclaimSupersededGenerationsAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(h.Deleted, Has.Count.EqualTo(1));
            Assert.That(h.Registry.ReceivedCalls(), Is.Empty);
        });
    }

    [Test]
    public async Task Generations_already_gone_are_passed_over_without_a_deletion()
    {
        // Generations 0-3 were never registered or are already purged, and 4 was
        // deleted by an earlier pass: none needs a deletion, and one pass covers
        // them before deleting 5.
        var h = Create(activeGeneration: 6, registeredGenerations: [4, 5, 6]);
        h.Deleted.Add(TreeIdFor(4));

        await PassAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(h.Deleted, Is.EqualTo(new[] { TreeIdFor(4), TreeIdFor(5) }));
            Assert.That(h.State.State.ReclaimedGenerationFloor, Is.EqualTo(6));
            Assert.That(h.Trees.ContainsKey(TreeIdFor(0)), Is.False,
                "an unregistered generation's router is never activated");
        });
    }

    [Test]
    public async Task A_pass_probes_a_bounded_number_of_generations()
    {
        var h = Create(activeGeneration: 100, registeredGenerations: []);

        await PassAsync(h);

        Assert.That(h.State.State.ReclaimedGenerationFloor, Is.EqualTo(ViewMaintainerGrain.MaxReclaimProbesPerPass));
    }

    [Test]
    public async Task The_most_recent_swap_keeps_its_generation_until_the_reader_grace_elapses()
    {
        var h = Create(activeGeneration: 2, registeredGenerations: [0, 1, 2], floor: 0);
        h.State.State.HasPendingReclaim = true;
        h.State.State.PendingReclaimGeneration = 1;
        h.State.State.ReclaimEligibleAtTicks = DateTime.MaxValue.Ticks;

        await PassAsync(h);
        await PassAsync(h);

        Assert.That(h.Deleted, Is.EqualTo(new[] { TreeIdFor(0) }),
            "an older generation is reclaimed, the one still inside its grace is not");

        h.State.State.ReclaimEligibleAtTicks = 0;
        await PassAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(h.Deleted, Is.EqualTo(new[] { TreeIdFor(0), TreeIdFor(1) }));
            Assert.That(h.State.State.HasPendingReclaim, Is.False);
        });
    }

    [Test]
    public async Task A_failed_deletion_is_retried_after_a_back_off_without_failing_the_caller()
    {
        var failing = true;
        var h = Create(activeGeneration: 3, registeredGenerations: [0, 1, 2], floor: 0, treeFactory: treeId =>
        {
            var tree = Substitute.For<ILattice>();
            if (treeId == TreeIdFor(1))
            {
                tree.DeleteTreeAsync(Arg.Any<CancellationToken>())
                    .Returns(_ => failing ? throw new TimeoutException("shard unavailable") : Task.CompletedTask);
            }

            return tree;
        });

        await PassAsync(h);
        Assert.That(async () => await h.Grain.TryReclaimSupersededGenerationsAsync(CancellationToken.None), Throws.Nothing);
        Assert.That(h.State.State.ReclaimedGenerationFloor, Is.EqualTo(1),
            "the floor stops at the generation that could not be deleted");

        failing = false;
        h.PacerClock.Advance(ViewGenerationReclaimPacer.DefaultInterval);
        await PassAsync(h);
        Assert.That(h.State.State.ReclaimedGenerationFloor, Is.EqualTo(1), "the back-off has not elapsed");
    }

    [Test]
    public async Task Nothing_to_reclaim_does_no_io()
    {
        var h = Create(activeGeneration: 2, registeredGenerations: [2], floor: 2);

        await PassAsync(h);

        Assert.Multiple(() =>
        {
            Assert.That(h.Registry.ReceivedCalls(), Is.Empty);
            Assert.That(h.State.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_pending_record_the_sweep_has_passed_is_cleared()
    {
        var h = Create(activeGeneration: 3, registeredGenerations: [3], floor: 3);
        h.State.State.HasPendingReclaim = true;
        h.State.State.PendingReclaimGeneration = 1;
        h.State.State.ReclaimEligibleAtTicks = DateTime.MaxValue.Ticks;

        await PassAsync(h);

        Assert.That(h.State.State.HasPendingReclaim, Is.False);
    }

    [Test]
    public void Retry_delay_doubles_and_saturates()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ViewMaintainerGrain.ReclaimRetryDelay(1), Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(ViewMaintainerGrain.ReclaimRetryDelay(2), Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(ViewMaintainerGrain.ReclaimRetryDelay(1000), Is.EqualTo(TimeSpan.FromMinutes(30)));
        });
    }
}
