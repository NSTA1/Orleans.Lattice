using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for <see cref="IViewMaintainerGrain.DecommissionAsync"/> - the
/// view teardown that reverses everything an active maintainer establishes: the
/// keepalive reminder, the source WAL cursor pin, every backing view-tree
/// generation, and the durable checkpoint.
/// <para>
/// Teardown is deliberately best-effort at every step: a failure to release the
/// pin, read the durable registry, unregister the reminder, or delete one backing
/// generation must be logged and stepped over rather than aborting the teardown
/// and stranding the remaining resources. These tests drive the grain directly
/// (the construction pattern established by
/// <see cref="ViewMaintainerSourceIdentityTests"/>) so each of those arms can be
/// faulted independently.
/// </para>
/// </summary>
[TestFixture]
public class ViewMaintainerDecommissionTests
{
    private const string ViewName = "orders-view";
    private const string SourceTreeId = "orders";
    private const string ConsumerId = "view:" + ViewName;

    private sealed record Harness(
        ViewMaintainerGrain Grain,
        FakePersistentState<ViewCheckpointState> State,
        IWalCursorRegistry CursorRegistry,
        IReminderRegistry Reminders,
        IViewCatalog Catalog,
        IViewRegistryGrain Registry,
        IGrainFactory Factory,
        Dictionary<string, ILattice> Trees);

    /// <summary>
    /// Builds a maintainer whose teardown collaborators are all observable. Every
    /// <see cref="ILattice"/> handed out by the grain factory is recorded by tree
    /// id in <c>Trees</c>, so which generations were deleted is asserted directly
    /// rather than inferred.
    /// </summary>
    private static Harness Create(
        string? catalogSourceTreeId = SourceTreeId,
        long activeGeneration = 0,
        bool hasPendingReclaim = false,
        long pendingReclaimGeneration = 0,
        IWalCursorRegistry? cursorRegistry = null,
        IReminderRegistry? reminderRegistry = null,
        IViewRegistryGrain? registryGrain = null,
        Func<string, ILattice>? treeFactory = null)
    {
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(ViewName).Returns(
            catalogSourceTreeId is null
                ? null
                : new ViewRegistration(ViewName, catalogSourceTreeId, Substitute.For<ILatticeViewProjection>()));

        var trees = new Dictionary<string, ILattice>(StringComparer.Ordinal);
        // The default stub goes INSIDE the null branch: applying it to a
        // caller-supplied registry would silently overwrite the fault the test
        // just injected (and re-invoking ListAsync here would itself throw).
        IViewRegistryGrain registry;
        if (registryGrain is null)
        {
            registry = Substitute.For<IViewRegistryGrain>();
            registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>([]));
        }
        else
        {
            registry = registryGrain;
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            var treeId = call.ArgAt<string>(0);
            if (!trees.TryGetValue(treeId, out var tree))
            {
                tree = treeFactory?.Invoke(treeId) ?? Substitute.For<ILattice>();
                trees[treeId] = tree;
            }
            return tree;
        });

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));

        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.ActiveGeneration = activeGeneration;
        state.State.HasPendingReclaim = hasPendingReclaim;
        state.State.PendingReclaimGeneration = pendingReclaimGeneration;

        // Materialised once so the harness hands back the SAME instances the grain
        // was constructed with - a second Substitute.For here would silently
        // observe a collaborator the grain never calls.
        var cursors = cursorRegistry ?? Substitute.For<IWalCursorRegistry>();
        var reminders = reminderRegistry ?? Substitute.For<IReminderRegistry>();

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            reminders,
            NullLogger<ViewMaintainerGrain>.Instance,
            catalog,
            commitLogReader: null!,
            subscriber: null!,
            cursorRegistry: cursors,
            optionsResolver: null!,
            viewOptions: null!,
            latticeOptions: null!,
            replicationContext: null!,
            saturationSignal: null,
            historyRowCodec: null!,
            state);

        return new Harness(grain, state, cursors, reminders, catalog, registry, factory, trees);
    }

    private static string TreeIdFor(long generation) =>
        LatticeViewTrees.ComposeTreeId(ViewName, generation, useLegacySeparator: generation <= 0);

    // ------------------------------------------------------------- happy path

    [Test]
    public async Task Decommission_releases_the_pin_deletes_the_tree_and_clears_the_checkpoint()
    {
        var cursors = Substitute.For<IWalCursorRegistry>();
        var reminders = Substitute.For<IReminderRegistry>();
        var reminder = Substitute.For<IGrainReminder>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>()).Returns(reminder);
        var h = Create(cursorRegistry: cursors, reminderRegistry: reminders);

        await h.Grain.DecommissionAsync();

        await cursors.Received(1).UnregisterAsync(SourceTreeId, ConsumerId, Arg.Any<CancellationToken>());
        await reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
        await h.Trees[TreeIdFor(0)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1),
                "The checkpoint must advance past every generation just deleted so a re-created "
                + "view never addresses a permanently inaccessible tree id.");
            Assert.That(h.State.WriteCount, Is.GreaterThanOrEqualTo(1),
                "The cleared checkpoint must be persisted.");
        });
    }

    [Test]
    public async Task Decommission_deletes_every_generation_up_to_and_including_the_active_one()
    {
        var h = Create(activeGeneration: 2);

        await h.Grain.DecommissionAsync();

        foreach (var generation in new[] { 0L, 1L, 2L })
        {
            await h.Trees[TreeIdFor(generation)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        }
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(3));
    }

    [Test]
    public async Task Decommission_also_deletes_a_generation_awaiting_reclamation()
    {
        // A generation awaiting reclamation still holds shard state - reclamation
        // only clears its keys - so teardown must delete it too.
        var h = Create(activeGeneration: 1, hasPendingReclaim: true, pendingReclaimGeneration: 5);

        await h.Grain.DecommissionAsync();

        await h.Trees[TreeIdFor(5)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(6),
            "The checkpoint must advance past the HIGHEST deleted generation, not merely the active one.");
    }

    [Test]
    public async Task Decommission_ignores_a_pending_reclaim_generation_that_is_not_flagged()
    {
        var h = Create(activeGeneration: 0, hasPendingReclaim: false, pendingReclaimGeneration: 9);

        await h.Grain.DecommissionAsync();

        Assert.That(h.Trees.ContainsKey(TreeIdFor(9)), Is.False,
            "An unflagged reclaim generation must not be treated as a backing tree.");
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
    }

    [Test]
    public async Task Decommission_resets_the_whole_checkpoint_not_just_the_generation()
    {
        var h = Create(activeGeneration: 1, hasPendingReclaim: true, pendingReclaimGeneration: 1);
        h.State.State.AppliedOffsets = new Dictionary<int, long> { [0] = 42 };
        h.State.State.ProjectionVersion = "v1";
        h.State.State.BoundPhysicalTreeId = "orders-physical";

        await h.Grain.DecommissionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.AppliedOffsets, Is.Empty);
            Assert.That(h.State.State.ProjectionVersion, Is.Empty.Or.Null);
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.Empty.Or.Null);
            Assert.That(h.State.State.HasPendingReclaim, Is.False);
        });
    }

    // ------------------------------------------------- durable-registry fallback

    [Test]
    public async Task An_empty_catalog_falls_back_to_the_durable_registry_for_the_source_tree_id()
    {
        // A maintainer can activate fresh on a silo whose catalog never saw the
        // runtime Create, yet the pin reported by an earlier activation still holds
        // the source WAL GC and must be released.
        var cursors = Substitute.For<IWalCursorRegistry>();
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(
        [
            new RuntimeViewRegistration
            {
                ViewName = "some-other-view",
                SourceTreeId = "other-source",
                ProjectionTypeName = "T",
                ProjectionVersion = "v1",
            },
            new RuntimeViewRegistration
            {
                ViewName = ViewName,
                SourceTreeId = "durable-source",
                ProjectionTypeName = "T",
                ProjectionVersion = "v1",
            },
        ]));
        var h = Create(catalogSourceTreeId: null, cursorRegistry: cursors, registryGrain: registry);

        await h.Grain.DecommissionAsync();

        await cursors.Received(1).UnregisterAsync("durable-source", ConsumerId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task No_pin_is_released_when_neither_the_catalog_nor_the_registry_knows_the_source()
    {
        var cursors = Substitute.For<IWalCursorRegistry>();
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(
        [
            new RuntimeViewRegistration
            {
                ViewName = "unrelated",
                SourceTreeId = "other",
                ProjectionTypeName = "T",
                ProjectionVersion = "v1",
            },
        ]));
        var h = Create(catalogSourceTreeId: null, cursorRegistry: cursors, registryGrain: registry);

        await h.Grain.DecommissionAsync();

        await cursors.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1),
            "Teardown must still complete when the source id cannot be resolved.");
    }

    [Test]
    public async Task An_unreadable_durable_registry_does_not_abort_teardown()
    {
        var cursors = Substitute.For<IWalCursorRegistry>();
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Throws(new TimeoutException("registry unreachable"));
        var h = Create(catalogSourceTreeId: null, cursorRegistry: cursors, registryGrain: registry);

        await h.Grain.DecommissionAsync();

        await cursors.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await h.Trees[TreeIdFor(0)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
    }

    [Test]
    public async Task An_empty_source_tree_id_skips_the_pin_release()
    {
        var cursors = Substitute.For<IWalCursorRegistry>();
        var h = Create(catalogSourceTreeId: "", cursorRegistry: cursors);

        await h.Grain.DecommissionAsync();

        await cursors.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------- best-effort arms

    [Test]
    public async Task A_failing_pin_release_does_not_abort_teardown()
    {
        var cursors = Substitute.For<IWalCursorRegistry>();
        cursors.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("cursor registry down"));
        var h = Create(cursorRegistry: cursors);

        await h.Grain.DecommissionAsync();

        await h.Trees[TreeIdFor(0)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1),
            "A stranded pin must not leave the backing trees and checkpoint behind as well.");
    }

    [Test]
    public async Task A_failing_reminder_lookup_does_not_abort_teardown()
    {
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Throws(new TimeoutException("reminder table unreachable"));
        var h = Create(reminderRegistry: reminders);

        await h.Grain.DecommissionAsync();

        await h.Trees[TreeIdFor(0)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
    }

    [Test]
    public async Task A_failing_reminder_unregister_does_not_abort_teardown()
    {
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Substitute.For<IGrainReminder>());
        reminders.UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>())
            .Throws(new TimeoutException("reminder table unreachable"));
        var h = Create(reminderRegistry: reminders);

        await h.Grain.DecommissionAsync();

        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
    }

    [Test]
    public async Task No_reminder_registered_means_no_unregister_call()
    {
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>()).Returns((IGrainReminder?)null);
        var h = Create(reminderRegistry: reminders);

        await h.Grain.DecommissionAsync();

        await reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task A_failing_generation_delete_is_stepped_over_and_the_rest_still_deleted()
    {
        var h = Create(activeGeneration: 2, treeFactory: treeId =>
        {
            var tree = Substitute.For<ILattice>();
            if (treeId == TreeIdFor(1))
            {
                tree.DeleteTreeAsync(Arg.Any<CancellationToken>())
                    .Throws(new InvalidOperationException("shard unavailable"));
            }
            return tree;
        });

        await h.Grain.DecommissionAsync();

        await h.Trees[TreeIdFor(0)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        await h.Trees[TreeIdFor(2)].Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(3),
            "One undeletable generation must not strand the checkpoint on a dead generation.");
    }

    // ------------------------------------------------------------- plumbing

    [Test]
    public async Task Decommission_forwards_the_cancellation_token_to_the_pin_release()
    {
        var cursors = Substitute.For<IWalCursorRegistry>();
        var h = Create(cursorRegistry: cursors);
        using var cts = new CancellationTokenSource();

        await h.Grain.DecommissionAsync(cts.Token);

        await cursors.Received(1).UnregisterAsync(SourceTreeId, ConsumerId, cts.Token);
    }

    [Test]
    public async Task Teardown_runs_under_a_view_write_scope_that_is_released_afterwards()
    {
        // Deleting a view tree is a maintainer-authorised view write, but the scope
        // must not leak onto the caller's request context after teardown returns.
        var authorisedDuringDelete = false;
        var h = Create(treeFactory: _ =>
        {
            var tree = Substitute.For<ILattice>();
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                authorisedDuringDelete = ViewWriteContext.IsAuthorised;
                return Task.CompletedTask;
            });
            return tree;
        });

        await h.Grain.DecommissionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(authorisedDuringDelete, Is.True,
                "The tree delete must run inside the maintainer view-write scope.");
            Assert.That(ViewWriteContext.IsAuthorised, Is.False,
                "The scope must be released once teardown completes.");
        });
    }

    [Test]
    public async Task Decommission_is_idempotent_across_repeated_calls()
    {
        var cursors = Substitute.For<IWalCursorRegistry>();
        var h = Create(cursorRegistry: cursors);

        await h.Grain.DecommissionAsync();
        await h.Grain.DecommissionAsync();

        // The second pass tears down the generation the first pass advanced to.
        await h.Trees[TreeIdFor(0)].Received(2).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(2));
    }
}
