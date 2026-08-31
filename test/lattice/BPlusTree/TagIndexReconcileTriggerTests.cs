using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="TagIndexReconcileTrigger"/>: the default
/// <see cref="ITagIndexReconcileTrigger"/> that discovers the tag indexes covering a
/// swapped subject tree by prefix-scanning the tree registry for <c>tag-{indexName}</c>
/// entries and fires a coverage-gated reconcile on each index's coordinator.
/// <para>
/// The trigger is deliberately best-effort: the recurring scheduled sweep is the
/// correctness backstop, so neither a registry enumeration failure nor a single index's
/// reconcile failure may fault the physical-identity swap that fired it. These tests pin
/// that swallow-and-continue contract, the <c>tag-</c> prefix filter, the index-name
/// derivation, and the cancellation seam.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class TagIndexReconcileTriggerTests
{
    private static IGrainFactory FactoryWith(
        out ILatticeRegistry registry,
        out Dictionary<string, ITagIndexReconcileGrain> indexes,
        params string[] registeredTreeIds)
    {
        registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(registeredTreeIds));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var created = new Dictionary<string, ITagIndexReconcileGrain>(StringComparer.Ordinal);
        factory.GetGrain<ITagIndexReconcileGrain>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(call =>
            {
                var key = call.ArgAt<string>(0);
                if (!created.TryGetValue(key, out var grain))
                {
                    grain = Substitute.For<ITagIndexReconcileGrain>();
                    grain.ReconcileTreeAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
                    created[key] = grain;
                }

                return grain;
            });

        indexes = created;
        return factory;
    }

    private static TagIndexReconcileTrigger CreateTrigger(IGrainFactory factory) =>
        new(factory, Substitute.For<ILogger<TagIndexReconcileTrigger>>());

    [Test]
    public void TriggerForTreeAsync_rejects_a_null_subject_tree_id()
    {
        var trigger = CreateTrigger(FactoryWith(out _, out _));

        Assert.ThrowsAsync<ArgumentNullException>(() => trigger.TriggerForTreeAsync(null!));
    }

    [Test]
    public async Task TriggerForTreeAsync_pushes_the_tag_prefix_down_to_the_registry_scan()
    {
        var factory = FactoryWith(out var registry, out _, "tag-byOwner");

        await CreateTrigger(factory).TriggerForTreeAsync("orders");

        // The bounded range scan is the point: a full catalog read would be filtered
        // client-side instead, so assert the prefix actually reaches the registry.
        await registry.Received(1).GetAllTreeIdsAsync("tag-");
    }

    [Test]
    public async Task TriggerForTreeAsync_reconciles_every_registered_tag_index()
    {
        var factory = FactoryWith(out _, out var indexes, "tag-byOwner", "tag-byStatus");

        await CreateTrigger(factory).TriggerForTreeAsync("orders");

        Assert.That(indexes.Keys, Is.EquivalentTo(new[] { "byOwner", "byStatus" }));
        await indexes["byOwner"].Received(1).ReconcileTreeAsync("orders");
        await indexes["byStatus"].Received(1).ReconcileTreeAsync("orders");
    }

    [Test]
    public async Task TriggerForTreeAsync_strips_the_tag_prefix_to_derive_the_index_name()
    {
        var factory = FactoryWith(out _, out var indexes, "tag-tag-nested");

        await CreateTrigger(factory).TriggerForTreeAsync("orders");

        // Only the first prefix occurrence is stripped: the index name is the
        // remainder verbatim, so a legitimately "tag-"-prefixed index name survives.
        Assert.That(indexes.Keys, Is.EquivalentTo(new[] { "tag-nested" }));
    }

    [Test]
    public async Task TriggerForTreeAsync_skips_registered_trees_without_the_tag_prefix()
    {
        // A registry that ignores the prefix hint (or a legacy implementation that
        // returns everything) must not cause a non-index tree to be treated as one.
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["orders", "tag-byOwner", "customers"]));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var index = Substitute.For<ITagIndexReconcileGrain>();
        index.ReconcileTreeAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        factory.GetGrain<ITagIndexReconcileGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(index);

        await CreateTrigger(factory).TriggerForTreeAsync("orders");

        factory.Received(1).GetGrain<ITagIndexReconcileGrain>("byOwner", Arg.Any<string?>());
        await index.Received(1).ReconcileTreeAsync("orders");
    }

    [Test]
    public async Task TriggerForTreeAsync_is_a_no_op_when_no_tag_index_is_registered()
    {
        var factory = FactoryWith(out _, out var indexes);

        await CreateTrigger(factory).TriggerForTreeAsync("orders");

        Assert.That(indexes, Is.Empty);
    }

    [Test]
    public async Task TriggerForTreeAsync_swallows_a_registry_enumeration_failure()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Throws(new InvalidOperationException("registry unavailable"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // Best-effort: the swap that fired the trigger must not fault.
        Assert.DoesNotThrowAsync(() => CreateTrigger(factory).TriggerForTreeAsync("orders"));

        factory.DidNotReceiveWithAnyArgs().GetGrain<ITagIndexReconcileGrain>(default!, default);
        await Task.CompletedTask;
    }

    [Test]
    public void TriggerForTreeAsync_logs_a_warning_when_enumeration_fails()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Throws(new InvalidOperationException("registry unavailable"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var logger = Substitute.For<ILogger<TagIndexReconcileTrigger>>();

        Assert.DoesNotThrowAsync(() => new TagIndexReconcileTrigger(factory, logger).TriggerForTreeAsync("orders"));

        logger.ReceivedWithAnyArgs(1).Log(
            LogLevel.Warning, default, Arg.Any<object>(), default, Arg.Any<Func<object, Exception?, string>>()!);
    }

    [Test]
    public async Task TriggerForTreeAsync_isolates_a_single_index_reconcile_failure()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["tag-broken", "tag-healthy"]));

        var broken = Substitute.For<ITagIndexReconcileGrain>();
        broken.ReconcileTreeAsync(Arg.Any<string>()).Throws(new TimeoutException("index busy"));
        var healthy = Substitute.For<ITagIndexReconcileGrain>();
        healthy.ReconcileTreeAsync(Arg.Any<string>()).Returns(Task.FromResult(true));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        factory.GetGrain<ITagIndexReconcileGrain>("broken", Arg.Any<string?>()).Returns(broken);
        factory.GetGrain<ITagIndexReconcileGrain>("healthy", Arg.Any<string?>()).Returns(healthy);

        Assert.DoesNotThrowAsync(() => CreateTrigger(factory).TriggerForTreeAsync("orders"));

        // The failure is swallowed per index, so the later index is still reconciled.
        await healthy.Received(1).ReconcileTreeAsync("orders");
    }

    [Test]
    public void TriggerForTreeAsync_observes_cancellation_between_indexes()
    {
        var factory = FactoryWith(out _, out var indexes, "tag-byOwner", "tag-byStatus");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            () => CreateTrigger(factory).TriggerForTreeAsync("orders", cts.Token));

        // Cancellation is checked before the first reconcile, so none is dispatched.
        Assert.That(indexes, Is.Empty);
    }

    [Test]
    public void TriggerForTreeAsync_cancellation_is_not_swallowed_as_a_reconcile_failure()
    {
        // The per-index catch must not absorb the cooperative cancellation the loop
        // raises, otherwise a shutting-down silo would keep dispatching reconciles.
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["tag-byOwner"]));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            () => CreateTrigger(factory).TriggerForTreeAsync("orders", cts.Token));
    }
}
