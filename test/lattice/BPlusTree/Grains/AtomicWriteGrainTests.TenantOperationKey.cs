using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the composite grain key split. The
/// <see cref="AtomicWriteGrain"/> grain key is <c>{treeId}/{operationId}</c>, and a
/// tenant-composed tree id is itself segmented (<c>t/{tenantId}/{name}</c>), so the
/// tree-id / operation-id boundary is the <b>last</b> separator, never the first.
/// Splitting on the first lands inside the tree id and yields a plausible-but-wrong
/// operation id (<c>acme/orders/op-123</c> instead of <c>op-123</c>) and a wrong tree
/// tag (<c>t</c>), and it is invisible on a tenancy-off cluster where a bare tree id
/// contains exactly one separator and both splits agree - which is precisely why every
/// case here uses a tenant-composed tree id.
/// </summary>
public partial class AtomicWriteGrainTests
{
    private const string TenantTreeId = "t/acme/orders";

    /// <summary>
    /// Anchors the fixture's tree id to the real tenant-composed grammar, so these
    /// cases keep exercising a segmented tree id rather than an arbitrary string.
    /// </summary>
    [Test]
    public void TenantTreeId_matches_the_composed_tenant_tree_grammar() =>
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantTrees.IsTenantScoped(TenantTreeId), Is.True);
            Assert.That(LatticeTenantTrees.LocalName(TenantTreeId), Is.EqualTo("orders"));
            Assert.That(TenantTreeId.Count(c => c == '/'), Is.EqualTo(2),
                "A tenant-composed tree id carries more than one separator - which is what makes "
                + "first-slash and last-slash splits disagree.");
        });

    // ---- OperationIdOnly -------------------------------------------------

    [Test]
    public void ExecuteAsync_key_mismatch_reports_the_operation_id_alone_for_a_tenant_composed_tree_id()
    {
        var original = MakeEntries(("k1", [1]), ("k2", [2]));
        var seeded = new FakePersistentState<AtomicWriteState>
        {
            State =
            {
                Phase = AtomicWritePhase.Execute,
                TreeId = TenantTreeId,
                Entries = original,
                KeyFingerprint = AtomicWriteGrain.ComputeKeyFingerprint(original),
            },
        };

        var (grain, _, _, _, _) = CreateGrain(existingState: seeded, treeId: TenantTreeId);
        var mismatched = MakeEntries(("k1", [1]), ("DIFFERENT", [9]));

        var ex = Assert.ThrowsAsync<LatticeIdempotencyKeyMismatchException>(
            () => grain.ExecuteAsync(TenantTreeId, mismatched));

        Assert.That(ex!.OperationId, Is.EqualTo(OperationId),
            "The attributed operation id must be the caller's operationId alone, not the tail "
            + "of a first-slash split through the tenant-composed tree id.");
    }

    // ---- StampOperationIdContext ----------------------------------------

    [Test]
    public async Task ExecuteAsync_stamps_the_operation_id_alone_into_request_context_for_a_tenant_composed_tree_id()
    {
        var (grain, _, _, lattice, shard) = CreateGrain(treeId: TenantTreeId);
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

        // RequestContext rides an AsyncLocal, so a stamp made inside the saga is
        // not visible to this method once ExecuteAsync returns. Observe it from
        // inside the downstream write instead - which is exactly the vantage
        // point that matters, because that is the call whose emitted events pick
        // the correlation id up.
        object? observedAtWrite = null;
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ =>
            {
                observedAtWrite = RequestContext.Get(LatticeEventConstants.OperationIdRequestContextKey);
                return Task.CompletedTask;
            });

        await grain.ExecuteAsync(TenantTreeId, MakeEntries(("a", [1])));

        Assert.That(observedAtWrite, Is.EqualTo(OperationId),
            "Every per-key event the saga emits correlates on this value, so it must be the "
            + "caller's operationId alone.");
    }

    // ---- PublishCompletedEventAsync -------------------------------------

    [Test]
    public async Task ExecuteAsync_publishes_the_operation_id_alone_on_the_completed_event_for_a_tenant_composed_tree_id()
    {
        RequestContext.Remove(LatticeEventConstants.OperationIdRequestContextKey);
        try
        {
            var published = new List<LatticeTreeEvent>();
            var stream = Substitute.For<IAsyncStream<LatticeTreeEvent>>();
            stream.OnNextAsync(Arg.Any<LatticeTreeEvent>(), Arg.Any<StreamSequenceToken?>())
                .Returns(callInfo =>
                {
                    published.Add((LatticeTreeEvent)callInfo[0]);
                    return Task.CompletedTask;
                });

            var streamProvider = Substitute.For<IStreamProvider>();
            streamProvider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>()).Returns(stream);

            var services = new ServiceCollection()
                .AddKeyedSingleton(LatticeOptions.DefaultEventStreamProviderName, streamProvider)
                .BuildServiceProvider();

            var options = new LatticeOptions { PublishEvents = true };
            var (grain, _, _, _, shard) = CreateGrain(
                options: options, treeId: TenantTreeId, activationServices: services);
            shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

            await grain.ExecuteAsync(TenantTreeId, MakeEntries(("a", [1])));

            var completed = published.SingleOrDefault(
                e => e.Kind == LatticeTreeEventKind.AtomicWriteCompleted);
            Assert.That(completed, Is.Not.Null,
                "The saga must emit its terminal atomic-write-completed event.");
            Assert.That(completed!.TreeId, Is.EqualTo(TenantTreeId));
            Assert.That(completed.OperationId, Is.EqualTo(OperationId),
                "A consumer joining this event to the saga's per-key events matches on the "
                + "operation id, so it must be the caller's operationId alone.");
        }
        finally
        {
            RequestContext.Remove(LatticeEventConstants.OperationIdRequestContextKey);
        }
    }

    // ---- GetSagaMetricTags ----------------------------------------------

    [Test]
    public async Task ExecuteAsync_tags_saga_metrics_with_the_whole_tenant_composed_tree_id()
    {
        var observedTrees = new List<string?>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == LatticeMetrics.MeterName
                && instrument.Name == LatticeMetrics.SagaFanoutSize.Name)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeMetrics.TagTree)
                {
                    observedTrees.Add(tag.Value as string);
                }
            }
        });
        listener.Start();

        var (grain, _, _, _, shard) = CreateGrain(treeId: TenantTreeId);
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

        await grain.ExecuteAsync(TenantTreeId, MakeEntries(("a", [1])));

        Assert.That(observedTrees, Is.Not.Empty, "The saga must observe its fan-out size.");
        Assert.That(observedTrees, Has.All.EqualTo(TenantTreeId),
            "A first-slash split reports tree=\"t\", collapsing every tenant's sagas onto one "
            + "meaningless series and mis-keying the per-tree options lookup.");
    }
}
