using NSubstitute;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class ViewReplicationTopologyTests
{
    private const string ViewName = "adults";
    private const string SourceTreeId = "people";
    private const string ViewTreeId = "view-adults";

    [Test]
    public void Resolve_replication_disabled_preserves_local_modes()
    {
        var context = Context(enabled: false);

        Assert.Multiple(() =>
        {
            Assert.That(
                Resolve(new LatticeViewOptions(), context),
                Is.EqualTo(ViewReplicationTopology.MaintenanceRole.Maintain));
            Assert.That(
                Resolve(new LatticeViewOptions { ReplicationMode = LatticeViewReplicationMode.ShipView }, context),
                Is.EqualTo(ViewReplicationTopology.MaintenanceRole.InferFromSource));
        });
    }

    [Test]
    public void Resolve_derive_locally_maintains_when_only_source_is_replicated()
    {
        var context = Context(replicatedTrees: [SourceTreeId]);

        Assert.That(
            Resolve(new LatticeViewOptions(), context),
            Is.EqualTo(ViewReplicationTopology.MaintenanceRole.Maintain));
    }

    [Test]
    public void Resolve_derive_locally_rejects_replicated_view_tree()
    {
        var context = Context(replicatedTrees: [SourceTreeId, ViewTreeId]);

        Assert.That(
            () => Resolve(new LatticeViewOptions(), context),
            Throws.InvalidOperationException.With.Message.Contains("multiple writers"));
    }

    [Test]
    public void Resolve_derive_locally_rejects_replicated_active_generation_tree()
    {
        const string generationTreeId = "view-adults#g4";
        var context = Context(replicatedTrees: [SourceTreeId, generationTreeId]);

        Assert.That(
            () => ViewReplicationTopology.Resolve(
                ViewName,
                SourceTreeId,
                new LatticeViewOptions(),
                context,
                generationTreeId),
            Throws.InvalidOperationException.With.Message.Contains(generationTreeId));
    }

    [Test]
    public void Resolve_ship_view_rejects_unreplicated_view_tree()
    {
        var context = Context(replicatedTrees: [SourceTreeId]);
        var options = new LatticeViewOptions { ReplicationMode = LatticeViewReplicationMode.ShipView };

        Assert.That(
            () => Resolve(options, context),
            Throws.InvalidOperationException.With.Message.Contains("never receive"));
    }

    [Test]
    public void Resolve_ship_view_infers_when_source_is_not_replicated()
    {
        var context = Context(replicatedTrees: [ViewTreeId]);
        var options = new LatticeViewOptions { ReplicationMode = LatticeViewReplicationMode.ShipView };

        Assert.That(
            Resolve(options, context),
            Is.EqualTo(ViewReplicationTopology.MaintenanceRole.InferFromSource));
    }

    [Test]
    public void Resolve_ship_view_rejects_explicit_producer_when_source_is_not_replicated()
    {
        var context = Context(replicatedTrees: [ViewTreeId]);
        var options = new LatticeViewOptions
        {
            ReplicationMode = LatticeViewReplicationMode.ShipView,
            ShipViewProducerClusterId = "site-a",
        };

        Assert.That(
            () => Resolve(options, context),
            Throws.InvalidOperationException.With.Message.Contains("Source-less-consumer topology"));
    }

    [Test]
    public void Resolve_ship_view_rejects_ambiguous_replicated_source_without_producer()
    {
        var context = Context(replicatedTrees: [SourceTreeId, ViewTreeId]);
        var options = new LatticeViewOptions { ReplicationMode = LatticeViewReplicationMode.ShipView };

        Assert.That(
            () => Resolve(options, context),
            Throws.InvalidOperationException.With.Message.Contains(nameof(LatticeViewOptions.ShipViewProducerClusterId)));
    }

    [Test]
    public void Resolve_ship_view_maintains_only_on_explicit_producer()
    {
        var context = Context(localReplicaId: "site-a", replicatedTrees: [SourceTreeId, ViewTreeId]);
        var localOptions = new LatticeViewOptions
        {
            ReplicationMode = LatticeViewReplicationMode.ShipView,
            ShipViewProducerClusterId = "site-a",
        };
        var remoteOptions = new LatticeViewOptions
        {
            ReplicationMode = LatticeViewReplicationMode.ShipView,
            ShipViewProducerClusterId = "site-b",
        };

        Assert.Multiple(() =>
        {
            Assert.That(
                Resolve(localOptions, context),
                Is.EqualTo(ViewReplicationTopology.MaintenanceRole.Maintain));
            Assert.That(
                Resolve(remoteOptions, context),
                Is.EqualTo(ViewReplicationTopology.MaintenanceRole.Suppress));
        });
    }

    [Test]
    public void Non_stable_generation_is_rejected_only_for_ship_view()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
                    ViewName,
                    new LatticeViewOptions { ReplicationMode = LatticeViewReplicationMode.ShipView },
                    1),
                Throws.InvalidOperationException);
            Assert.That(
                () => ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
                    ViewName,
                    new LatticeViewOptions(),
                    1),
                Throws.Nothing);
            Assert.That(
                () => ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
                    ViewName,
                    new LatticeViewOptions { ReplicationMode = LatticeViewReplicationMode.ShipView },
                    0),
                Throws.Nothing);
        });
    }

    [TestCase(null, "current", new[] { "current" })]
    [TestCase("", "current", new[] { "current" })]
    [TestCase("current", "current", new[] { "current" })]
    [TestCase("retired", "current", new[] { "retired", "current" })]
    public void Cursor_cleanup_includes_every_distinct_physical_tree(
        string? bound,
        string current,
        string[] expected)
    {
        Assert.That(
            ViewReplicationTopology.SourceCursorTreesToUnregister(bound, current),
            Is.EqualTo(expected));
    }

    private static ViewReplicationTopology.MaintenanceRole Resolve(
        LatticeViewOptions options,
        ILatticeReplicationContext context) =>
        ViewReplicationTopology.Resolve(ViewName, SourceTreeId, options, context);

    private static ILatticeReplicationContext Context(
        bool enabled = true,
        string localReplicaId = "site-a",
        params string[] replicatedTrees)
    {
        var trees = replicatedTrees.ToHashSet(StringComparer.Ordinal);
        var context = Substitute.For<ILatticeReplicationContext>();
        context.IsReplicationEnabled.Returns(enabled);
        context.LocalReplicaId.Returns(localReplicaId);
        context.ResolveMergeMode(Arg.Any<string>()).Returns(call =>
            trees.Contains(call.Arg<string>()) ? LatticeMergeMode.LwwRegister : null);
        return context;
    }
}
