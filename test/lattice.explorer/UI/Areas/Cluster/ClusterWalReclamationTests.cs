using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// The WAL reclamation section (#4195) on <c>/cluster/wal</c> and on a tree's storage
/// tab: it names the leaf holding the WAL floor, its state and its pin offset, and
/// flags a wedge only for a usable pin above a never-persisted checkpoint. The benign
/// <c>-1</c> sentinel in the same state reads as waiting for a checkpoint, and a
/// checkpointed holder reads as not blocked - so a tree that reclaims nothing because
/// it is wedged no longer reads like one with nothing to reclaim.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterWalReclamationTests : ClusterTestContext
{
    private const string TreeId = "orders";
    private const string Leaf = "bplusleaf/7b16d935";
    private const string Consumer = "_lattice_materialiser_t/acme/orders-physical_bplusleaf/7b16d935";

    private readonly ILatticeWalReclamation _reclamation = Substitute.For<ILatticeWalReclamation>();

    [SetUp]
    public void Arrange()
    {
        Services.AddKeyedSingleton(ShellFacades.Key, _reclamation);
        Admin.AuditWalPlacementAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeWalPlacementAudit
        {
            TreeId = TreeId, Version = 5, PartitionCount = 2, AllResolvableOnThisSilo = true,
            Partitions = [new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "blob-a", ResolvableOnThisSilo = true }],
            KnownProviderKeys = ["blob-a"],
        });
        Admin.GetTreeStatsAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeStatsReport { TreeId = TreeId, WalRetainedBytes = 512, TotalBytes = 512 });
        Admin.GetWalPlacementAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeWalPlacement { TreeId = TreeId, Version = 4, Partitions = [] });
    }

    private void Answer(long pinOffset, TreeWalFloorHolderState state, long? checkpoint) =>
        _reclamation.GetWalReclamationAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeWalReclamationReport
        {
            TreeId = TreeId,
            PinStoreReadable = true,
            PinCount = 3,
            PinsWithoutOffset = pinOffset < 0 ? 3 : 1,
            FloorHolder = new TreeWalFloorHolder
            {
                ConsumerId = Consumer,
                LeafId = Leaf,
                Partition = 1,
                PinOffset = pinOffset,
                PersistedCheckpoint = checkpoint,
                State = state,
            },
        });

    private string Verdict(IRenderedComponent<ClusterPage> cut) =>
        cut.Find("[data-lt-cluster='wal-reclamation-verdict']").TextContent;

    [Test]
    public void The_wal_page_flags_a_wedged_tree_and_names_the_leaf_its_state_and_pin_offset()
    {
        Answer(42, TreeWalFloorHolderState.NeverCheckpointed, -1);

        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(Verdict(cut), Does.Contain("Blocked")
                .And.Contain($"Reclamation is blocked: leaf {Leaf} holds the floor with a durable pin at offset 42 on partition 1")
                .And.Contain("does not clear on its own"));
            Assert.That(cut.Find("[data-lt-cluster='wal-reclamation-verdict'] [role='alert']"), Is.Not.Null);
            var reclamation = cut.Find("[data-lt-cluster='wal-reclamation']").TextContent;
            Assert.That(reclamation, Does.Contain("Floor holder").And.Contain(Leaf)
                .And.Contain("Pin offset").And.Contain("42")
                .And.Contain("Persisted checkpoint").And.Contain("None (-1)")
                .And.Contain("Never checkpointed"));
        });
        Assert.That(cut.Markup, Does.Not.Contain(Consumer), "the consumer id carries the physical tree id, which is never shown");
    }

    [Test]
    public void The_wal_page_reads_the_benign_minus_one_sentinel_as_waiting_for_a_checkpoint()
    {
        Answer(-1, TreeWalFloorHolderState.NeverCheckpointed, -1);

        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(Verdict(cut), Does.Contain("Waiting for a checkpoint")
                .And.Contain("reports no offset (-1)")
                .And.Contain("clears once the leaf checkpoints")
                .And.Not.Contain("Blocked"));
            Assert.That(cut.FindAll("[data-lt-cluster='wal-reclamation-verdict'] [role='alert']"), Is.Empty);
        });
    }

    [Test]
    public void The_wal_page_reads_a_checkpointed_holder_as_not_blocked()
    {
        Answer(42, TreeWalFloorHolderState.CheckpointedCoverageUnknown, 42);

        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(Verdict(cut), Does.Contain("Not blocked")
                .And.Contain($"Leaf {Leaf} holds the floor at offset 42 on partition 1")
                .And.Not.Contain("Blocked:"));
            Assert.That(cut.FindAll("[data-lt-cluster='wal-reclamation-verdict'] [role='alert']"), Is.Empty);
        });
    }

    [Test]
    public void The_wal_page_says_when_no_pin_holds_the_floor()
    {
        _reclamation.GetWalReclamationAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeWalReclamationReport { TreeId = TreeId, PinStoreReadable = true });

        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() => Assert.That(Verdict(cut), Does.Contain("Not blocked").And.Contain("No leaf holds a WAL pin")));
    }

    [Test]
    public void An_unreadable_pin_store_is_not_established_rather_than_healthy()
    {
        _reclamation.GetWalReclamationAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeWalReclamationReport { TreeId = TreeId, PinStoreReadable = false });

        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() => Assert.That(Verdict(cut), Does.Contain("Not established").And.Not.Contain("Not blocked")));
    }

    [Test]
    public void A_cluster_that_does_not_serve_the_read_gets_a_quiet_note()
    {
        _reclamation.GetWalReclamationAsync(TreeId, Arg.Any<CancellationToken>())
            .ThrowsAsync(new NotSupportedException("unimplemented"));

        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-cluster='wal-reclamation']").TextContent,
            Does.Contain("does not report which pin holds the WAL floor")));
        Assert.That(cut.FindAll("[data-lt-cluster='wal-reclamation'] [role='alert']"), Is.Empty);
    }

    [Test]
    public void The_storage_tab_flags_a_wedged_tree_beside_its_retained_wal()
    {
        Answer(42, TreeWalFloorHolderState.NeverCheckpointed, -1);

        var cut = Render<ClusterTreeStorage>(parameters => parameters
            .Add(tab => tab.TreeId, TreeId)
            .Add(tab => tab.Capabilities, new LatticeTreeAdminCapabilities
            {
                TreeId = TreeId,
                Schema = new LatticeSchemaCapabilities { TreeId = TreeId },
                CanViewDiagnostics = true,
            }));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Retained WAL"));
            Assert.That(cut.Find("[data-lt-cluster='wal-reclamation-verdict']").TextContent, Does.Contain("Blocked")
                .And.Contain($"leaf {Leaf} holds the floor with a durable pin at offset 42"));
        });
    }
}
