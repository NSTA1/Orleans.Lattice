using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end cover for issue #2693 over a real cluster: keys are written
/// through the public <see cref="ILattice"/> surface into real leaves, the
/// storage-usage report is assembled from the real shard roots, and the
/// observable byte gauges are then scraped and required to agree with it.
/// <para>
/// This is the assertion that would have contradicted the retracted #2692
/// diagnosis. On the deployment that produced it,
/// <c>storage_leaf_state_bytes</c> and <c>storage_snapshot_bytes</c> read
/// <c>0</c> for the largest tree in the cluster - one holding 674 MB of
/// retained WAL across 118 leaves - because only the cheap WAL-only publish
/// path had ever run and it seeded those surfaces to zero without measuring
/// them. A reader took the zero for a measurement. The two tests here pin both
/// halves of the fix: an unmeasured surface reports nothing at all, and a
/// measured surface reports the real byte count.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StorageUsageGaugeIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    /// <summary>
    /// Scrapes one observable gauge for one tree. Returns <c>null</c> when the
    /// gauge reported no measurement for that tree at all - the distinction
    /// that separates "never measured" from "measured and zero".
    /// </summary>
    private static long? ReadGauge(string instrument, string tree)
    {
        long? found = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter) && inst.Name == instrument)
                {
                    l.EnableMeasurementEvents(inst);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var t in tags)
            {
                if (t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree)
                {
                    found = value;
                }
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return found;
    }

    /// <summary>
    /// The headline end-to-end regression for issue #2693. Before any deep
    /// publish the byte gauges must report <i>nothing</i> for the tree - not a
    /// zero - and after a real measurement they must report the real,
    /// non-zero, report-agreeing byte count.
    /// </summary>
    [Test]
    public async Task Deep_byte_gauges_report_nothing_until_measured_then_report_the_real_footprint()
    {
        var treeId = $"usage-gauge-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Real state: 64 keys with non-trivial values, written through the
        // public surface into real leaves.
        for (var i = 0; i < 64; i++)
        {
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes(new string('v', 256) + i));
        }

        // Before the deep read: the tree holds real bytes, but nothing has
        // measured them. A pre-fix build published a seeded zero here, which is
        // exactly the false measurement that produced the #2692 diagnosis.
        Assert.Multiple(() =>
        {
            Assert.That(ReadGauge(LatticeMetrics.StorageLeafStateBytesName, treeId), Is.Null,
                "an unmeasured leaf-state surface must report no data, never a zero");
            Assert.That(ReadGauge(LatticeMetrics.StorageSnapshotBytesName, treeId), Is.Null,
                "an unmeasured snapshot surface must report no data, never a zero");
            Assert.That(ReadGauge(LatticeMetrics.StorageTotalBytesName, treeId), Is.Null,
                "a total that omits two of its three terms must not be published at all");
            Assert.That(ReadGauge(LatticeMetrics.StorageUsageDeepPublishedName, treeId), Is.Null.Or.EqualTo(0),
                "the depth gauge must not claim a deep measurement before one has happened");
        });

        var usage = await tree.GetStorageUsageAsync();

        // Ground truth: the report itself, assembled from the real shard roots.
        Assert.That(usage.LeafStateBytes, Is.GreaterThan(0),
            "64 keys of 256-byte values must produce a non-zero leaf-state footprint");

        Assert.Multiple(() =>
        {
            Assert.That(ReadGauge(LatticeMetrics.StorageLeafStateBytesName, treeId),
                Is.EqualTo(usage.LeafStateBytes),
                "the gauge must agree with the report it was published from");
            Assert.That(ReadGauge(LatticeMetrics.StorageSnapshotBytesName, treeId),
                Is.EqualTo(usage.SnapshotBytes));
            Assert.That(ReadGauge(LatticeMetrics.StorageTotalBytesName, treeId),
                Is.EqualTo(usage.TotalBytes));
            Assert.That(ReadGauge(LatticeMetrics.StorageUsageDeepPublishedName, treeId), Is.EqualTo(1),
                "a tree whose deep surfaces were measured must say so positively");
        });
    }

    /// <summary>
    /// The other half of the discrimination: a tree that genuinely holds no
    /// leaf state, measured, must export an explicit <c>0</c> rather than
    /// falling silent. Without this a reader could not distinguish the fix from
    /// simply deleting the gauges.
    /// </summary>
    [Test]
    public async Task Deep_byte_gauges_export_an_explicit_zero_for_a_measured_empty_tree()
    {
        var treeId = $"usage-gauge-empty-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var usage = await tree.GetStorageUsageAsync();
        Assert.That(usage.LeafStateBytes, Is.Zero, "the tree was never written to");

        Assert.Multiple(() =>
        {
            Assert.That(ReadGauge(LatticeMetrics.StorageLeafStateBytesName, treeId), Is.EqualTo(0),
                "a measured empty tree must export a real zero, not silence");
            Assert.That(ReadGauge(LatticeMetrics.StorageUsageDeepPublishedName, treeId), Is.EqualTo(1),
                "an empty tree was still measured, so the depth gauge must report 1");
        });
    }
}
