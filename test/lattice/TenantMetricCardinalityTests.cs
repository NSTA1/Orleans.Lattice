using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Proves the derived <c>tenant</c> dimension is cardinality-neutral: because
/// <c>tree -&gt; tenant</c> is a function, attaching the label to an existing
/// instrument attaches it to series that already exist rather than multiplying
/// them. Runs a fixed, deterministic workload through a real
/// <see cref="Meter"/> and compares the distinct series counts observed with
/// and without the label. Nothing here depends on timing or ordering.
/// </summary>
[TestFixture]
public sealed class TenantMetricCardinalityTests
{
    private static readonly string[] Trees =
    [
        "orders",
        "invoices",
        "view-orders-by-region",
        "t/acme/orders",
        "t/acme/invoices",
        "t/globex/orders",
        LatticeConstants.SystemTreePrefix + "registry",
        LatticeConstants.SystemDataTreePrefix + "tenant-registry",
    ];

    private const int Shards = 4;

    /// <summary>Receives one measurement's tag span. A named delegate because
    /// <see cref="ReadOnlySpan{T}"/> cannot be a generic type argument.</summary>
    private delegate void MeasurementObserver(ReadOnlySpan<KeyValuePair<string, object?>> tags);
    private const int Repeats = 5;

    [Test]
    public void Adding_the_derived_tenant_tag_leaves_the_series_count_unchanged()
    {
        var baseline = CountDistinctSeries(withTenant: false);
        var withTenant = CountDistinctSeries(withTenant: true);

        Assert.That(withTenant, Is.EqualTo(baseline),
            "The derived tenant label must attach to existing series, never split them: " +
            "tree -> tenant is a function, so two measurements that shared a series before " +
            "must still share one after.");
    }

    [Test]
    public void The_fixed_workload_produces_a_non_trivial_series_count()
    {
        // Guards the neutrality assertion above from passing vacuously.
        Assert.That(CountDistinctSeries(withTenant: false), Is.EqualTo(Trees.Length * Shards));
    }

    [Test]
    public void Every_series_carries_exactly_one_tenant_label()
    {
        var tenantValues = new List<string>();

        RunWorkload(withTenant: true, tags =>
        {
            var seen = 0;
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeTenantLabel.TagTenant)
                {
                    seen++;
                    tenantValues.Add((string)tag.Value!);
                }
            }

            Assert.That(seen, Is.EqualTo(1));
        });

        Assert.That(tenantValues, Is.Not.Empty);
        Assert.That(tenantValues.Distinct(StringComparer.Ordinal), Is.EquivalentTo(
            new[] { LatticeTenantLabel.DefaultTenant, "acme", "globex", LatticeTenantLabel.PlatformTenant }));
    }

    [Test]
    public void Each_tree_maps_to_exactly_one_tenant_label()
    {
        // The functional property the neutrality argument rests on, asserted
        // directly rather than inferred from the counts.
        var byTree = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        RunWorkload(withTenant: true, tags =>
        {
            string? tree = null;
            string? tenant = null;
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeMetrics.TagTree) tree = (string)tag.Value!;
                else if (tag.Key == LatticeTenantLabel.TagTenant) tenant = (string)tag.Value!;
            }

            if (tree is not null && tenant is not null)
            {
                if (!byTree.TryGetValue(tree, out var set))
                {
                    byTree[tree] = set = new HashSet<string>(StringComparer.Ordinal);
                }
                set.Add(tenant);
            }
        });

        Assert.That(byTree, Has.Count.EqualTo(Trees.Length));
        Assert.That(byTree.Values.Select(static v => v.Count), Is.All.EqualTo(1));
    }

    private static int CountDistinctSeries(bool withTenant)
    {
        var series = new HashSet<string>(StringComparer.Ordinal);
        RunWorkload(withTenant, tags => series.Add(Canonicalise(tags)));
        return series.Count;
    }

    private static void RunWorkload(bool withTenant, MeasurementObserver onMeasurement)
    {
        // A private meter with a unique name so the fixture never observes (or is
        // perturbed by) the process-wide Orleans.Lattice instruments.
        var meterName = "orleans.lattice.tests.cardinality." + Guid.NewGuid().ToString("N");
        using var meter = new Meter(meterName);
        var counter = meter.CreateCounter<long>("writes");

        var observer = onMeasurement;
        using (var listener = new MeterListener())
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == meterName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, _, tags, _) => observer(tags));
            listener.Start();

            for (var repeat = 0; repeat < Repeats; repeat++)
            {
                foreach (var tree in Trees)
                {
                    for (var shard = 0; shard < Shards; shard++)
                    {
                        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, tree);
                        var shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, shard);
                        if (withTenant)
                        {
                            counter.Add(1, treeTag, shardTag, LatticeTenantLabel.ForTree(tree));
                        }
                        else
                        {
                            counter.Add(1, treeTag, shardTag);
                        }
                    }
                }
            }
        }
    }

    private static string Canonicalise(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        // Every emitted tag participates in the series key, which is what makes
        // the comparison meaningful: if the label were not a function of the tree,
        // one (tree, shard) pair would yield two distinct tenant values and the
        // with-tenant run would report strictly more series than the baseline.
        var parts = new List<string>(tags.Length);
        foreach (var tag in tags)
        {
            parts.Add(tag.Key + "=" + tag.Value);
        }

        parts.Sort(StringComparer.Ordinal);
        return string.Join('|', parts);
    }
}
