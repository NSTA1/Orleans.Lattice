using System.Collections.Concurrent;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Issue #3761 item 3. <see cref="LatticeMetrics.SaturationRefusals"/> attributes
/// every saturation refusal to the seam that raised it, so the ~490 refusals a
/// minute a deployment logged can be read by source instead of reconstructed
/// from logs.
/// </summary>
[TestFixture]
public class LatticeSaturationRefusalMetricsTests
{
    [Test]
    public void SaturationSourceTag_maps_every_source_to_a_distinct_snake_case_arm()
    {
        var arms = Enum.GetValues<LatticeSaturationSource>()
            .Select(source => LatticeMetrics.SaturationSourceTag(source))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(arms.Select(a => a.Key), Is.All.EqualTo(LatticeMetrics.TagSaturationSource));
            Assert.That(arms.Select(a => a.Value), Is.Unique,
                "two sources sharing an arm would make them indistinguishable, which is the defect.");
            Assert.That(arms.Select(a => (string)a.Value!), Is.All.Matches("^[a-z]+(_[a-z]+)*$"));
        });
    }

    [Test]
    public void SaturationSourceTag_names_the_replay_permit_admission_arm()
    {
        Assert.That(
            LatticeMetrics.SaturationSourceTag(LatticeSaturationSource.ReplayPermitAdmission).Value,
            Is.EqualTo("replay_permit_admission"));
    }

    [Test]
    public void SaturationSourceTag_throws_for_an_unmapped_source()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => LatticeMetrics.SaturationSourceTag((LatticeSaturationSource)int.MaxValue));
    }

    [Test]
    [NonParallelizable]
    public void RecordSaturationRefusal_counts_one_refusal_tagged_with_tree_source_and_tenant()
    {
        var treeId = "saturation-refusal-" + Guid.NewGuid().ToString("N");
        var captured = new ConcurrentQueue<Dictionary<string, object?>>();
        using (Listen(captured, tags => Equals(tags.GetValueOrDefault(LatticeMetrics.TagTree), treeId)))
        {
            LatticeMetrics.RecordSaturationRefusal(treeId, LatticeSaturationSource.WalAdmission);
        }

        Assert.That(captured, Has.Count.EqualTo(1));
        var tags = captured.Single();
        Assert.Multiple(() =>
        {
            Assert.That(tags[LatticeMetrics.TagSaturationSource], Is.EqualTo("wal_admission"));
            Assert.That(tags.Keys, Does.Contain(LatticeTenantLabel.TagTenant));
        });
    }

    [Test]
    [NonParallelizable]
    public void RecordSaturationRefusal_records_an_empty_tree_tag_when_no_tree_is_known()
    {
        var captured = new ConcurrentQueue<Dictionary<string, object?>>();
        using (Listen(captured, tags =>
            Equals(tags.GetValueOrDefault(LatticeMetrics.TagSaturationSource), "tx_registry_capacity")
            && Equals(tags.GetValueOrDefault(LatticeMetrics.TagTree), string.Empty)))
        {
            LatticeMetrics.RecordSaturationRefusal(null, LatticeSaturationSource.TxRegistryCapacity);
        }

        Assert.That(captured, Has.Count.GreaterThanOrEqualTo(1));
    }

    private static IDisposable Listen(
        ConcurrentQueue<Dictionary<string, object?>> captured,
        Func<Dictionary<string, object?>, bool> filter)
        => MeterListening.StartForInstrument(
            LatticeMetrics.SaturationRefusals,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var map = new Dictionary<string, object?>();
                foreach (var tag in tags)
                {
                    map[tag.Key] = tag.Value;
                }

                if (value == 1 && filter(map))
                {
                    captured.Enqueue(map);
                }
            }));
}
