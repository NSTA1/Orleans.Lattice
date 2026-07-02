using System.Globalization;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Lattice;

/// <summary>
/// Unit tests for <see cref="ComplianceFoldProjection"/>. These exercise the
/// projection's <see cref="ILatticeAggregationProjection.Project"/> lowering and
/// its <see cref="ILatticeFoldProjection.Initial"/> / <see cref="ILatticeFoldProjection.Apply"/>
/// fold in isolation (no cluster), including the reverse-arrival case that proves
/// the fold orders members by the business HLC and reproduces
/// <see cref="ComplianceFold.Fold"/>.
/// </summary>
[TestFixture]
public class ComplianceFoldProjectionTests
{
    private static readonly ComplianceFoldProjection Projection = new();

    private static readonly ILatticeSerializer<ComplianceAccumulator> Accumulators =
        JsonLatticeSerializer<ComplianceAccumulator>.Default;

    [Test]
    public void Project_Set_yields_Fold_keyed_by_serial_with_business_Hlc()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-70001");
        var fact = Nc(serial, 42, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var key = KeyFor(fact);
        var mutation = new LatticeMutation
        {
            TreeId = LatticeFactBackend.FactTreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = FactJsonCodec.Encode(fact),
            // Deliberately different from the business HLC so the assertion proves
            // the projection carries fact.Hlc, not the write timestamp.
            Timestamp = new HybridLogicalClock { WallClockTicks = 999, Counter = 7 },
        };

        var contributions = Projection.Project(mutation).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        var contribution = contributions[0];
        Assert.Multiple(() =>
        {
            Assert.That(contribution.Kind, Is.EqualTo(AggregationContributionKind.Contribute));
            Assert.That(contribution.GroupKey, Is.EqualTo(serial.Value));
            Assert.That(contribution.SourceKey, Is.EqualTo(key));
            Assert.That(contribution.Value, Is.EqualTo(mutation.Value));
            Assert.That(contribution.Timestamp, Is.EqualTo(fact.Hlc));
        });
    }

    [Test]
    public void Project_Set_with_null_value_yields_nothing()
    {
        var mutation = new LatticeMutation
        {
            TreeId = LatticeFactBackend.FactTreeId,
            Kind = MutationKind.Set,
            Key = "HPT-BLD-S1-2028-70002/x",
            Value = null,
            Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
        };

        Assert.That(Projection.Project(mutation), Is.Empty);
    }

    [Test]
    public void Project_Delete_yields_Retract_for_the_source_key()
    {
        var timestamp = new HybridLogicalClock { WallClockTicks = 123, Counter = 4 };
        var mutation = new LatticeMutation
        {
            TreeId = LatticeFactBackend.FactTreeId,
            Kind = MutationKind.Delete,
            Key = "HPT-BLD-S1-2028-70003/deadbeef",
            Timestamp = timestamp,
            IsTombstone = true,
        };

        var contributions = Projection.Project(mutation).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.Retract));
            Assert.That(contributions[0].SourceKey, Is.EqualTo(mutation.Key));
            Assert.That(contributions[0].Timestamp, Is.EqualTo(timestamp));
        });
    }

    [Test]
    public void Initial_seeds_a_Nominal_unarmed_accumulator()
    {
        var seed = Accumulators.Deserialize(Projection.Initial());

        Assert.Multiple(() =>
        {
            Assert.That(seed.State, Is.EqualTo(ComplianceState.Nominal));
            Assert.That(seed.RetestArmed, Is.False);
            Assert.That(seed.LatestStage, Is.Null);
            Assert.That(seed.FactCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void Fold_records_newest_stage_and_counts_every_fact()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-70010");
        // Two steps in ascending business HLC; the newest (Machining@20) sets the
        // latest stage, and every folded fact bumps the count.
        var early = Step(serial, 10, ProcessStage.Forge, ProcessSite.OhioForge);
        var late = Step(serial, 20, ProcessStage.Machining, ProcessSite.StuttgartMachining);

        var folded = FoldThroughProjection(new Fact[] { early, late });

        Assert.Multiple(() =>
        {
            Assert.That(folded.LatestStage, Is.EqualTo(ProcessStage.Machining));
            Assert.That(folded.FactCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void Fold_latest_stage_follows_business_Hlc_not_arrival_order()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-70011");
        var early = Step(serial, 10, ProcessStage.Forge, ProcessSite.OhioForge);
        var late = Step(serial, 20, ProcessStage.Machining, ProcessSite.StuttgartMachining);

        // Reversed arrival: the fold still orders by business HLC, so the newest
        // fact - and thus the latest stage - is Machining@20.
        var folded = FoldThroughProjection(new Fact[] { late, early });

        Assert.That(folded.LatestStage, Is.EqualTo(ProcessStage.Machining));
    }

    [Test]
    public void Refold_after_retraction_recomputes_stage_and_count()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-70012");
        var early = Step(serial, 10, ProcessStage.Forge, ProcessSite.OhioForge);
        var late = Step(serial, 20, ProcessStage.Machining, ProcessSite.StuttgartMachining);

        // The maintainer re-folds all surviving members from Initial() on any
        // retraction, so counting per-Apply stays correct: dropping the newest
        // member re-derives FactCount = 1 and LatestStage = the surviving fact.
        var before = FoldThroughProjection(new Fact[] { early, late });
        var afterRetraction = FoldThroughProjection(new Fact[] { early });

        Assert.Multiple(() =>
        {
            Assert.That(before.FactCount, Is.EqualTo(2));
            Assert.That(before.LatestStage, Is.EqualTo(ProcessStage.Machining));
            Assert.That(afterRetraction.FactCount, Is.EqualTo(1));
            Assert.That(afterRetraction.LatestStage, Is.EqualTo(ProcessStage.Forge));
        });
    }

    [Test]
    public void Fold_of_reversed_arrival_reproduces_ComplianceFold()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-70004");
        // NC-Minor at HLC 10, then MRB-UseAsIs at HLC 20 => Nominal under the
        // business-HLC-ordered fold, even though they arrive reversed.
        var first = Nc(serial, 10, "NC-A", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var second = Mrb(serial, 20, "NC-A", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb);

        var folded = FoldThroughProjection(new Fact[] { second, first });
        var reference = ComplianceFold.Fold(new Fact[] { second, first });

        Assert.Multiple(() =>
        {
            Assert.That(reference, Is.EqualTo(ComplianceState.Nominal));
            Assert.That(folded.State, Is.EqualTo(reference));
        });
    }

    [Test]
    public void Fold_matches_ComplianceFold_for_a_shuffled_rework_sequence()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-70005");
        var nc = Nc(serial, 1, "NC-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab);
        var rework = new ReworkCompleted
        {
            Serial = serial, FactId = Guid.NewGuid(), Hlc = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 },
            Site = ProcessSite.StuttgartMachining, Operator = Op,
            Description = "rework op", ReworkOperation = "re-blend", RetestPassed = true,
        };
        var mrb = Mrb(serial, 4, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb);

        // Shuffled arrival order; the fold must still land on ComplianceFold's answer.
        var arrival = new Fact[] { mrb, nc, rework };
        var folded = FoldThroughProjection(arrival);
        var reference = ComplianceFold.Fold(arrival);

        Assert.That(folded.State, Is.EqualTo(reference));
    }

    /// <summary>
    /// Drives <paramref name="arrival"/> through the projection exactly as the
    /// view maintainer does: lower each Set to a Fold contribution, sort the
    /// group's members by (contribution HLC, source key ordinal), then fold from
    /// <see cref="ILatticeFoldProjection.Initial"/> through
    /// <see cref="ILatticeFoldProjection.Apply"/>.
    /// </summary>
    private static ComplianceAccumulator FoldThroughProjection(IEnumerable<Fact> arrival)
    {
        var members = new List<(string SourceKey, byte[] Value, HybridLogicalClock Timestamp)>();
        foreach (var fact in arrival)
        {
            var mutation = new LatticeMutation
            {
                TreeId = LatticeFactBackend.FactTreeId,
                Kind = MutationKind.Set,
                Key = KeyFor(fact),
                Value = FactJsonCodec.Encode(fact),
                Timestamp = new HybridLogicalClock { WallClockTicks = 500, Counter = 0 },
            };

            foreach (var contribution in Projection.Project(mutation))
            {
                members.Add((contribution.SourceKey, contribution.Value!, contribution.Timestamp));
            }
        }

        members.Sort((a, b) =>
        {
            var cmp = a.Timestamp.CompareTo(b.Timestamp);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.SourceKey, b.SourceKey);
        });

        var accumulator = Projection.Initial();
        foreach (var (sourceKey, value, timestamp) in members)
        {
            accumulator = Projection.Apply(accumulator, sourceKey, value, timestamp);
        }

        return Accumulators.Deserialize(accumulator);
    }

    private static string KeyFor(Fact fact) =>
        string.Create(CultureInfo.InvariantCulture,
            $"{fact.Serial.Value}/{fact.Hlc.WallClockTicks:D20}/{fact.Hlc.Counter:D10}/{fact.FactId:N}");
}
