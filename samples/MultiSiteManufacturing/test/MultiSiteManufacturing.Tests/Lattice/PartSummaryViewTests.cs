using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;
using NUnit.Framework;

namespace MultiSiteManufacturing.Tests.Lattice;

/// <summary>
/// Round-trip coverage for the materialised per-part dashboard summary view
/// (<see cref="PartSummaryView"/>) that backs the dashboard snapshot after the
/// scan-storm fix (issue #1038): upsert / read-all fidelity, one-row-per-part
/// overwrite, and the empty-tree cold-start case the snapshot bootstraps from.
/// </summary>
[TestFixture]
public sealed class PartSummaryViewTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public Task SetUp() => (_fixture = new FederationTestClusterFixture()).InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private static PartSummaryUpdate Summary(
        string serial,
        ProcessStage? stage,
        ComplianceState baseline,
        ComplianceState lattice,
        int factCount) =>
        new()
        {
            Serial = new PartSerialNumber(serial),
            Family = "HPT-BLD",
            LatestStage = stage,
            BaselineState = baseline,
            LatticeState = lattice,
            FactCount = factCount,
        };

    [Test]
    public async Task ReadAll_returns_empty_for_a_fresh_view()
    {
        var view = _fixture.NewPartSummaryView();
        var rows = await view.ReadAllAsync();
        Assert.That(rows, Is.Empty);
    }

    [Test]
    public async Task Upsert_then_ReadAll_round_trips_every_field()
    {
        var view = _fixture.NewPartSummaryView();
        var summary = Summary("HPT-BLD-S1-2028-90001", ProcessStage.NDT, ComplianceState.Nominal, ComplianceState.FlaggedForReview, 7);

        await view.UpsertAsync(summary);
        var rows = await view.ReadAllAsync();

        Assert.That(rows, Has.Count.EqualTo(1));
        var row = rows[0];
        Assert.Multiple(() =>
        {
            Assert.That(row.Serial, Is.EqualTo(summary.Serial));
            Assert.That(row.Family, Is.EqualTo(summary.Family));
            Assert.That(row.LatestStage, Is.EqualTo(summary.LatestStage));
            Assert.That(row.BaselineState, Is.EqualTo(summary.BaselineState));
            Assert.That(row.LatticeState, Is.EqualTo(summary.LatticeState));
            Assert.That(row.FactCount, Is.EqualTo(summary.FactCount));
            Assert.That(row.Diverges, Is.True);
        });
    }

    [Test]
    public async Task Upsert_round_trips_a_null_latest_stage()
    {
        var view = _fixture.NewPartSummaryView();
        var summary = Summary("HPT-BLD-S1-2028-90002", stage: null, ComplianceState.Nominal, ComplianceState.Nominal, 0);

        await view.UpsertAsync(summary);
        var rows = await view.ReadAllAsync();

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].LatestStage, Is.Null);
    }

    [Test]
    public async Task Repeated_upsert_for_one_serial_keeps_a_single_row()
    {
        var view = _fixture.NewPartSummaryView();
        const string serial = "HPT-BLD-S1-2028-90003";

        await view.UpsertAsync(Summary(serial, ProcessStage.Forge, ComplianceState.Nominal, ComplianceState.Nominal, 1));
        await view.UpsertAsync(Summary(serial, ProcessStage.FAI, ComplianceState.Nominal, ComplianceState.Scrap, 9));

        var rows = await view.ReadAllAsync();
        Assert.That(rows, Has.Count.EqualTo(1), "a part must occupy exactly one row regardless of upsert count");
        Assert.Multiple(() =>
        {
            Assert.That(rows[0].LatestStage, Is.EqualTo(ProcessStage.FAI), "the latest upsert wins");
            Assert.That(rows[0].LatticeState, Is.EqualTo(ComplianceState.Scrap));
            Assert.That(rows[0].FactCount, Is.EqualTo(9));
        });
    }

    [Test]
    public async Task ReadAll_returns_one_row_per_distinct_part()
    {
        var view = _fixture.NewPartSummaryView();
        await view.UpsertAsync(Summary("HPT-BLD-S1-2028-90010", ProcessStage.Forge, ComplianceState.Nominal, ComplianceState.Nominal, 1));
        await view.UpsertAsync(Summary("HPT-BLD-S1-2028-90011", ProcessStage.MRB, ComplianceState.Nominal, ComplianceState.Rework, 4));
        await view.UpsertAsync(Summary("HPT-BLD-S1-2028-90012", ProcessStage.NDT, ComplianceState.Nominal, ComplianceState.Nominal, 2));

        var rows = await view.ReadAllAsync();
        Assert.That(rows.Select(r => r.Serial.Value), Is.EquivalentTo(new[]
        {
            "HPT-BLD-S1-2028-90010",
            "HPT-BLD-S1-2028-90011",
            "HPT-BLD-S1-2028-90012",
        }));
    }
}
