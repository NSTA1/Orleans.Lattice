using System.Text;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// The progress a compliance scan reports when it runs as a tracked operation
/// (#4126): it counts the tree's live entries first, then reports the entries
/// scanned against that count, never reports a total below the entries it has
/// scanned, and reports nothing - and counts nothing - outside an operation.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaComplianceAdminProgressTests
{
    private const string Tree = "orders";

    private sealed class RecordingProgress : ILatticeOperationProgress
    {
        public List<(string Phase, long Completed, long? Total, string? Unit)> Reports { get; } = [];

        public ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            Reports.Add((phase, completedUnits, totalUnits, unitName));
            return ValueTask.CompletedTask;
        }
    }

    private static (LatticeSchemaComplianceAdmin Admin, ILattice Grain, ILatticeSchemaPolicyProvider Provider) Create()
    {
        var grain = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Tree).Returns(grain);
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(
                CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }))));
        return (new LatticeSchemaComplianceAdmin(grainFactory, provider), grain, provider);
    }

    private static void SetEntries(ILattice grain, int count) =>
        grain.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => Entries(count));

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new KeyValuePair<string, byte[]>($"k{i:D5}", Encoding.UTF8.GetBytes(i % 2 == 0 ? "{}" : "x"));
        }

        await Task.CompletedTask;
    }

    [Test]
    public async Task A_tracked_scan_counts_first_then_reports_entries_scanned_against_the_count()
    {
        var (admin, grain, _) = Create();
        SetEntries(grain, 600);
        grain.CountAsync(Arg.Any<CancellationToken>()).Returns(600);
        var progress = new RecordingProgress();

        LatticeSchemaComplianceReport report;
        using (LatticeOperationProgress.Enter(progress))
        {
            report = await admin.ScanComplianceAsync(Tree);
        }

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(600));
            Assert.That(progress.Reports[0].Phase, Is.EqualTo(SchemaComplianceScanOperation.CountingPhase));
            Assert.That(progress.Reports[1], Is.EqualTo((SchemaComplianceScanOperation.ScanningPhase, 0L, (long?)600, SchemaComplianceScanOperation.EntriesUnit)));
            Assert.That(progress.Reports[^1], Is.EqualTo((SchemaComplianceScanOperation.ScanningPhase, 600L, (long?)600, SchemaComplianceScanOperation.EntriesUnit)));
            Assert.That(progress.Reports.Count, Is.GreaterThan(3), "Intermediate progress is reported while the scan runs.");
            Assert.That(progress.Reports.Skip(1).Select(r => r.Completed), Is.Ordered, "Scanned entries never go backwards.");
        });
    }

    [Test]
    public async Task A_count_that_falls_behind_the_scan_turns_the_total_unknown_rather_than_below_the_scanned_entries()
    {
        var (admin, grain, _) = Create();
        SetEntries(grain, 700);
        grain.CountAsync(Arg.Any<CancellationToken>()).Returns(300);
        var progress = new RecordingProgress();

        using (LatticeOperationProgress.Enter(progress))
        {
            await admin.ScanComplianceAsync(Tree);
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                progress.Reports.Where(r => r.Total is { } total && r.Completed > total),
                Is.Empty,
                "A total is never reported below the entries already scanned.");
            Assert.That(progress.Reports[^1].Completed, Is.EqualTo(700));
            Assert.That(progress.Reports[^1].Total, Is.Null);
        });
    }

    [Test]
    public async Task An_untracked_scan_reports_nothing_and_does_not_count_the_tree()
    {
        var (admin, grain, _) = Create();
        SetEntries(grain, 3);

        var report = await admin.ScanComplianceAsync(Tree);

        Assert.That(report.ScannedCount, Is.EqualTo(3));
        await grain.DidNotReceive().CountAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_tracked_scan_of_an_ungoverned_tree_neither_counts_nor_scans()
    {
        var (admin, grain, provider) = Create();
        provider.GetCompiledPolicyAsync(Tree, Arg.Any<CancellationToken>()).Returns(new ValueTask<CompiledSchemaPolicy?>((CompiledSchemaPolicy?)null));
        var progress = new RecordingProgress();

        using (LatticeOperationProgress.Enter(progress))
        {
            await admin.ScanComplianceAsync(Tree);
        }

        Assert.That(progress.Reports, Is.Empty);
        await grain.DidNotReceive().CountAsync(Arg.Any<CancellationToken>());
    }
}
