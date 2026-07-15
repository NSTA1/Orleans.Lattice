using System.Text;
using NSubstitute;
using Orleans.Lattice;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaComplianceAdmin"/>: the read-only
/// per-tree compliance audit. Drives the admin with a substituted grain factory
/// (returning a fake <see cref="ILattice"/> whose <see cref="ILattice.EntriesAsync"/>
/// yields a fixed value set) and a substituted <see cref="ILatticeSchemaPolicyProvider"/>
/// so the scan's counting, reason breakdown, ungoverned short-circuit, empty-tree
/// behaviour, parameter guards, and cancellation are asserted without a cluster.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaComplianceAdminTests
{
    private const string Tree = "orders";

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static (LatticeSchemaComplianceAdmin Admin, ILattice Grain, ILatticeSchemaPolicyProvider Provider) Create()
    {
        var grain = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Tree).Returns(grain);
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        return (new LatticeSchemaComplianceAdmin(grainFactory, provider), grain, provider);
    }

    private static void SetEntries(ILattice grain, params KeyValuePair<string, byte[]>[] entries) =>
        grain.EntriesAsync(
                Arg.Any<string?>(),
                Arg.Any<string?>(),
                Arg.Any<bool>(),
                Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => ToAsync(entries));

    private static void SetPolicy(ILatticeSchemaPolicyProvider provider, CompiledSchemaPolicy? policy) =>
        provider.GetCompiledPolicyAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(policy));

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ToAsync(
        KeyValuePair<string, byte[]>[] entries)
    {
        foreach (var entry in entries)
        {
            yield return entry;
        }

        await Task.CompletedTask;
    }

    private static KeyValuePair<string, byte[]> Kv(string key, string value) => new(key, Utf8(value));

    // ---- Ungoverned: null policy short-circuits -------------------------

    [Test]
    public async Task ScanCompliance_ungoverned_tree_returns_an_ungoverned_report_without_scanning()
    {
        var (admin, grain, provider) = Create();
        SetPolicy(provider, null);

        var report = await admin.ScanComplianceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.HasPolicy, Is.False);
            Assert.That(report.CompliantCount, Is.Zero);
            Assert.That(report.NonCompliantCount, Is.Zero);
            Assert.That(report.ScannedCount, Is.Zero);
            Assert.That(report.RuleBreakdown, Is.Empty);
        });

        // The tree is never enumerated when there is no policy.
        grain.DidNotReceive().EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>());
    }

    // ---- Empty tree: governed but nothing scanned -----------------------

    [Test]
    public async Task ScanCompliance_empty_tree_reports_zero_counts_but_has_policy()
    {
        var (admin, grain, provider) = Create();
        SetPolicy(provider, CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() })));
        SetEntries(grain);

        var report = await admin.ScanComplianceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.HasPolicy, Is.True);
            Assert.That(report.ScannedCount, Is.Zero);
            Assert.That(report.CompliantCount, Is.Zero);
            Assert.That(report.NonCompliantCount, Is.Zero);
            Assert.That(report.RuleBreakdown, Is.Empty);
        });
    }

    // ---- All compliant --------------------------------------------------

    [Test]
    public async Task ScanCompliance_all_values_compliant_reports_no_breakdown()
    {
        var (admin, grain, provider) = Create();
        SetPolicy(provider, CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() })));
        SetEntries(grain, Kv("k1", "{\"a\":1}"), Kv("k2", "{\"b\":2}"), Kv("k3", "[]"));

        var report = await admin.ScanComplianceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.HasPolicy, Is.True);
            Assert.That(report.CompliantCount, Is.EqualTo(3));
            Assert.That(report.NonCompliantCount, Is.Zero);
            Assert.That(report.ScannedCount, Is.EqualTo(3));
            Assert.That(report.RuleBreakdown, Is.Empty);
        });
    }

    // ---- Mixed with per-reason breakdown --------------------------------

    [Test]
    public async Task ScanCompliance_mixed_values_group_non_compliant_by_failure_reason()
    {
        var (admin, grain, provider) = Create();
        var policy = new LatticeSchemaPolicy(new[]
        {
            LatticeSchemaRule.Json("must be json"),
            LatticeSchemaRule.MaxLength(8, "too long"),
        });
        SetPolicy(provider, CompiledSchemaPolicy.Compile(policy));
        SetEntries(
            grain,
            Kv("ok", "{\"a\":1}"),           // compliant
            Kv("bad1", "not json"),          // fails json
            Kv("bad2", "also not json"),     // fails json
            Kv("long", "{\"aaaaaaaa\":1}")); // valid json but too long

        var report = await admin.ScanComplianceAsync(Tree);

        var breakdown = report.RuleBreakdown.ToDictionary(r => r.Reason, r => r.Count);
        Assert.Multiple(() =>
        {
            Assert.That(report.CompliantCount, Is.EqualTo(1));
            Assert.That(report.NonCompliantCount, Is.EqualTo(3));
            Assert.That(report.ScannedCount, Is.EqualTo(4));
            Assert.That(breakdown["must be json"], Is.EqualTo(2));
            Assert.That(breakdown["too long"], Is.EqualTo(1));
        });
    }

    // ---- Parameter guards -----------------------------------------------

    [Test]
    public void ScanCompliance_null_or_empty_tree_id_throws()
    {
        var (admin, _, _) = Create();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await admin.ScanComplianceAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await admin.ScanComplianceAsync(""), Throws.ArgumentException);
        });
    }

    [Test]
    public void Constructor_null_dependencies_throw()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeSchemaComplianceAdmin(null!, provider), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaComplianceAdmin(grainFactory, null!), Throws.ArgumentNullException);
        });
    }

    // ---- Cancellation ---------------------------------------------------

    [Test]
    public void ScanCompliance_cancelled_token_stops_the_scan()
    {
        var (admin, grain, provider) = Create();
        SetPolicy(provider, CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() })));
        SetEntries(grain, Kv("k1", "{\"a\":1}"), Kv("k2", "{\"b\":2}"));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await admin.ScanComplianceAsync(Tree, cts.Token));
    }
}
