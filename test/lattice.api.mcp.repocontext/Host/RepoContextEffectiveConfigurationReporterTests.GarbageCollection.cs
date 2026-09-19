using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Issue #2596, at the reporter: the collector configuration has to reach the report an
/// operator actually reads, and the hazardous combination has to arrive at warning level.
/// </summary>
/// <remarks>
/// <see cref="RepoContextGarbageCollectionTests"/> covers the rules in isolation. This
/// fixture covers the wiring, which is the half that failed before: the rule was never
/// wrong, it was in a channel nobody read. A correct rule that never reaches the log is
/// indistinguishable from no rule at all.
/// </remarks>
public sealed partial class RepoContextEffectiveConfigurationReporterTests
{
    private const long TwelveGiBLimit = 12L * 1024 * 1024 * 1024;

    private static RepoContextGarbageCollectionFacts HazardousFacts { get; }
        = new(IsServerGc: false, ResolvedHeapCount: 1, TwelveGiBLimit, TimeSpan.FromSeconds(252.2));

    private static RepoContextGarbageCollectionFacts RemediedFacts { get; }
        = new(IsServerGc: true, ResolvedHeapCount: 6, TwelveGiBLimit, TimeSpan.FromSeconds(1.4));

    private static CapturingLogger ReportUnder(
        IConfiguration configuration,
        RepoContextGarbageCollectionFacts facts)
    {
        var logger = new CapturingLogger();
        var reporter = new RepoContextEffectiveConfigurationReporter(
            RepoContextHostConfiguration.FromConfiguration(configuration),
            configuration,
            logger)
        {
            GarbageCollection = facts,
        };

        reporter.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return logger;
    }

    [Test]
    public void The_report_states_which_collector_this_process_runs()
    {
        var logger = ReportUnder(Configuration(), HazardousFacts);

        Assert.Multiple(() =>
        {
            Assert.That(
                logger.Messages,
                Has.Exactly(1).Contains(
                    RepoContextGarbageCollection.RuntimeModeKey + " = "
                    + RepoContextGarbageCollection.WorkstationMode),
                "an operator asking what a deployment runs consults this report. Until now "
                + "the collector flavour was reachable only from an Orleans startup note "
                + "phrased as an aside, which is where it sat unread through two failed "
                + "gate runs");
            Assert.That(
                logger.Messages,
                Has.Exactly(1).Contains(RepoContextGarbageCollection.RuntimeMemoryLimitKey));
            Assert.That(
                logger.Messages,
                Has.Exactly(1).Contains(RepoContextGarbageCollection.RuntimeHeapCountKey + " = 1"));
        });
    }

    [Test]
    public void The_hazardous_combination_is_reported_at_warning_level()
    {
        var logger = ReportUnder(Configuration(), HazardousFacts);

        Assert.Multiple(() =>
        {
            Assert.That(
                logger.Warnings,
                Has.Exactly(1).Contains(RepoContextGarbageCollection.HazardMarker),
                "level is the whole point. The runtime's own note was true, present, and "
                + "at information level among hundreds of other information lines");
            Assert.That(
                logger.Warnings,
                Has.Exactly(1).Contains(RepoContextGarbageCollection.ServerGcKey),
                "the warning has to carry the variable that changes the outcome, or it "
                + "leaves its reader exactly where the unread note did");
        });
    }

    [Test]
    public void A_remedied_process_produces_no_collector_warning()
    {
        var logger = ReportUnder(
            Configuration(
                (RepoContextGarbageCollection.ServerGcKey, "1"),
                (RepoContextGarbageCollection.HeapCountKey, "6")),
            RemediedFacts);

        Assert.Multiple(() =>
        {
            Assert.That(
                logger.Warnings.Where(
                    w => w.Contains(RepoContextGarbageCollection.HazardMarker, StringComparison.Ordinal)),
                Is.Empty,
                "the negative control. A hazard line that appeared unconditionally would "
                + "satisfy every positive assertion here while teaching its readers to skip "
                + "it, which is precisely how the signal that already existed stopped "
                + "being read");
            Assert.That(
                logger.Messages,
                Has.Exactly(1).Contains(
                    RepoContextGarbageCollection.RuntimeModeKey + " = "
                    + RepoContextGarbageCollection.ServerMode),
                "the facts are still reported when they are healthy; only the warning is "
                + "conditional");
        });
    }

    /// <summary>
    /// The #2593 trap: a value nothing declared must not print in the shape of one somebody
    /// did, and this report has an existing guard that would catch it - so this asserts the
    /// guard actually reaches the new lines rather than passing over them.
    /// </summary>
    [Test]
    public void A_declared_collector_setting_reads_differently_from_an_undeclared_one()
    {
        var declared = ReportUnder(
            Configuration((RepoContextGarbageCollection.ServerGcKey, "1")),
            RemediedFacts).Messages;
        var undeclared = ReportUnder(Configuration(), RemediedFacts).Messages;

        var declaredLine = declared.Single(
            m => m.Contains(RepoContextGarbageCollection.ServerGcKey, StringComparison.Ordinal));
        var undeclaredLine = undeclared.Single(
            m => m.Contains(RepoContextGarbageCollection.ServerGcKey, StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                declaredLine,
                Does.Contain(RepoContextEffectiveConfiguration.DeclaredMarker));
            Assert.That(
                undeclaredLine,
                Does.Contain(RepoContextEffectiveConfiguration.DefaultedMarker));
            Assert.That(
                undeclaredLine,
                Does.Contain(RepoContextEffectiveConfiguration.UnsetMarker),
                "the process resolves Server GC in both cases here, so without the "
                + "declaration marker and the <unset> value the two runs would print the "
                + "same line for opposite configurations - the directly self-contradicting "
                + "pair issue #2586 was filed about");
        });
    }

    /// <summary>
    /// The runtime facts are facts, not settings, and the report has a third marker for
    /// exactly that so a reader cannot mistake an observation for a declaration.
    /// </summary>
    [Test]
    public void The_resolved_collector_facts_are_marked_as_runtime_facts()
    {
        var logger = ReportUnder(
            Configuration((RepoContextGarbageCollection.ServerGcKey, "1")),
            RemediedFacts);

        var mode = logger.Messages.Single(
            m => m.Contains(RepoContextGarbageCollection.RuntimeModeKey, StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(mode, Does.Contain(RepoContextEffectiveConfiguration.RuntimeMarker));
            Assert.That(
                mode,
                Does.Not.Contain(RepoContextEffectiveConfiguration.DeclaredMarker),
                "nobody declares GC.Mode; it is what the collector resolved. Marking it "
                + "declared would invite an operator to go looking for a variable of that "
                + "name");
        });
    }

    [Test]
    public void The_scope_statement_admits_the_collector_variables_it_now_covers()
        => Assert.That(
            RepoContextEffectiveConfiguration.ScopeStatement,
            Does.Contain("DOTNET_"),
            "the scope statement is what makes absence readable as out-of-scope rather "
            + "than as unset. Extending the report without extending its stated boundary "
            + "would leave the new lines outside the claim the report makes about itself");

    /// <summary>
    /// A value withheld by the allowlist would defeat the entire deliverable, silently.
    /// </summary>
    [Test]
    public void No_collector_line_is_withheld_by_the_redaction_allowlist()
    {
        var logger = ReportUnder(
            Configuration((RepoContextGarbageCollection.ServerGcKey, "1")),
            RemediedFacts);

        var collectorLines = logger.Messages.Where(
            m => m.Contains(RepoContextGarbageCollection.RuntimeModeKey, StringComparison.Ordinal)
                || m.Contains(RepoContextGarbageCollection.RuntimeHeapCountKey, StringComparison.Ordinal)
                || m.Contains(RepoContextGarbageCollection.ServerGcKey, StringComparison.Ordinal)
                || m.Contains(RepoContextGarbageCollection.HeapCountKey, StringComparison.Ordinal))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(collectorLines, Is.Not.Empty);
            Assert.That(
                collectorLines.Where(
                    l => l.Contains(
                        RepoContextEffectiveConfiguration.UnclassifiedMarker,
                        StringComparison.Ordinal)),
                Is.Empty,
                "the allowlist redacts by default, which is the right direction for "
                + "credentials and fatal here: a collector mode printed as "
                + "'<redacted: unclassified>' would leave the report looking complete "
                + "while saying nothing");
        });
    }
}
