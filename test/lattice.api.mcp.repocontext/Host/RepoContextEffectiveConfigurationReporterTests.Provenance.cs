using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers issue #2586: the effective-configuration report must state, on the same line as
/// every value, whether that value was declared or merely defaulted.
/// </summary>
/// <remarks>
/// <para>
/// <b>What actually happened.</b> A running container logged a warning naming
/// <c>LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD</c> as unset, predicting the exact failure
/// that followed, and then logged
/// <c>Repository-context effective configuration: LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD = 120s</c>
/// about the same variable seconds later. The variable was genuinely absent from the
/// container environment. The second line won, and three agents inspecting that container
/// across two gate runs concluded the grant was configured.
/// </para>
/// <para>
/// <b>Why the negative control is load-bearing.</b> A fixture that only asserted the
/// marker appears cannot separate "the marker is emitted correctly" from "the marker is
/// emitted unconditionally", and an unconditional marker is its own defect: it trains
/// readers to ignore it, which is how the unmarked line failed in the first place. So
/// every positive assertion below is paired with a declared case asserting the defaulted
/// marker is <b>absent</b>, and the two end-to-end cases are run at the <i>same resolved
/// value</i> so that nothing but the declaration can be producing the difference.
/// </para>
/// </remarks>
public sealed partial class RepoContextEffectiveConfigurationReporterTests
{
    /// <summary>
    /// The provenance markers the report emits, which is the closed set every line must
    /// draw from.
    /// </summary>
    private static IReadOnlyList<string> ProvenanceMarkers { get; } =
    [
        RepoContextEffectiveConfiguration.DeclaredMarker,
        RepoContextEffectiveConfiguration.DefaultedMarker,
        RepoContextEffectiveConfiguration.RuntimeMarker,
    ];

    private static IReadOnlyList<string> ReportedSettingLines(IConfiguration configuration)
    {
        var logger = new CapturingLogger();
        var reporter = new RepoContextEffectiveConfigurationReporter(
            RepoContextHostConfiguration.FromConfiguration(configuration), configuration, logger);

        reporter.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        // The header and the scope statement are prose about the report, not entries in
        // it. Everything else is a rendered "NAME = value" line and is in scope for the
        // totality guard below.
        return logger.Messages
            .Where(m => m.Contains(" = ", StringComparison.Ordinal))
            .ToList();
    }

    /// <summary>
    /// The two markers must be distinguishable by substring, or every
    /// <c>Does.Not.Contain</c> assertion in this fixture is vacuous.
    /// </summary>
    [Test]
    public void The_declared_and_defaulted_markers_are_not_substrings_of_each_other()
        => Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEffectiveConfiguration.DeclaredMarker,
                Does.Not.Contain(RepoContextEffectiveConfiguration.DefaultedMarker),
                "the negative controls in this fixture assert the defaulted marker is "
                + "absent from a declared line; if one marker contained the other those "
                + "assertions would be checking nothing");
            Assert.That(
                ProvenanceMarkers,
                Is.Unique,
                "a provenance that rendered the same text as another would collapse the "
                + "distinction the whole issue is about");
        });

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void An_absent_or_blank_value_is_classified_as_defaulted(string? raw)
        => Assert.That(
            RepoContextEffectiveConfiguration.ProvenanceOf(raw),
            Is.EqualTo(RepoContextSettingProvenance.Defaulted),
            "whitespace counts as absent because RepoContextShutdownBudget.Resolve treats "
            + "it that way; a provenance marker derived from a different emptiness rule "
            + "than the resolution it describes would be this same defect one level down");

    [Test]
    public void A_supplied_value_is_classified_as_declared()
        => Assert.That(
            RepoContextEffectiveConfiguration.ProvenanceOf("120s"),
            Is.EqualTo(RepoContextSettingProvenance.Declared),
            "the negative half: if everything classified as defaulted the marker would be "
            + "unconditional, which trains readers to ignore it");

    [Test]
    public void A_defaulted_value_says_so_beside_the_value()
        => Assert.That(
            RepoContextEffectiveConfiguration.DescribeSetting(
                RepoContextShutdownBudget.StopGracePeriodKey,
                "120s",
                "120s",
                RepoContextSettingProvenance.Defaulted),
            Is.EqualTo(
                "LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD = 120s (DEFAULTED, not declared)"),
            "this is the exact line the container printed, with the qualification it was "
            + "missing, and the qualification is on the same line because a reader who "
            + "greps for the variable name has to receive it in the same result");

    /// <summary>
    /// The negative control for the assertion above: identical value, identical default,
    /// and the defaulted marker must not appear.
    /// </summary>
    [Test]
    public void A_declared_value_carries_no_defaulted_marker()
    {
        var line = RepoContextEffectiveConfiguration.DescribeSetting(
            RepoContextShutdownBudget.StopGracePeriodKey,
            "120s",
            "120s",
            RepoContextSettingProvenance.Declared);

        Assert.Multiple(() =>
        {
            Assert.That(
                line,
                Does.Not.Contain(RepoContextEffectiveConfiguration.DefaultedMarker),
                "an operator who declared this value must not be told nobody did; a marker "
                + "that appeared on every line would be noise readers learn to skip, which "
                + "is the failure mode the unmarked line already demonstrated");
            Assert.That(
                line,
                Is.EqualTo("LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD = 120s (DECLARED)"),
                "the value and default are identical to the defaulted case above, so the "
                + "declaration is the only thing that can be producing the difference");
        });
    }

    /// <summary>
    /// Provenance and <c>[OVERRIDDEN]</c> answer different questions and are not
    /// interchangeable.
    /// </summary>
    [Test]
    public void A_value_that_moved_without_being_declared_carries_both_markers()
    {
        var line = RepoContextEffectiveConfiguration.DescribeSetting(
            RepoContextHostConfiguration.WalDirKey,
            "/mnt/data/wal",
            "/data/wal",
            RepoContextSettingProvenance.Defaulted);

        Assert.Multiple(() =>
        {
            Assert.That(
                line,
                Does.Contain(RepoContextEffectiveConfiguration.DefaultedMarker),
                "[OVERRIDDEN] answers 'did this move?' and provenance answers 'did anybody "
                + "set it?'; a key moved by a neighbouring key was never declared, and "
                + "[OVERRIDDEN] alone reads as though somebody set it");
            Assert.That(line, Does.Contain("[OVERRIDDEN, default /data/wal]"));
        });
    }

    /// <summary>
    /// An unrecognised provenance must not render as silence, because silence is the shape
    /// that reads as declared.
    /// </summary>
    [Test]
    public void An_unknown_provenance_is_refused_rather_than_rendered_as_nothing()
        => Assert.That(
            () => RepoContextEffectiveConfiguration.RenderProvenance((RepoContextSettingProvenance)99),
            Throws.InstanceOf<ArgumentOutOfRangeException>(),
            "a provenance added later that fell through to an empty marker would restore "
            + "the unqualified line for exactly the settings nobody has thought about yet");

    /// <summary>
    /// The end-to-end reproduction: the container's own configuration, which declared
    /// nothing.
    /// </summary>
    [Test]
    public void The_reporter_marks_an_undeclared_grace_period_as_defaulted()
        => Assert.That(
            ReportedSettingLines(Configuration()),
            Has.Exactly(1).Contains(
                "LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD = 120s (DEFAULTED, not declared)"),
            "the resolver already computes GrantWasDeclared and its own doc comment says "
            + "the distinction exists so it is visible in the startup log; the report used "
            + "to compute it, carry it, and throw it away at the point of printing");

    /// <summary>
    /// The end-to-end negative control, run at the same resolved value as the case above.
    /// </summary>
    /// <remarks>
    /// 120s is the default, so the resolved value, the default, and therefore the
    /// <c>[OVERRIDDEN]</c> comparison are all identical between the two tests. The only
    /// difference is that this configuration declares the variable. If the marker were
    /// unconditional, or were derived from the value comparison rather than from the
    /// declaration, this test fails and the one above still passes.
    /// </remarks>
    [Test]
    public void The_reporter_does_not_mark_a_declared_grace_period_as_defaulted()
    {
        var lines = ReportedSettingLines(
            Configuration((RepoContextShutdownBudget.StopGracePeriodKey, "120s")));
        var grant = lines.Single(l => l.Contains(
            RepoContextShutdownBudget.StopGracePeriodKey, StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                grant,
                Does.Not.Contain(RepoContextEffectiveConfiguration.DefaultedMarker),
                "an operator who declared 120s beside stop_grace_period must not be told "
                + "the value was defaulted - that is the same laundering in the other "
                + "direction, and it would make the marker worthless");
            Assert.That(
                grant,
                Does.Contain(RepoContextEffectiveConfiguration.DeclaredMarker));
        });
    }

    /// <summary>
    /// The end-to-end orthogonality case, which is a second live instance of the same
    /// laundering: <c>LATTICE_WAL_DIR</c> defaults under the data root, so declaring only
    /// <c>LATTICE_DATA_ROOT</c> moves it without anybody declaring it.
    /// </summary>
    [Test]
    public void The_reporter_marks_a_key_moved_by_a_neighbour_as_still_undeclared()
    {
        var lines = ReportedSettingLines(
            Configuration((RepoContextHostConfiguration.DataRootKey, "/mnt/data")));
        var walDir = lines.Single(l => l.StartsWith(
            "Repository-context effective configuration: " + RepoContextHostConfiguration.WalDirKey + " ",
            StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                walDir,
                Does.Contain("[OVERRIDDEN"),
                "the value genuinely moved, so the existing marker is right to appear");
            Assert.That(
                walDir,
                Does.Contain(RepoContextEffectiveConfiguration.DefaultedMarker),
                "nobody declared LATTICE_WAL_DIR; before #2586 this line said [OVERRIDDEN] "
                + "and nothing else, which reads as an operator having set it. Deriving "
                + "provenance from the value comparison rather than from the declaration "
                + "is what this case rules out");
        });
    }

    /// <summary>
    /// The totality guard, and the reason this fix is not confined to one variable.
    /// </summary>
    /// <remarks>
    /// The grace period is where the defect was caught, not where it lives. Any line
    /// reporting a value without stating its origin has the same property, so the
    /// invariant asserted here is over the whole report: no line reaches the log
    /// unqualified. A per-variable assertion would have passed on the day #2586 was filed
    /// for every variable except the one somebody happened to look at.
    /// </remarks>
    [Test]
    public void Every_reported_line_states_where_its_value_came_from()
    {
        var lines = ReportedSettingLines(Configuration(
            (RepoContextHostConfiguration.DataRootKey, "/mnt/data"),
            (RepoContextShutdownBudget.StopGracePeriodKey, "120s"),
            (UnknownKey, "3")));

        Assert.Multiple(() =>
        {
            Assert.That(
                lines,
                Has.Count.GreaterThan(20),
                "a guard over an empty or truncated set passes vacuously; the report "
                + "carries the host settings, the knobs, the grant, the runtime facts and "
                + "the package's own settings, so anything near zero means this fixture "
                + "stopped reading the report rather than that the report shrank");

            Assert.That(
                lines.Where(l => !ProvenanceMarkers.Any(m => l.Contains(m, StringComparison.Ordinal))),
                Is.Empty,
                "every value in this report must state its origin. Marking only the "
                + "settings somebody has been burned by leaves the rest in the shape that "
                + "reads as declared, and the next reader has no way to tell which "
                + "convention a given line was written under");

            Assert.That(
                lines.Where(l => ProvenanceMarkers.Count(
                    m => l.Contains(m, StringComparison.Ordinal)) != 1),
                Is.Empty,
                "exactly one, not at least one: a line carrying two origins is not a line "
                + "a reader can act on");
        });
    }

    /// <summary>
    /// The runtime facts are covered by the guard above, and covered as runtime facts
    /// rather than by being quietly labelled defaulted.
    /// </summary>
    [Test]
    public void A_runtime_fact_is_marked_as_one_rather_than_as_a_defaulted_setting()
    {
        var processorCount = ReportedSettingLines(Configuration()).Single(
            l => l.Contains(
                RepoContextEffectiveConfiguration.RuntimeProcessorCountKey,
                StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                processorCount,
                Does.Contain(RepoContextEffectiveConfiguration.RuntimeMarker),
                "ProcessorCount is not a setting, so calling it defaulted would be a claim "
                + "about a declaration that could never have existed");
            Assert.That(
                processorCount,
                Does.Not.Contain(RepoContextEffectiveConfiguration.DefaultedMarker));
        });
    }

    /// <summary>
    /// A variable an operator supplied that nothing reads is still a variable they
    /// supplied, and the warning about it must not read as though the host defaulted it.
    /// </summary>
    [Test]
    public void A_supplied_but_unread_variable_is_reported_as_declared()
        => Assert.That(
            RepoContextEffectiveConfiguration.DescribeUnreadVariables(
                [new KeyValuePair<string, string?>(UnknownKey, "3")],
                RepoContextEffectiveConfigurationReporter.KnownKeys),
            Has.Exactly(1).Contains(RepoContextEffectiveConfiguration.DeclaredMarker),
            "this arm exists because the operator set something; reporting it without a "
            + "provenance marker would leave one class of line outside the invariant, and "
            + "a partial invariant is one a reader cannot rely on");
}
