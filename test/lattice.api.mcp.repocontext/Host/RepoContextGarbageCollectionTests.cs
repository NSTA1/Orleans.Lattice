using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers issue #2596: the collector this process runs under has to be visible, and the
/// combination that suspends the whole process for minutes has to be loud.
/// </summary>
/// <remarks>
/// <para>
/// <b>What actually happened.</b> The container ran Workstation GC against a 12 GiB memory
/// limit at about 11 GiB resident. The runtime attributed a 252.2 second pause to garbage
/// collection in a single stall, against a 30 second request timeout. Orleans had logged
/// <c>Note: Silo not running with ServerGC turned on</c> at startup on both of the two
/// gate runs that failed, and the runtime had separately attributed 172 individual stalls
/// to collector pauses in one of those runs. Every one of those signals was true, present,
/// and unread.
/// </para>
/// <para>
/// <b>Why the facts are a parameter.</b> The collector reads its configuration once, at
/// process start, so no fixture can put its own process into the hazardous combination. A
/// diagnostic reachable only by inspection is one nobody can assert on, which is how a
/// warning comes to be trusted without ever having been exercised.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextGarbageCollectionTests
{
    private const long TwelveGiB = 12L * 1024 * 1024 * 1024;
    private const long OneGiB = 1024L * 1024 * 1024;

    /// <summary>The measured container: Workstation GC on a 12 GiB ceiling.</summary>
    private static RepoContextGarbageCollectionFacts Hazardous { get; }
        = new(IsServerGc: false, ResolvedHeapCount: 1, TwelveGiB, TimeSpan.FromSeconds(252.2));

    /// <summary>The recommended remedy: Server GC with a bounded heap count.</summary>
    private static RepoContextGarbageCollectionFacts Remedied { get; }
        = new(IsServerGc: true, ResolvedHeapCount: 6, TwelveGiB, TimeSpan.FromSeconds(1.4));

    private static IConfiguration Configuration(params (string Key, string Value)[] settings)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(settings.ToDictionary(s => s.Key, s => (string?)s.Value))
            .Build();

    [Test]
    public void The_running_process_reports_coherent_collector_facts()
    {
        var facts = RepoContextGarbageCollection.ReadRuntimeFacts();

        Assert.Multiple(() =>
        {
            Assert.That(
                facts.TotalAvailableMemoryBytes,
                Is.GreaterThan(0),
                "the ceiling is the figure that decides how large the heap can grow before "
                + "a blocking collection has to walk it; a zero would make the hazard rule "
                + "silently unfireable");
            Assert.That(
                facts.ResolvedHeapCount,
                Is.Null.Or.GreaterThan(0),
                "the heap count is read from a dictionary whose key names are not a "
                + "documented contract, so it is allowed to be unknown - but never a "
                + "fabricated zero, which would read as a real figure");
            Assert.That(facts.TotalPauseDuration, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
        });
    }

    /// <summary>
    /// The resolved heap count under this test process must agree with the mode, which is
    /// the invariant the whole "declared is not resolved" distinction rests on.
    /// </summary>
    [Test]
    public void Workstation_collection_resolves_a_single_heap()
    {
        var facts = RepoContextGarbageCollection.ReadRuntimeFacts();

        Assume.That(facts.IsServerGc, Is.False, "this assertion is about the workstation case");

        Assert.That(
            facts.ResolvedHeapCount,
            Is.EqualTo(1).Or.Null,
            "workstation GC collects a single heap by construction. If this ever reports "
            + "otherwise, the key this host reads the resolved count from has changed "
            + "meaning and every heap-count line in the report is wrong");
    }

    [TestCase(false, TwelveGiB, true, TestName = "Workstation_on_a_large_ceiling_is_hazardous")]
    [TestCase(true, TwelveGiB, false, TestName = "Server_on_a_large_ceiling_is_not")]
    [TestCase(false, OneGiB, false, TestName = "Workstation_on_a_small_ceiling_is_not")]
    [TestCase(true, OneGiB, false, TestName = "Server_on_a_small_ceiling_is_not")]
    public void The_hazard_is_the_pair_and_not_either_half(bool serverGc, long limit, bool expected)
        => Assert.That(
            RepoContextGarbageCollection.IsPauseHazard(
                new RepoContextGarbageCollectionFacts(serverGc, 1, limit, TimeSpan.Zero)),
            Is.EqualTo(expected),
            "neither half is a defect alone: workstation GC on a small heap is the correct "
            + "default that most hosts should run, and a large ceiling under server GC is "
            + "exactly the configuration being recommended. A rule that fired on either "
            + "half would be noise, and noise is how the original warning came to be "
            + "ignored");

    [Test]
    public void The_hazard_threshold_sits_below_the_measured_failure()
        => Assert.That(
            RepoContextGarbageCollection.PauseHazardMemoryLimitBytes,
            Is.LessThan(TwelveGiB),
            "the measured failure was at about 11 GiB on a 12 GiB limit; a threshold at or "
            + "above it would leave the one configuration this whole surface was written "
            + "for unflagged");

    [Test]
    public void The_report_names_the_collector_mode_and_the_ceiling_it_runs_against()
    {
        var lines = RepoContextGarbageCollection.DescribeSettings(Hazardous, Configuration());

        Assert.Multiple(() =>
        {
            Assert.That(
                lines,
                Has.Exactly(1).Contains(
                    RepoContextGarbageCollection.RuntimeModeKey + " = "
                    + RepoContextGarbageCollection.WorkstationMode),
                "the mode is the single fact that was never in the operator-facing report, "
                + "and reading it out of the container took an argument that spanned two "
                + "rounds");
            Assert.That(
                lines,
                Has.Exactly(1).Contains(RepoContextGarbageCollection.RuntimeHeapCountKey + " = 1"));
            Assert.That(
                lines,
                Has.Exactly(1).Contains("12 GiB"),
                "the ceiling has to be stated beside the mode, because the hazard is the "
                + "pair - a reader given only the mode cannot tell whether it matters here");
        });
    }

    [Test]
    public void The_report_names_the_pause_total_the_runtime_already_measures()
        => Assert.That(
            RepoContextGarbageCollection.DescribeSettings(Hazardous, Configuration()),
            Has.Exactly(1).Contains(RepoContextGarbageCollection.RuntimePauseTotalKey),
            "two people spent two rounds building a proxy for collector pause time out of "
            + "gaps between log timestamps while the runtime was emitting the quantity by "
            + "name in the file both were reading. Naming the measurand in the report is "
            + "what stops a third attempt");

    /// <summary>
    /// The #2586 trap, applied to this surface: a resolved figure must not print in the
    /// shape of a declared one.
    /// </summary>
    [Test]
    public void A_declared_heap_count_is_distinguishable_from_an_inferred_one()
    {
        var declared = RepoContextGarbageCollection.DescribeSettings(
            Remedied,
            Configuration((RepoContextGarbageCollection.HeapCountKey, "6")));
        var inferred = RepoContextGarbageCollection.DescribeSettings(Remedied, Configuration());

        var declaredLine = declared.Single(l => l.StartsWith(
            RepoContextGarbageCollection.HeapCountKey, StringComparison.Ordinal));
        var inferredLine = inferred.Single(l => l.StartsWith(
            RepoContextGarbageCollection.HeapCountKey, StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                declaredLine,
                Does.Contain(RepoContextEffectiveConfiguration.DeclaredMarker));
            Assert.That(
                declaredLine,
                Does.Not.Contain(RepoContextEffectiveConfiguration.DefaultedMarker));
            Assert.That(
                inferredLine,
                Does.Contain(RepoContextEffectiveConfiguration.DefaultedMarker),
                "the resolved heap count is 6 in both cases, so nothing but the declaration "
                + "can be producing the difference. Without this, a report stating '6' for "
                + "a container that declared nothing is the #2593 defect verbatim");
        });
    }

    [Test]
    public void Every_collector_line_states_where_its_value_came_from()
    {
        var lines = RepoContextGarbageCollection.DescribeSettings(
            Hazardous,
            Configuration((RepoContextGarbageCollection.ServerGcKey, "0")));

        string[] markers =
        [
            RepoContextEffectiveConfiguration.DeclaredMarker,
            RepoContextEffectiveConfiguration.DefaultedMarker,
            RepoContextEffectiveConfiguration.RuntimeMarker,
        ];

        Assert.Multiple(() =>
        {
            Assert.That(lines, Is.Not.Empty, "a guard over an empty set passes vacuously");
            Assert.That(
                lines.Where(l => markers.Count(m => l.Contains(m, StringComparison.Ordinal)) != 1),
                Is.Empty,
                "exactly one origin per line. These lines join a report whose totality "
                + "guard already holds for every other setting, so an unqualified collector "
                + "line would be the one exception a reader has no way to spot");
        });
    }

    [Test]
    public void The_hazardous_combination_is_named_with_the_variables_that_fix_it()
    {
        var hazards = RepoContextGarbageCollection.DescribeHazards(Hazardous, Configuration());

        Assert.Multiple(() =>
        {
            Assert.That(hazards, Has.Exactly(1).Contains(RepoContextGarbageCollection.HazardMarker));
            Assert.That(
                hazards,
                Has.Exactly(1).Contains(RepoContextGarbageCollection.ServerGcKey),
                "a warning that describes a problem without naming the variable that "
                + "changes it leaves the reader exactly where Orleans' own note left two "
                + "gate runs");
            Assert.That(hazards, Has.Exactly(1).Contains(RepoContextGarbageCollection.HeapCountKey));
        });
    }

    /// <summary>
    /// The claim in the warning must be the narrow one the evidence supports.
    /// </summary>
    /// <remarks>
    /// Garbage collection accounted for 31.7% of long-silence time in the measured run and
    /// did not explain the largest timeout burst at all, so a warning promising to fix the
    /// container would be false in advance. Attaching correct evidence to a broader remedy
    /// than it supports is the failure this epic keeps repeating, and a warning is exactly
    /// where it would be repeated next.
    /// </remarks>
    [Test]
    public void The_warning_does_not_claim_to_cure_stalls_in_general()
    {
        var hazard = RepoContextGarbageCollection.DescribeHazards(Hazardous, Configuration())
            .Single(h => h.Contains("Workstation garbage collection against", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                hazard,
                Does.Contain("not a remedy for long stalls in general"),
                "the honest claim is that this removes the multi-minute collector-attributed "
                + "pause class, not that it fixes the deployment");
            Assert.That(
                hazard,
                Does.Contain("class of pause"),
                "the remedy is scoped to a class, and the scoping has to survive in the "
                + "text an operator actually reads");
        });
    }

    [Test]
    public void A_safe_combination_is_not_flagged()
        => Assert.That(
            RepoContextGarbageCollection.DescribeHazards(
                Remedied,
                Configuration(
                    (RepoContextGarbageCollection.ServerGcKey, "1"),
                    (RepoContextGarbageCollection.HeapCountKey, "6"))),
            Is.Empty,
            "the negative control, and it is load-bearing: a rule that fired unconditionally "
            + "would pass every positive assertion above while training readers to skip the "
            + "line, which is the failure mode the unread Orleans note already demonstrated");

    /// <summary>
    /// A declaration that binds to nothing reads as configured, which is #2279's shape.
    /// </summary>
    [Test]
    public void A_heap_count_declared_under_workstation_collection_is_reported_as_inert()
    {
        var hazards = RepoContextGarbageCollection.DescribeHazards(
            Hazardous,
            Configuration((RepoContextGarbageCollection.HeapCountKey, "6")));

        Assert.That(
            hazards,
            Has.Exactly(1).Contains("inert"),
            "DOTNET_GCHeapCount takes effect only under server GC. An operator who set it "
            + "while the process ran workstation GC has a variable in their compose file "
            + "that binds to nothing, with no error and nothing distinguishing 'applied' "
            + "from 'ignored'");
    }

    /// <summary>
    /// The notation trap, verified against this runtime rather than taken from prose:
    /// <c>DOTNET_GCHeapCount=10</c> resolves 16 heaps, because the collector reads its
    /// numeric environment variables as hexadecimal.
    /// </summary>
    [TestCase("10", 16, TestName = "Ten_declared_resolves_sixteen")]
    [TestCase("16", 22, TestName = "Sixteen_declared_resolves_twenty_two")]
    [TestCase("12", 18, TestName = "Twelve_declared_resolves_eighteen")]
    public void A_heap_count_written_in_decimal_is_reported_as_read_in_hexadecimal(
        string declared, int resolved)
    {
        var hazard = RepoContextGarbageCollection.DescribeHeapCountNotationHazard(declared, resolved);

        Assert.Multiple(() =>
        {
            Assert.That(
                hazard,
                Is.Not.Null,
                "the same setting is decimal in runtimeconfig.json and hexadecimal as an "
                + "environment variable, so an operator deriving a heap count from a CPU "
                + "grant and writing it plainly gets a number they did not ask for and no "
                + "signal that they did");
            Assert.That(hazard, Does.Contain("HEXADECIMAL"));
            Assert.That(
                hazard,
                Does.Contain(resolved.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                "the resolved figure is the evidence, so it has to be in the line rather "
                + "than left for the reader to go and find");
        });
    }

    [TestCase(null, 6, TestName = "Nothing_declared")]
    [TestCase("", 6, TestName = "Blank_declared")]
    [TestCase("6", 6, TestName = "Single_digit_reads_the_same_either_way")]
    [TestCase("0x10", 16, TestName = "Explicit_hex_prefix_is_unambiguous")]
    [TestCase("c", 12, TestName = "A_hex_digit_cannot_be_read_as_decimal")]
    [TestCase("10", 10, TestName = "A_value_that_resolved_as_written_is_not_a_trap")]
    public void An_unambiguous_heap_count_is_not_flagged(string? declared, int resolved)
        => Assert.That(
            RepoContextGarbageCollection.DescribeHeapCountNotationHazard(declared, resolved),
            Is.Null,
            "the check compares the resolved figure against the decimal reading, so it "
            + "stays silent whenever the operator got the number they wrote. A warning on "
            + "every declaration would be noise");

    [Test]
    public void An_unknown_resolved_heap_count_is_not_guessed_at()
        => Assert.That(
            RepoContextGarbageCollection.DescribeHeapCountNotationHazard("10", null),
            Is.Null,
            "with no resolved figure there is no evidence of a disagreement, and asserting "
            + "one anyway would be a claim about configuration that nothing checks - the "
            + "hazard this report family exists to remove");

    [Test]
    public void A_byte_count_is_rendered_for_both_of_its_readers()
        => Assert.That(
            RepoContextGarbageCollection.RenderBytes(TwelveGiB),
            Is.EqualTo("12 GiB (12884901888 bytes)"),
            "the GiB figure is what an operator compares against a compose file's memory "
            + "limit; the exact byte count is what survives being pasted into an issue as "
            + "evidence");

    [Test]
    public void An_unknown_heap_count_is_stated_as_unknown_rather_than_omitted()
    {
        var lines = RepoContextGarbageCollection.DescribeSettings(
            new RepoContextGarbageCollectionFacts(true, null, TwelveGiB, TimeSpan.Zero),
            Configuration());

        Assert.That(
            lines,
            Has.Exactly(1).Contains(RepoContextGarbageCollection.RuntimeHeapCountKey + " = <unknown"),
            "an omitted line reads as a setting that was checked and found irrelevant. The "
            + "key this figure is read from is not a documented contract, so it has to be "
            + "able to say it does not know");
    }

    [Test]
    public void The_describe_entry_points_reject_a_null_configuration()
        => Assert.Multiple(() =>
        {
            Assert.That(
                () => RepoContextGarbageCollection.DescribeSettings(Hazardous, null!),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => RepoContextGarbageCollection.DescribeHazards(Hazardous, null!),
                Throws.InstanceOf<ArgumentNullException>());
        });

    [Test]
    public void The_two_collector_modes_render_distinguishably()
        => Assert.That(
            RepoContextGarbageCollection.ServerMode,
            Is.Not.EqualTo(RepoContextGarbageCollection.WorkstationMode),
            "the report's whole value here is that a reader can tell which one is running");

    [Test]
    public void The_facts_record_carries_the_values_it_was_constructed_with()
    {
        var facts = new RepoContextGarbageCollectionFacts(true, 6, TwelveGiB, TimeSpan.FromSeconds(3));

        Assert.Multiple(() =>
        {
            Assert.That(facts.IsServerGc, Is.True);
            Assert.That(facts.ResolvedHeapCount, Is.EqualTo(6));
            Assert.That(facts.TotalAvailableMemoryBytes, Is.EqualTo(TwelveGiB));
            Assert.That(facts.TotalPauseDuration, Is.EqualTo(TimeSpan.FromSeconds(3)));
            Assert.That(
                facts,
                Is.EqualTo(new RepoContextGarbageCollectionFacts(true, 6, TwelveGiB, TimeSpan.FromSeconds(3))),
                "value equality is what lets a fixture state an expected configuration "
                + "rather than assert field by field");
        });
    }
}
