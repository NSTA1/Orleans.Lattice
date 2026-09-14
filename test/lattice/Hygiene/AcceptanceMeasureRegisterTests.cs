using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that the acceptance-measure register at
/// <c>docs/lattice.api.mcp.repocontext/acceptance-measures.md</c> parses, names
/// instruments and tag arms that exist in source, and registers no lower-bound
/// threshold above the structural ceiling its own derivation computes
/// (issues #2961, #2879).
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect this exists for is an absence, not a wrong value.</b> Acceptance
/// measure M5 was registered at <c>attempted &gt;= 200</c> against a remedy whose
/// constants bound it at 16. That was wrong and it was loud: it scored a `FAIL`
/// somebody could read. What happened next was the real failure - between two runs
/// the measure was not corrected but <b>dropped</b>, and an absent measure scores
/// nothing at all. The work it covered shipped with no reading and no artefact
/// recorded that a reading was owed.
/// </para>
/// <para>
/// Neither event was reviewable, because the register was not in git. This fixture
/// is the second half of the remedy: tracking the register makes a removal a
/// deletion in a diff, and this guard makes the arithmetic behind every threshold
/// checkable rather than assertable. <b>Re-registering M5 at 200 cannot be
/// committed</b>, because 200 exceeds the ceiling the register's own derivation
/// computes from <c>MaxReportedBlockingConsumers</c>,
/// <c>ReactivationMinBlockAge</c> and <c>ReactivationRetryCooldown</c>.
/// </para>
/// <para>
/// <b>The derivation is evaluated, never read.</b> A register that carried the
/// derivation as prose would be the same artefact that failed before: a
/// true-sounding sentence nothing checks, which stays true-sounding while the
/// constants move underneath it. So the cell is an expression over named source
/// members, and raising <c>MaxReportedBlockingConsumers</c> from 8 reddens the
/// register on the next build rather than silently invalidating the bar.
/// </para>
/// <para>
/// <b>What a green run does NOT establish.</b> Stated explicitly, because a gate
/// whose coverage is unstated will be assumed total.
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>It does not know which measures a run read.</b> The run predicate lives in
/// the scoring harness, not here. This guard proves what is owed is arithmetically
/// coherent; it cannot prove a run paid it. A measure deleted from the harness but
/// left in the register is still invisible, and closing that needs the harness to
/// publish the measure ids it evaluated.
/// </description></item>
/// <item><description>
/// <b>It cannot know the register is complete.</b> No mechanism can tell this file
/// about a measure nobody wrote down. That limit is why the register carries an
/// explicit seeding note rather than presenting its rows as the whole population.
/// </description></item>
/// <item><description>
/// <b>A row with an <c>n/a</c> ceiling has an unverified threshold.</b> It is
/// carried with a stated reason and the arithmetic is not checked, so the guard's
/// strength is not uniform across the table.
/// </description></item>
/// </list>
/// <para>
/// <b>Loud when it matches nothing</b>, on the model of
/// <see cref="MeterFieldDeclarationOrderTests"/>. Every stage of the scan has its
/// own assertion that the stage found something. A register that was emptied would
/// otherwise pass every clause above by quantifying over nothing, which is the
/// untracked register's failure reproduced inside the guard written to prevent it.
/// </para>
/// </remarks>
[TestFixture]
[Category("Hygiene")]
public sealed class AcceptanceMeasureRegisterTests
{
    private const string RegisterPath = "docs/lattice.api.mcp.repocontext/acceptance-measures.md";

    /// <summary>The column headers the register table must carry, in order.</summary>
    private static readonly string[] ExpectedHeaders =
    [
        "Measure", "Status", "Instrument", "Arm", "Arm source",
        "Threshold", "Ceiling", "Ceiling derivation", "Registered", "Retired",
    ];

    /// <summary>
    /// Measures known to be registered when this guard was written, held as a
    /// <i>lower bound</i>. Adding a measure must not redden a guard whose purpose is
    /// to encourage registering them; removing one of these is the exact defect of
    /// issue #2961 and is meant to be loud.
    /// </summary>
    private static readonly string[] KnownMeasures = ["M5", "M6", "A10.1"];

    private const BindingFlags AnyStatic =
        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;

    /// <summary>One parsed register row.</summary>
    /// <param name="Line">The 1-based line the row occupies.</param>
    /// <param name="Cells">The row's cells, by header name.</param>
    internal sealed record MeasureRow(int Line, IReadOnlyDictionary<string, string> Cells)
    {
        internal string this[string column] => Cells[column];

        internal string Measure => this["Measure"];
    }

    // ------------------------------------------------------------- the gate

    /// <summary>
    /// The register parses into rows whose every mandatory cell carries a value and
    /// whose status is one this guard understands.
    /// </summary>
    [Test]
    public void EveryRowIsWellFormed()
    {
        var failures = new List<string>();

        foreach (var row in Rows())
        {
            foreach (var header in ExpectedHeaders)
            {
                if (string.IsNullOrWhiteSpace(row[header]))
                {
                    failures.Add($"line {row.Line}: '{header}' is empty.");
                }
            }

            if (row["Status"] is not ("gating" or "diagnostic" or "retired"))
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} has status '{row["Status"]}'. "
                    + "Use gating, diagnostic, or retired.");
            }

            var retired = row["Retired"];
            if (row["Status"] == "retired" && (retired == "-" || !retired.StartsWith("run ", StringComparison.Ordinal)))
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} is retired but 'Retired' does not record "
                    + "the run and the reason as 'run N: reason'. A retirement nobody can date is "
                    + "indistinguishable from a measure that was silently dropped.");
            }

            if (row["Status"] != "retired" && retired != "-")
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} is live but carries a retirement note "
                    + $"'{retired}'. Set Status to retired, or clear the note to '-'.");
            }
        }

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// Every row names an instrument the core assembly declares, and an arm whose
    /// pre-allocated tag field resolves to exactly the key and value the row states.
    /// </summary>
    /// <remarks>
    /// This is the correspondence clause: a registered measure must point at a
    /// series something in source actually arms. Renaming or deleting an arm reddens
    /// the register rather than leaving the measure pointing at a series that can
    /// never appear - which would read, at scoring time, as a measured absence.
    /// </remarks>
    [Test]
    public void EveryMeasureNamesAnArmThatSourceActuallyArms()
    {
        var declared = DeclaredInstrumentNames();
        Assert.That(
            declared,
            Is.Not.Empty,
            "No instruments were discovered in the core assembly, so the instrument-name clause "
            + "below would pass vacuously for every row.");

        var failures = new List<string>();

        foreach (var row in Rows())
        {
            var instrument = Unbackticked(row["Instrument"]);
            if (!declared.Contains(instrument))
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} reads instrument '{instrument}', which "
                    + "the core assembly does not declare. Either it was renamed or the register is wrong.");
            }

            var armSource = Unbackticked(row["Arm source"]);
            if (ResolveTagField(armSource) is not { } tag)
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} names arm source '{armSource}', which does "
                    + "not resolve to a static KeyValuePair<string, object?> tag field in the core assembly.");
                continue;
            }

            var stated = Unbackticked(row["Arm"]);
            var actual = $"{tag.Key}={tag.Value}";
            if (!string.Equals(stated, actual, StringComparison.Ordinal))
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} states arm '{stated}' but '{armSource}' arms "
                    + $"'{actual}'. The two columns are independent statements of one fact and must agree.");
            }
        }

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// Every stated ceiling is what its derivation computes from the constants
    /// actually in source, and no lower-bound threshold sits above it.
    /// </summary>
    /// <remarks>
    /// The second clause is the one that makes the M5 defect uncommittable. A bar of
    /// <c>&gt;= 200</c> against a derived ceiling of 16 is not a judgement call about
    /// ambition; it is a claim the system's own constants contradict, so it fails
    /// here rather than at scoring time three runs later.
    /// </remarks>
    [Test]
    public void EveryThresholdIsReachableUnderItsDerivedCeiling()
    {
        var failures = new List<string>();

        foreach (var row in Rows())
        {
            var ceilingCell = Unbackticked(row["Ceiling"]);
            var thresholdCell = Unbackticked(row["Threshold"]);

            if (ceilingCell == "n/a")
            {
                if (thresholdCell != "n/a" && !LooksLikeAReason(row["Ceiling derivation"]))
                {
                    failures.Add(
                        $"line {row.Line}: measure {row.Measure} has no structural ceiling but its "
                        + "derivation cell does not say why. An unverified bar must state that it is one.");
                }

                continue;
            }

            if (!int.TryParse(ceilingCell, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ceiling))
            {
                failures.Add($"line {row.Line}: measure {row.Measure} has a non-integer ceiling '{ceilingCell}'.");
                continue;
            }

            int derived;
            try
            {
                derived = DerivationExpression.Evaluate(Unbackticked(row["Ceiling derivation"]));
            }
            catch (InvalidOperationException ex)
            {
                failures.Add($"line {row.Line}: measure {row.Measure} derivation did not evaluate: {ex.Message}");
                continue;
            }

            if (derived != ceiling)
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} states a ceiling of {ceiling} but its "
                    + $"derivation evaluates to {derived} against the constants now in source. Either a "
                    + "constant moved and the bar is stale, or the arithmetic was never done.");
                continue;
            }

            if (ParseThreshold(thresholdCell) is not { } threshold)
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} has an unparseable threshold "
                    + $"'{thresholdCell}'. Use a comparator and an integer, or 'n/a'.");
                continue;
            }

            var unreachable = threshold.Comparator switch
            {
                ">=" => threshold.Value > ceiling,
                ">" => threshold.Value >= ceiling,
                _ => false,
            };

            if (unreachable)
            {
                failures.Add(
                    $"line {row.Line}: measure {row.Measure} registers '{thresholdCell}' against a derived "
                    + $"ceiling of {ceiling}, so it can never pass. This is the M5 defect of issue #2879: a "
                    + "threshold is a claim about the system and must come from the remedy's own constants, "
                    + "not from the magnitude of the defect it addresses.");
            }
        }

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    // --------------------------------------------------- loud-on-empty scan

    /// <summary>
    /// The register exists, parses, and still contains the measures it contained
    /// when this guard was written.
    /// </summary>
    /// <remarks>
    /// Without this every clause above quantifies over an empty set and reports the
    /// same green as a fully checked register. That is precisely the equivalence
    /// issue #2961 is about - an absent measure and a passing one are
    /// indistinguishable - so the guard must not be able to reach it.
    /// </remarks>
    [Test]
    public void TheRegisterIsNotEmptyAndRetainsItsKnownMeasures()
    {
        Assert.That(
            File.Exists(RegisterFullPath()),
            Is.True,
            $"{RegisterPath} does not exist. The register is the artefact; deleting it turns every "
            + "clause in this fixture into a vacuous green.");

        var rows = Rows();

        Assert.Multiple(() =>
        {
            Assert.That(
                rows,
                Is.Not.Empty,
                $"No rows parsed out of {RegisterPath}. Either the table was emptied or its shape "
                + "changed and this guard is checking nothing.");

            Assert.That(
                rows.Select(static r => r.Measure).ToList(),
                Is.SupersetOf(KnownMeasures),
                "A measure registered when this guard was written is no longer in the register. "
                + "Retire it with a run and a reason instead of deleting the row: a deletion is the "
                + "silent drop of issue #2961, and nothing can tell it from a measure that never existed.");
        });
    }

    /// <summary>
    /// At least one row's ceiling is genuinely derived from named source constants.
    /// </summary>
    /// <remarks>
    /// A register in which every ceiling were <c>n/a</c>, or every derivation a bare
    /// literal, would pass the arithmetic clause without ever resolving a constant.
    /// The threshold guard would then be green while checking no property of the
    /// system at all.
    /// </remarks>
    [Test]
    public void AtLeastOneCeilingIsDerivedFromSourceConstants()
    {
        var derivedRows = Rows()
            .Where(static r => Unbackticked(r["Ceiling"]) != "n/a")
            .Where(static r => DerivationExpression.References(Unbackticked(r["Ceiling derivation"])).Count > 0)
            .ToList();

        Assert.That(
            derivedRows,
            Is.Not.Empty,
            "No row derives its ceiling from a named source constant. Every ceiling is either absent "
            + "or a bare number, so EveryThresholdIsReachableUnderItsDerivedCeiling is comparing "
            + "literals to literals and cannot notice a constant moving.");
    }

    // ------------------------------------------------------ positive controls

    /// <summary>
    /// The evaluator reports a lower bound above the ceiling, using the register's
    /// own live derivation rather than a literal.
    /// </summary>
    /// <remarks>
    /// Built from the real row at run time, so it keeps proving the same thing after
    /// the constants change. A control pinned to 16 would rot into a tautology the
    /// day <c>MaxReportedBlockingConsumers</c> moves.
    /// </remarks>
    [Test]
    public void AThresholdAboveTheDerivedCeilingIsRejected()
    {
        var row = ADerivedRow();
        var ceiling = DerivationExpression.Evaluate(Unbackticked(row["Ceiling derivation"]));

        Assert.Multiple(() =>
        {
            Assert.That(ceiling, Is.GreaterThan(0), "The control row's derivation must yield a real bound.");
            Assert.That(
                ParseThreshold($">= {ceiling + 1}")!.Value.Value > ceiling,
                Is.True,
                $"A bar of {ceiling + 1} against a ceiling of {ceiling} must read as unreachable, or the "
                + "clause that makes the M5 defect uncommittable is not doing anything.");
            Assert.That(
                ParseThreshold($">= {ceiling}")!.Value.Value > ceiling,
                Is.False,
                "A bar exactly at the ceiling is reachable and must not be reported, or the guard would "
                + "reject the corrected M5 as well as the broken one.");
        });
    }

    /// <summary>
    /// The derivation evaluator resolves real members and rejects absent ones, so a
    /// renamed constant cannot silently satisfy a stale expression.
    /// </summary>
    [Test]
    public void TheDerivationEvaluatorResolvesRealMembersAndRejectsAbsentOnes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                DerivationExpression.Evaluate("LatticeWalGc.MaxReportedBlockingConsumers"),
                Is.EqualTo(8),
                "The evaluator must read the live constant. If this number changed, the register's "
                + "derivations need re-deriving, which is exactly what this guard is for.");

            Assert.That(
                DerivationExpression.Evaluate("LatticeWalGcScheduler.ReactivationRetryCooldown"),
                Is.EqualTo(15),
                "A TimeSpan member must resolve to its whole minutes, and a private static must be "
                + "reachable without widening its visibility.");

            Assert.That(
                () => DerivationExpression.Evaluate("LatticeWalGc.NoSuchConstantExists"),
                Throws.InstanceOf<InvalidOperationException>(),
                "An expression naming a member that does not exist must fail loudly. Resolving it to "
                + "zero would let a renamed constant satisfy a stale derivation in silence.");

            Assert.That(
                () => DerivationExpression.Evaluate("8 * ("),
                Throws.InstanceOf<InvalidOperationException>(),
                "An unparseable derivation must fail rather than evaluate to something.");
        });
    }

    // ------------------------------------------------------------ mechanics

    private static string RegisterFullPath() =>
        Path.Combine(HygieneRepository.FindRepoRoot(), RegisterPath.Replace('/', Path.DirectorySeparatorChar));

    private static IReadOnlyList<MeasureRow> Rows() => RowsLazy.Value;

    private static readonly Lazy<IReadOnlyList<MeasureRow>> RowsLazy = new(ParseRows);

    /// <summary>
    /// Parses the register's single measure table, located by its header row rather
    /// than by position, so adding explanatory tables above or below it is harmless.
    /// </summary>
    private static IReadOnlyList<MeasureRow> ParseRows()
    {
        var path = RegisterFullPath();
        if (!File.Exists(path))
        {
            return [];
        }

        var lines = File.ReadAllLines(path);
        var rows = new List<MeasureRow>();
        var inTable = false;

        for (var i = 0; i < lines.Length; i++)
        {
            var cells = SplitRow(lines[i]);
            if (cells is null)
            {
                inTable = false;
                continue;
            }

            if (!inTable)
            {
                if (cells.SequenceEqual(ExpectedHeaders, StringComparer.Ordinal))
                {
                    inTable = true;
                }

                continue;
            }

            if (cells.All(static c => c.Trim('-', ':').Length == 0))
            {
                continue;
            }

            if (cells.Count != ExpectedHeaders.Length)
            {
                throw new InvalidOperationException(
                    $"{RegisterPath} line {i + 1} has {cells.Count} cells, expected {ExpectedHeaders.Length}. "
                    + "A register that cannot be parsed must fail the build, not be skipped.");
            }

            rows.Add(new MeasureRow(
                i + 1,
                ExpectedHeaders
                    .Select((h, n) => (Header: h, Value: cells[n]))
                    .ToDictionary(static x => x.Header, static x => x.Value, StringComparer.Ordinal)));
        }

        return rows;
    }

    /// <summary>Splits a markdown table row, or returns null when the line is not one.</summary>
    private static IReadOnlyList<string>? SplitRow(string line)
    {
        var trimmed = line.Trim();
        if (trimmed.Length < 2 || trimmed[0] != '|' || trimmed[^1] != '|')
        {
            return null;
        }

        return trimmed[1..^1].Split('|').Select(static c => c.Trim()).ToList();
    }

    private static string Unbackticked(string cell) => cell.Trim().Trim('`').Trim();

    /// <summary>
    /// A derivation cell stands in for a missing ceiling only if it says something.
    /// A bare dash would leave an unverified bar with no recorded reason.
    /// </summary>
    private static bool LooksLikeAReason(string cell) => cell.Trim().Length > 20;

    private static (string Comparator, int Value)? ParseThreshold(string cell)
    {
        var text = cell.Trim();
        if (text == "n/a")
        {
            return null;
        }

        foreach (var comparator in new[] { ">=", "<=", "==", ">", "<" })
        {
            if (!text.StartsWith(comparator, StringComparison.Ordinal))
            {
                continue;
            }

            return int.TryParse(
                text[comparator.Length..].Trim(),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var value)
                ? (comparator, value)
                : null;
        }

        return null;
    }

    /// <summary>A register row whose ceiling is genuinely derived, for the controls.</summary>
    private static MeasureRow ADerivedRow() =>
        Rows().FirstOrDefault(static r =>
            Unbackticked(r["Ceiling"]) != "n/a"
            && DerivationExpression.References(Unbackticked(r["Ceiling derivation"])).Count > 0)
        ?? throw new InvalidOperationException(
            "No register row derives a ceiling from source constants, so the controls cannot be built. "
            + "AtLeastOneCeilingIsDerivedFromSourceConstants reports the same condition.");

    private static readonly Lazy<IReadOnlySet<string>> DeclaredInstrumentNamesLazy = new(() =>
    {
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var type in typeof(LatticeMetrics).Assembly.GetTypes())
        {
            foreach (var field in type.GetFields(AnyStatic))
            {
                if (!typeof(Instrument).IsAssignableFrom(field.FieldType))
                {
                    continue;
                }

                try
                {
                    if (field.GetValue(null) is Instrument instrument)
                    {
                        names.Add(instrument.Name);
                    }
                }
                catch (Exception)
                {
                    // A field whose static initialiser needs a host is not evidence either way.
                }
            }
        }

        return names;
    });

    private static IReadOnlySet<string> DeclaredInstrumentNames() => DeclaredInstrumentNamesLazy.Value;

    /// <summary>
    /// Resolves a <c>Type.Member</c> reference to the tag it pre-allocates, or null
    /// when it is not a tag field.
    /// </summary>
    private static KeyValuePair<string, object?>? ResolveTagField(string reference)
    {
        if (DerivationExpression.ResolveMember(reference) is not FieldInfo field)
        {
            return null;
        }

        if (field.FieldType != typeof(KeyValuePair<string, object?>))
        {
            return null;
        }

        return (KeyValuePair<string, object?>)field.GetValue(null)!;
    }
}
