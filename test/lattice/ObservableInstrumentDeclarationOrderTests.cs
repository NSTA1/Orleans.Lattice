using System.Diagnostics.Metrics;
using System.IO;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Guards the <b>generalised</b> instrument declaration-order invariant across
/// all of <c>src/</c>: an observable instrument field must be declared below
/// every static field its own callback reads.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="MeterFieldDeclarationOrderTests"/> guards the narrow case - the
/// <c>Meter</c> field above the first instrument - and by construction scans
/// only classes that declare a <c>Meter</c> field of their own. A class that
/// builds its instruments from another type's meter declares no <c>Meter</c>
/// field, so that guard does not scan it at all. Its silence there is correct,
/// but it leaves the generalised invariant unguarded over exactly the files
/// which need it most: at the time of writing
/// <c>BPlusLeafGrain.Activation.cs</c> (4 observables),
/// <c>CoordinatorPhaseTickCensus.cs</c> (1), and
/// <c>LatticeBackupMetrics.cs</c> (7) are all invisible to it.
/// </para>
/// <para>
/// The hazard: <c>MeterListener.Start()</c> raises
/// <c>InstrumentPublished</c> for instruments that already exist, outside the
/// lock that registers the listener. A callback holding the first reference in
/// the process to a metrics class therefore runs that class's static
/// initialiser <b>re-entrantly and part-way through</b>. Static field
/// initialisers execute in declaration order, so any field declared below the
/// instrument being published is still at its default when the callback fires.
/// </para>
/// <para>
/// <b>The symptom depends on the field's type, and only one of the two shapes
/// is documented elsewhere.</b> A <i>reference</i>-typed read null-dereferences,
/// the callback throws inside the listener, and the series goes missing - the
/// shape the repository instructions describe. An <i>int</i>-typed read cannot
/// null-dereference, because the CLR zero-initialises statics before any
/// initialiser runs: it returns <c>0</c>, and is correct on the very next
/// collection. That is a <b>transient false zero</b>, and it is strictly worse -
/// <c>0</c> is a plausible real reading for a queue depth or an unsized
/// ceiling, it self-heals so re-reading erases the evidence, and nothing is
/// logged. Both shapes are in scope here (issue #3052).
/// </para>
/// <para>
/// Only static <b>fields</b> are checked. Methods have no initialiser and are
/// callable from the moment the type exists, so a callback may reference a
/// method declared below it with no hazard - which is why the analysis follows
/// a method group or helper call <i>through</i> to the fields it reads rather
/// than reporting the method's own position. <c>const</c> fields are excluded
/// for the same reason: they are compile-time constants with no initialiser to
/// be part-way through.
/// </para>
/// </remarks>
[TestFixture]
public class ObservableInstrumentDeclarationOrderTests
{
    /// <summary>
    /// The observable instrument type names, discovered reflectively from the
    /// running framework rather than hard-coded.
    /// </summary>
    /// <remarks>
    /// Deliberately not a literal list. A list would have to be edited by the
    /// same person who forgot to edit the analysis, so it would drift in
    /// lockstep and prove nothing - the reasoning
    /// <see cref="MeterFieldDeclarationOrderTests"/> already applies to its own
    /// pattern. Derived this way, an observable instrument kind added to the BCL
    /// is covered the day the test framework is retargeted.
    /// </remarks>
    private static readonly HashSet<string> ObservableInstrumentNames = DiscoverObservableInstrumentTypeNames();

    /// <summary>One static field read by a callback, and how the analysis reached it.</summary>
    private sealed record FieldRead(string Field, int FieldLine, string Via);

    /// <summary>An observable instrument declared above static state its callback reads.</summary>
    private sealed record Violation(string Type, string Instrument, int InstrumentLine, FieldRead Read);

    /// <summary>
    /// The outcome of analysing one compilation unit. The two counters exist so
    /// the caller can distinguish "scanned and clean" from "scanned nothing",
    /// which are otherwise byte-identical.
    /// </summary>
    private sealed record ScanResult(
        int ObservablesScanned,
        int FieldReadsResolved,
        IReadOnlyList<Violation> Violations);

    [Test]
    public void Every_observable_instrument_is_declared_below_the_static_state_its_callback_reads()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var violations = new List<string>();
        var observablesScanned = 0;
        var fieldReadsResolved = 0;
        var filesWithObservables = new List<string>();

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            var result = Analyse(File.ReadAllText(file));
            if (result.ObservablesScanned == 0) continue;

            observablesScanned += result.ObservablesScanned;
            fieldReadsResolved += result.FieldReadsResolved;
            filesWithObservables.Add(Path.GetRelativePath(root, file));

            foreach (var violation in result.Violations)
            {
                violations.Add(
                    $"{Path.GetRelativePath(root, file)}: {violation.Type}.{violation.Instrument} is declared at "
                    + $"line {violation.InstrumentLine}, above the static field '{violation.Read.Field}' its "
                    + $"callback reads at line {violation.Read.FieldLine}"
                    + (violation.Read.Via.Length == 0 ? "" : $" (via {violation.Read.Via})") + ".");
            }
        }

        // Anti-vacuity, first arm: the scan found observable instruments at all.
        Assert.That(observablesScanned, Is.GreaterThan(0),
            "Found no observable instrument field anywhere under src/. The analysis has drifted from the "
            + "source, so this guard is silently vacuous.");

        // Anti-vacuity, second arm, and the one that actually matters. Finding
        // instruments but resolving zero field reads yields zero violations and a
        // green test, which is byte-identical to a clean repository. This is the
        // arm that fires if identifier resolution silently stops working.
        Assert.That(fieldReadsResolved, Is.GreaterThan(0),
            $"Scanned {observablesScanned} observable instrument(s) across {filesWithObservables.Count} file(s) "
            + "but resolved no static field read from any of their callbacks. The ordering check therefore "
            + "compared nothing against nothing and would pass however the fields were ordered. Identifier "
            + "resolution has broken.\nFiles scanned: " + string.Join(", ", filesWithObservables));

        Assert.That(violations, Is.Empty,
            "An observable instrument is declared above static state its own callback reads. When a listener "
            + "publishes that instrument from inside the type initialiser, the callback runs before the field "
            + "is assigned: a reference field null-dereferences and the series goes missing, and an int field "
            + "silently reads 0 and self-heals on the next collection. Move the instrument below every field "
            + "its callback touches - declaring observable instruments last in the class satisfies this by "
            + "construction.\n" + string.Join("\n", violations));
    }

    /// <summary>
    /// Anti-vacuity, third arm, and the one aimed squarely at the population
    /// this guard exists for.
    /// <para>
    /// The two arms inside the repository scan assert only that *something* was
    /// found. A scan that had silently stopped matching four of the five files
    /// would satisfy both and still report clean, because the ordering check can
    /// only fail on a file it actually read. The blind spot the narrow
    /// Meter-field guard has is precisely a population it never reads, so
    /// reproducing that shape here would defeat the point of the fixture.
    /// </para>
    /// <para>
    /// The floor is deliberately a minimum rather than an equality. An observable
    /// added to one of these classes may only raise the count, so a floor cannot
    /// be tripped by legitimate growth and needs no edit when an instrument is
    /// added; it is crossed only by the analysis losing sight of something it can
    /// see today. An equality would convert every new instrument into a failing
    /// unrelated test, and a test that cries wolf gets its numbers bumped without
    /// being read.
    /// </para>
    /// </summary>
    [Test]
    public void Repository_scan_covers_the_population_the_meter_field_guard_cannot_see()
    {
        // Files declaring an observable instrument but NO Meter field of their
        // own, i.e. exactly what the narrow ordering guard skips. Counts are the
        // measured floor at the time of writing, not a budget.
        var expected = new (string Path, int MinObservables)[]
        {
            (Path.Combine("src", "lattice", "BPlusTree", "Grains", "BPlusLeafGrain.Activation.cs"), 4),
            (Path.Combine("src", "lattice", "BPlusTree", "Grains", "CoordinatorPhaseTickCensus.cs"), 1),
            (Path.Combine("src", "lattice.backup", "LatticeBackupMetrics.cs"), 7),
        };

        var root = HygieneRepository.FindRepoRoot();
        var shortfalls = new List<string>();

        foreach (var (relative, minimum) in expected)
        {
            var absolute = Path.Combine(root, relative);
            if (!File.Exists(absolute))
            {
                shortfalls.Add(
                    $"{relative}: no such file. It was in the population this guard was written for, so either "
                    + "it moved and this list needs updating, or the guard has quietly lost coverage of it.");
                continue;
            }

            var scanned = Analyse(File.ReadAllText(absolute)).ObservablesScanned;
            if (scanned < minimum)
            {
                shortfalls.Add(
                    $"{relative}: the analysis found {scanned} observable instrument(s), below the recorded "
                    + $"floor of {minimum}. A floor can only be crossed by the analysis losing sight of an "
                    + "instrument it could previously see, never by one being added.");
            }
        }

        Assert.That(shortfalls, Is.Empty,
            "The repository scan no longer covers the population this guard exists for: classes that build "
            + "their instruments from another type's meter and so declare no Meter field, which is the exact "
            + "set the narrow ordering guard skips. The scan-wide anti-vacuity arms cannot catch this, because "
            + "they pass as long as any one file still matches.\n" + string.Join("\n", shortfalls));
    }

    /// <summary>
    /// Reconciles the observable instrument names the analysis recognises
    /// against those the runtime actually offers, so the guard cannot silently
    /// stop covering an observable kind the BCL later adds.
    /// </summary>    [Test]
    public void Analysis_recognises_every_observable_instrument_type_the_runtime_offers()
    {
        var runtimeObservables = DiscoverObservableInstrumentTypeNames();

        Assert.That(runtimeObservables.Count, Is.GreaterThanOrEqualTo(3),
            "Discovered fewer observable instrument types than System.Diagnostics.Metrics is known to ship "
            + "(ObservableCounter, ObservableUpDownCounter, ObservableGauge). The reflection predicate has "
            + "drifted, so this test would pass vacuously.");

        var unmatched = runtimeObservables
            .Where(name => !IsObservableInstrumentType(SyntaxFactory.ParseTypeName($"{name}<long>")))
            .ToList();

        Assert.That(unmatched, Is.Empty,
            $"The analysis matched {runtimeObservables.Count - unmatched.Count} of {runtimeObservables.Count} "
            + "observable instrument types the runtime offers. A field of an unmatched type is invisible to "
            + "this guard, so its callback could read state declared below it and fail silently.\nUnmatched: "
            + string.Join(", ", unmatched));
    }

    /// <summary>
    /// Proves the analysis flags an instrument declared above a field its
    /// callback reads directly, and passes the same source once the declarations
    /// are swapped. Synthetic rather than drawn from <c>src/</c>, because every
    /// file under <c>src/</c> is correctly ordered today, so the repository
    /// corpus alone cannot falsify this.
    /// </summary>
    [Test]
    public void Analysis_flags_an_observable_declared_above_a_field_its_callback_reads()
    {
        const string violating = """
            internal static class ProbeMetrics
            {
                internal static readonly ObservableGauge<int> Depth =
                    Other.Meter.CreateObservableGauge("probe.depth", static () => _depth);

                private static int _depth = 7;
            }
            """;

        const string compliant = """
            internal static class ProbeMetrics
            {
                private static int _depth = 7;

                internal static readonly ObservableGauge<int> Depth =
                    Other.Meter.CreateObservableGauge("probe.depth", static () => _depth);
            }
            """;

        var bad = Analyse(violating);
        var good = Analyse(compliant);

        Assert.Multiple(() =>
        {
            Assert.That(bad.ObservablesScanned, Is.EqualTo(1),
                "The ObservableGauge<int> field was not detected, so the ordering check never ran on it.");
            Assert.That(bad.FieldReadsResolved, Is.EqualTo(1),
                "The callback's read of '_depth' was not resolved, so the check compared nothing.");
            Assert.That(bad.Violations, Has.Count.EqualTo(1));
            Assert.That(bad.Violations[0].Read.Field, Is.EqualTo("_depth"));
            Assert.That(bad.Violations[0].Read.Via, Is.Empty,
                "A direct read must be reported with no indirection chain.");

            Assert.That(good.ObservablesScanned, Is.EqualTo(1));
            Assert.That(good.FieldReadsResolved, Is.EqualTo(1),
                "The compliant arm must resolve the same read, or it passes for the wrong reason.");
            Assert.That(good.Violations, Is.Empty);
        });
    }

    /// <summary>
    /// Proves the analysis follows a method group callback through to the fields
    /// that method reads, and that the method's <i>own</i> position is
    /// irrelevant.
    /// </summary>
    /// <remarks>
    /// This is the <c>CoordinatorPhaseTickCensus</c> and
    /// <c>LatticeBackupMetrics</c> shape: the callback is
    /// <c>Observe</c> rather than a lambda, and <c>Observe</c> is declared far
    /// below the instrument. That is correct and must not be flagged - a method
    /// has no initialiser. Only the field it reads is subject to the ordering.
    /// Without this arm the analysis could report the method's line and appear
    /// to work.
    /// </remarks>
    [Test]
    public void Analysis_follows_a_method_group_callback_to_the_fields_it_reads()
    {
        const string violating = """
            internal static class ProbeMetrics
            {
                internal static readonly ObservableGauge<long> Live =
                    Other.Meter.CreateObservableGauge("probe.live", Observe);

                private static readonly Dictionary<long, int> Enrolments = new();

                private static IEnumerable<Measurement<long>> Observe()
                {
                    foreach (var e in Enrolments) yield return new Measurement<long>(e.Value);
                }
            }
            """;

        const string compliant = """
            internal static class ProbeMetrics
            {
                private static readonly Dictionary<long, int> Enrolments = new();

                internal static readonly ObservableGauge<long> Live =
                    Other.Meter.CreateObservableGauge("probe.live", Observe);

                private static IEnumerable<Measurement<long>> Observe()
                {
                    foreach (var e in Enrolments) yield return new Measurement<long>(e.Value);
                }
            }
            """;

        var bad = Analyse(violating);
        var good = Analyse(compliant);

        Assert.Multiple(() =>
        {
            Assert.That(bad.Violations, Has.Count.EqualTo(1),
                "A field read only inside the method group callback must still be checked.");
            Assert.That(bad.Violations[0].Read.Field, Is.EqualTo("Enrolments"));
            Assert.That(bad.Violations[0].Read.Via, Is.EqualTo("Observe"),
                "An indirect read must name the method it was reached through.");

            // Observe() itself sits below the instrument in BOTH arms. The
            // compliant arm proves that is not what is being measured.
            Assert.That(good.Violations, Is.Empty,
                "A method declared below the instrument is not a violation - only a field is.");
            Assert.That(good.FieldReadsResolved, Is.EqualTo(1),
                "The compliant arm must still resolve the read through Observe.");
        });
    }

    /// <summary>
    /// Proves that source the analysis cannot see through is reported as a
    /// <b>vacuous scan</b> and not as a clean one, with a distinct outcome from
    /// an ordering violation.
    /// </summary>
    /// <remarks>
    /// Per issue #3052 note 3: two different defects that produce the same
    /// reported reason mean at least one of them is not being detected. An empty
    /// or unparseable file and a correctly-ordered file both yield zero
    /// violations, so violation-emptiness alone cannot tell them apart. The
    /// counters are what separate them, and this arm asserts on the counters.
    /// </remarks>
    [Test]
    public void Analysis_reports_an_unscannable_source_as_vacuous_rather_than_clean()
    {
        var empty = Analyse(string.Empty);
        var noObservables = Analyse("internal static class Plain { private static int _x = 1; }");
        var unreadable = Analyse("this is not C# at all {{{");

        var compliant = Analyse("""
            internal static class ProbeMetrics
            {
                private static int _depth = 7;

                internal static readonly ObservableGauge<int> Depth =
                    Other.Meter.CreateObservableGauge("probe.depth", static () => _depth);
            }
            """);

        Assert.Multiple(() =>
        {
            // All four have no violations. That is exactly why violation-count
            // alone is not a sufficient assertion.
            Assert.That(empty.Violations, Is.Empty);
            Assert.That(noObservables.Violations, Is.Empty);
            Assert.That(unreadable.Violations, Is.Empty);
            Assert.That(compliant.Violations, Is.Empty);

            // The counters separate them, and only the last is genuinely clean.
            Assert.That(empty.ObservablesScanned, Is.Zero,
                "Empty source must scan nothing, so the repository arm's first anti-vacuity assertion fires.");
            Assert.That(noObservables.ObservablesScanned, Is.Zero);
            Assert.That(unreadable.ObservablesScanned, Is.Zero,
                "Unparseable source must not be silently counted as scanned.");

            Assert.That(compliant.ObservablesScanned, Is.EqualTo(1),
                "The genuinely clean source must be distinguishable from the unscannable ones by the counter.");
            Assert.That(compliant.FieldReadsResolved, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// Proves the second anti-vacuity arm is reachable: an observable whose
    /// callback reads no same-type static field scans as an instrument but
    /// resolves no field read.
    /// </summary>
    /// <remarks>
    /// This is the state the repository arm must refuse to call clean. Without
    /// it, a break in identifier resolution would present as
    /// <c>observablesScanned &gt; 0</c>, <c>violations empty</c>, green - and
    /// the ordering would go unchecked entirely.
    /// </remarks>
    [Test]
    public void Analysis_distinguishes_an_instrument_with_no_resolvable_field_read()
    {
        var result = Analyse("""
            internal static class ProbeMetrics
            {
                internal static readonly ObservableGauge<int> Constant =
                    Other.Meter.CreateObservableGauge("probe.constant", static () => 1);
            }
            """);

        Assert.Multiple(() =>
        {
            Assert.That(result.ObservablesScanned, Is.EqualTo(1),
                "The instrument must still be counted as scanned.");
            Assert.That(result.FieldReadsResolved, Is.Zero,
                "A callback reading no same-type static field must resolve no read, which is what makes the "
                + "repository arm's second anti-vacuity assertion meaningful.");
            Assert.That(result.Violations, Is.Empty);
        });
    }

    /// <summary>
    /// Analyses one compilation unit for the generalised ordering invariant.
    /// Shared by the repository scan and by the synthetic arms that prove it
    /// detects what it claims to.
    /// </summary>
    private static ScanResult Analyse(string source)
    {
        var root = CSharpSyntaxTree.ParseText(source).GetRoot();
        var violations = new List<Violation>();
        var observablesScanned = 0;
        var fieldReadsResolved = 0;

        foreach (var type in root.DescendantNodes().OfType<TypeDeclarationSyntax>())
        {
            var fieldLines = StaticFieldLines(type);
            var methods = type.Members
                .OfType<MethodDeclarationSyntax>()
                .GroupBy(m => m.Identifier.ValueText, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);

            foreach (var field in type.Members.OfType<FieldDeclarationSyntax>())
            {
                if (!IsObservableInstrumentType(field.Declaration.Type)) continue;

                var instrumentLine = LineOf(field);

                foreach (var variable in field.Declaration.Variables)
                {
                    if (variable.Initializer is null) continue;

                    observablesScanned++;

                    var reads = CollectStaticFieldReads(
                        variable.Initializer.Value, fieldLines, methods, variable.Identifier.ValueText);

                    fieldReadsResolved += reads.Count;

                    foreach (var read in reads.Where(r => r.FieldLine > instrumentLine))
                    {
                        violations.Add(new Violation(
                            type.Identifier.ValueText, variable.Identifier.ValueText, instrumentLine, read));
                    }
                }
            }
        }

        return new ScanResult(observablesScanned, fieldReadsResolved, violations);
    }

    /// <summary>
    /// Maps every non-<c>const</c> static field declared directly on
    /// <paramref name="type"/> to its 1-based declaration line.
    /// </summary>
    /// <remarks>
    /// <c>const</c> is excluded because a compile-time constant is inlined at
    /// every use site and has no initialiser to be caught part-way through, so
    /// its position cannot matter. Including it would produce false positives
    /// against real code - <c>LatticeBackupMetrics</c> reads the
    /// <c>const string TagScope</c> from inside a gauge callback.
    /// </remarks>
    private static Dictionary<string, int> StaticFieldLines(TypeDeclarationSyntax type)
    {
        var lines = new Dictionary<string, int>(StringComparer.Ordinal);

        foreach (var field in type.Members.OfType<FieldDeclarationSyntax>())
        {
            if (field.Modifiers.Any(SyntaxKind.ConstKeyword)) continue;
            if (!field.Modifiers.Any(SyntaxKind.StaticKeyword)) continue;

            var line = LineOf(field);
            foreach (var variable in field.Declaration.Variables)
            {
                lines[variable.Identifier.ValueText] = line;
            }
        }

        return lines;
    }

    /// <summary>
    /// Collects the same-type static fields reachable from
    /// <paramref name="start"/>, following same-type method bodies transitively.
    /// </summary>
    /// <remarks>
    /// Identifiers on the right of a member access (<c>x.Name</c>) are skipped:
    /// they name a member of some other type, and matching them against this
    /// type's field names would fabricate reads. The instrument's own name is
    /// skipped so a self-reference cannot be reported against itself.
    /// </remarks>
    private static List<FieldRead> CollectStaticFieldReads(
        SyntaxNode start,
        IReadOnlyDictionary<string, int> fieldLines,
        IReadOnlyDictionary<string, List<MethodDeclarationSyntax>> methods,
        string instrumentName)
    {
        var found = new Dictionary<string, FieldRead>(StringComparer.Ordinal);
        var visitedMethods = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Queue<(SyntaxNode Node, string Via)>();
        pending.Enqueue((start, string.Empty));

        while (pending.Count > 0)
        {
            var (node, via) = pending.Dequeue();

            foreach (var identifier in node.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>())
            {
                if (identifier.Parent is MemberAccessExpressionSyntax access && access.Name == identifier) continue;

                var name = identifier.Identifier.ValueText;
                if (string.Equals(name, instrumentName, StringComparison.Ordinal)) continue;

                if (fieldLines.TryGetValue(name, out var fieldLine))
                {
                    if (!found.ContainsKey(name)) found[name] = new FieldRead(name, fieldLine, via);
                    continue;
                }

                if (!methods.TryGetValue(name, out var overloads) || !visitedMethods.Add(name)) continue;

                foreach (var overload in overloads)
                {
                    SyntaxNode? body = overload.Body;
                    body ??= overload.ExpressionBody;
                    if (body is null) continue;

                    pending.Enqueue((body, via.Length == 0 ? name : $"{via} -> {name}"));
                }
            }
        }

        return [.. found.Values];
    }

    /// <summary>
    /// Reports whether <paramref name="type"/> names an observable instrument,
    /// unwrapping any qualification (<c>Metrics.ObservableGauge&lt;int&gt;</c>).
    /// </summary>
    private static bool IsObservableInstrumentType(TypeSyntax type)
    {
        var candidate = type;
        while (candidate is QualifiedNameSyntax qualified) candidate = qualified.Right;

        return candidate is GenericNameSyntax generic
            && ObservableInstrumentNames.Contains(generic.Identifier.ValueText);
    }

    /// <summary>The 1-based line the node's own text starts on, excluding leading trivia.</summary>
    private static int LineOf(SyntaxNode node) =>
        node.GetLocation().GetLineSpan().StartLinePosition.Line + 1;

    /// <summary>
    /// Enumerates the generic observable instrument type names the running
    /// framework exposes, by walking each exported type's base chain to
    /// <see cref="Instrument"/>.
    /// </summary>
    private static HashSet<string> DiscoverObservableInstrumentTypeNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var type in typeof(Meter).Assembly.GetExportedTypes())
        {
            if (!type.IsGenericTypeDefinition || type.IsAbstract) continue;

            for (var current = type.BaseType; current is not null; current = current.BaseType)
            {
                if (current != typeof(Instrument)) continue;

                var tick = type.Name.IndexOf('`');
                var name = tick < 0 ? type.Name : type.Name[..tick];
                if (name.StartsWith("Observable", StringComparison.Ordinal)) names.Add(name);
                break;
            }
        }

        return names;
    }
}
