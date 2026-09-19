using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Classifies the empty-state shape of every observable instrument declared under <c>src/</c>:
/// whether its callback seeds zero samples from a retained collection, enumerates only live
/// state, or reads a collection whose retention this parser cannot see.
/// </summary>
/// <remarks>
/// <para>
/// The defect class this addresses is the epic's signature failure: an absent series because
/// nothing ever wired the instrument is byte-identical, at the query, to an absent series
/// because the measured quantity was genuinely empty. For a counter, the existing
/// <c>InstrumentPrimingEnrolment.tsv</c> gate answers this. For an observable instrument it
/// cannot, and the reason is structural rather than an oversight: that gate recognises a prime
/// only as a literal zero in the first argument of an <c>.Add(</c> or <c>.Record(</c> call, and
/// an observable instrument has no <c>.Add(</c> site anywhere. There is nothing there to widen.
/// </para>
/// <para>
/// A caution that is load-bearing, because it was nearly got wrong twice while this fixture was
/// being specified. The <c>enrolment</c> column of that TSV is NOT blind to priming; it measures
/// a different property - per-value zero-priming of a <em>bounded tag domain</em> - and its
/// <c>none</c> verdict is the correct answer for a tree-tagged instrument rather than a missing
/// measurement. Reading that column as a negative about observable priming is an inference from
/// an absence produced by a detector answering a different question. This fixture therefore adds
/// a new, separate observation and deliberately does not touch that column.
/// </para>
/// <para>
/// WHAT THIS FIXTURE DOES NOT DO, AND CANNOT. It does not decide whether an instrument
/// <em>ought</em> to be primed. That is undecidable from source here, and the proof is in the
/// ground-truth arms below: <see cref="LatticeMetrics.LeafSplitCompletionsInFlight"/> and
/// <see cref="LatticeMetrics.LeafSplitCompletionOldestAge"/> enumerate the same transient
/// registry with no seeding in either, are structurally identical to a parser, and are
/// semantically opposite - the first is a defect (a level whose zero is meaningful and is never
/// published) and the second is correct by design (the age of the oldest waiter when there is no
/// waiter is undefined, not zero). A classifier asked to separate them must either guess or
/// return the same answer for both, and returning the same answer is the honest outcome. The
/// property that separates them must be DECLARED and falsified by a parser, never DERIVED by
/// one. This fixture supplies only the falsifiable half.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ObservableInstrumentEmptyStateClassifierTests
{
    /// <summary>
    /// The mechanically observable empty-state shape of an observable instrument's callback.
    /// Each value is a claim this parser can be contradicted on; there is deliberately no value
    /// meaning "exempt", because a vocabulary containing an unfalsifiable verdict reports
    /// coverage it never measured.
    /// </summary>
    internal enum EmptyStateShape
    {
        /// <summary>
        /// The callback assigns a literal zero through an indexer, inside a loop over a
        /// collection that has no removal site in its declaring file, and then overlays live
        /// state. Every key the retained collection has ever seen is therefore sampled on every
        /// observation, so a drained tree reports a measured <c>0</c> rather than vanishing.
        /// </summary>
        Seeded,

        /// <summary>
        /// Every collection the callback enumerates is read directly, and none of them is seeded
        /// with zeros. The instrument reports no series at all for a key with nothing live. This
        /// is a statement about shape only - it is NOT a claim that the instrument is defective,
        /// because for an undefined-when-empty quantity it is the correct shape.
        /// </summary>
        LiveOnly,

        /// <summary>
        /// The callback could not be resolved, or it enumerates the result of a method call, so
        /// the contents of the enumerated sequence are produced by code this parser does not
        /// read. Retention is unknowable from here. This verdict names the blind spot rather
        /// than laundering it into a negative, which is the convention
        /// <c>InstrumentPrimingEnrolmentTests</c> already established with its own
        /// <c>Unresolved</c> value.
        /// </summary>
        Unresolved,
    }

    // Floors, not non-zero checks. A scan that silently narrows returns a smaller clean number
    // rather than an error, so the only thing separating "measured few" from "matched almost
    // nothing" is a floor set below the observed population and far above zero. At the time of
    // writing the scan resolves 21 observable instruments across the repository.
    private const int MinimumObservableDeclarations = 15;

    // The load-bearing non-vacuity floor. "Zero instruments classified Seeded" is exactly the
    // reading that looks like data and means "this dimension was never measured" - the failure
    // this fixture exists because of. It must fail loudly rather than pass quietly.
    private const int MinimumSeededInstruments = 1;

    private const string PermitWaitsInFlight = "WalReplayPermitWaitsInFlight";
    private const string LeafSplitInFlight = "LeafSplitCompletionsInFlight";
    private const string LeafSplitOldestAge = "LeafSplitCompletionOldestAge";
    private const string ScopeLastRunStatus = "ScopeLastRunStatus";

    /// <summary>
    /// The package the cross-package positive control lives in. Asserted to be reached by the
    /// scan in its own right: an instrument outside the scan's scope is not a control, it is a
    /// decoration, and it fails in the direction that looks like a classifier bug rather than a
    /// scope bug.
    /// </summary>
    private const string CrossPackageControlDirectory = "lattice.backup";

    private static readonly string[] ObservableFactoryNames =
    {
        "CreateObservableGauge",
        "CreateObservableCounter",
        "CreateObservableUpDownCounter",
    };

    private static readonly Lazy<ClassificationScan> Scan = new(ClassificationScan.Run);

    [Test]
    public void Observable_instrument_scan_is_not_vacuous()
    {
        var scan = Scan.Value;

        Assert.That(
            scan.FilesScanned,
            Is.GreaterThan(0),
            "The source scan read no files at all. A repository-wide gate that matches nothing "
            + "reports green over an unexamined repository, which is worse than having no gate.");

        Assert.That(
            scan.Instruments,
            Is.Not.Empty,
            "The observable-instrument scan matched no declarations. A guard that resolves zero "
            + "names is byte-identical to a clean repository, so this assertion exists to make "
            + "the guard's own crossing loud rather than silently green.");

        Assert.That(
            scan.Instruments.Count,
            Is.GreaterThanOrEqualTo(MinimumObservableDeclarations),
            $"Only {scan.Instruments.Count} observable instrument declarations were resolved, "
            + $"below the floor of {MinimumObservableDeclarations}. Either the factory-name list "
            + "or the declaration shape recognised by the parser has narrowed, or instruments "
            + "were removed wholesale. Both produce a smaller clean number rather than an error, "
            + "which is why this is a floor and not a non-zero check.");
    }

    [Test]
    public void Scan_reaches_the_package_holding_the_cross_package_control()
    {
        var scan = Scan.Value;

        Assert.That(
            scan.PackagesReached,
            Does.Contain(CrossPackageControlDirectory),
            $"The scan never read any file under src/{CrossPackageControlDirectory}/. The "
            + "cross-package positive control lives there, and an instrument the scan never "
            + "reaches is never classified - so an arm asserting its verdict would fail as "
            + "though the classifier were wrong, when the real fault is that the scope excluded "
            + "it. Packages actually reached: "
            + string.Join(", ", scan.PackagesReached.OrderBy(p => p, StringComparer.Ordinal)));
    }

    [Test]
    public void Every_ground_truth_instrument_is_in_scope()
    {
        var scan = Scan.Value;

        // Asserted individually, naming the missing one. Absent and Unresolved are different
        // verdicts and only one of them is an arm passing; an arm that cannot tell them apart
        // would report a scope failure as a classifier failure.
        foreach (var name in new[]
                 {
                     PermitWaitsInFlight, LeafSplitInFlight, LeafSplitOldestAge, ScopeLastRunStatus,
                 })
        {
            Assert.That(
                scan.Instruments.Any(i => i.Field == name),
                Is.True,
                $"The ground-truth instrument '{name}' was not found by the scan at all. It is "
                + "absent from the classified population rather than classified wrongly, so "
                + "every verdict asserted about it below would be vacuous. Either the "
                + "instrument was renamed or removed, or the scan's scope no longer reaches its "
                + "file.");
        }
    }

    [Test]
    public void Classifier_recognises_seeded_priming_on_an_observable_instrument()
    {
        var seeded = Scan.Value.Instruments.Where(i => i.Shape == EmptyStateShape.Seeded).ToList();

        Assert.That(
            seeded.Count,
            Is.GreaterThanOrEqualTo(MinimumSeededInstruments),
            "No observable instrument was classified Seeded. This is the reading that looks "
            + "like data and means the dimension was never measured: a classifier that can "
            + "never return Seeded answers every question with the same value and is "
            + "indistinguishable from one that is simply silent. Whatever else this fixture "
            + "asserts, it is worthless if this count is zero, which is why it fails here "
            + "rather than reporting a clean sweep of negatives.");
    }

    [Test]
    public void Classifier_returns_more_than_one_verdict()
    {
        var shapes = Scan.Value.Instruments.Select(i => i.Shape).Distinct().ToList();

        // Pins the classifier from both sides. A classifier that returns Seeded for everything,
        // or Unresolved for everything, satisfies any single-sided arm; only requiring genuine
        // divergence catches the degenerate cases that a positive-and-negative pair cannot.
        Assert.That(
            shapes.Count,
            Is.GreaterThan(1),
            "Every observable instrument received the same verdict ("
            + string.Join(", ", shapes)
            + "). A uniformly-answering classifier passes every one-sided check while "
            + "discriminating nothing, and is exactly the degenerate outcome the existing "
            + "enrolment column already produces for this population.");
    }

    [Test]
    public void Permit_wait_in_flight_gauge_classifies_as_seeded()
    {
        AssertShape(
            PermitWaitsInFlight,
            EmptyStateShape.Seeded,
            "Its callback seeds a literal zero for every key of WalReplayPermitWaitObservedTrees "
            + "- a collection that is only ever added to - and then overlays the live registry, "
            + "so a tree that has queued at least once keeps reporting a measured zero after "
            + "draining rather than vanishing from the scrape.");
    }

    [Test]
    public void Leaf_split_in_flight_gauge_classifies_as_live_only()
    {
        AssertShape(
            LeafSplitInFlight,
            EmptyStateShape.LiveOnly,
            "Its callback enumerates only the transient LeafSplitCompletionsInFlightRegistry, "
            + "whose entries are removed on Dispose, and seeds nothing. Note carefully what this "
            + "verdict does and does not say: it records the SHAPE, and takes no position on "
            + "whether that shape is correct for this instrument. Whether a drained tree ought "
            + "to publish a measured zero here is a semantic question this parser cannot answer "
            + "- see the sibling age-gauge arm, which is structurally identical and correct.");
    }

    [Test]
    public void Leaf_split_oldest_age_gauge_classifies_as_live_only()
    {
        AssertShape(
            LeafSplitOldestAge,
            EmptyStateShape.LiveOnly,
            "This is the arm that makes the fixture honest, and it must NEVER be 'improved' into "
            + "Seeded. The age of the oldest waiter when there is no waiter is undefined, not "
            + "zero, so priming it would publish the healthiest possible value for the emptiest "
            + "possible state. It is structurally identical to the in-flight count gauge above - "
            + "same registry, same absence of seeding - and semantically opposite to it. That a "
            + "single classifier returns the same verdict for both is the correct behaviour and "
            + "the direct evidence that 'should this be primed' is undecidable from source.");
    }

    [Test]
    public void Backup_scope_last_run_status_gauge_classifies_as_unresolved()
    {
        AssertShape(
            ScopeLastRunStatus,
            EmptyStateShape.Unresolved,
            "READ THIS BEFORE 'FIXING' IT. ScopeLastRunStatus IS primed (issue #2645): a scope "
            + "enters the registry at schedule registration and its zero means 'scheduled, "
            + "nothing has completed yet'. Unresolved is nevertheless the correct and deliberate "
            + "verdict, because that priming is achieved by RETENTION inside "
            + "BackupInventoryRegistry - a different type in a different file - and is invisible "
            + "at the callback, which enumerates Registry.EnumerateScopes() and contains no "
            + "literal zero anywhere. Making this parser clever enough to answer Seeded here "
            + "would mean guessing about the retention policy of a type it does not read, "
            + "trading a named blind spot for an unfalsifiable positive claim, which is strictly "
            + "worse. A change that turns this arm green as Seeded is a defect unless the parser "
            + "has genuinely acquired cross-file retention analysis. This mirrors the precedent "
            + "in InstrumentPrimingEnrolmentTests, where WalGcBlockedLeafReactivations is "
            + "enrolled Unresolved while documented as fully primed.");
    }

    [Test]
    public void Seeding_source_for_the_permit_wait_gauge_is_never_emptied()
    {
        var instrument = Scan.Value.Instruments.Single(i => i.Field == PermitWaitsInFlight);

        // Corroborates what Seeded is asserting. Seeding zeros from a collection that is itself
        // drained would publish nothing for a drained tree after all, so the verdict would be
        // true of the syntax and false of the behaviour.
        Assert.That(
            instrument.SeedSources,
            Is.Not.Empty,
            "No seeding source was recorded for the instrument classified Seeded, so the verdict "
            + "rests on nothing this fixture can show.");

        foreach (var source in instrument.SeedSources)
        {
            Assert.That(
                ClassificationScan.HasRemovalSite(instrument.FileText, source),
                Is.False,
                $"The collection '{source}' that seeds the zero samples for {PermitWaitsInFlight} "
                + "has acquired a removal site. Seeding from a collection that can be drained "
                + "publishes zeros only for keys it still holds, so the instrument would silently "
                + "revert to dropping drained trees while this fixture still reported Seeded.");
        }
    }

    private static void AssertShape(string field, EmptyStateShape expected, string because)
    {
        var matches = Scan.Value.Instruments.Where(i => i.Field == field).ToList();

        Assert.That(
            matches.Count,
            Is.EqualTo(1),
            $"Expected exactly one observable instrument named '{field}', found {matches.Count}. "
            + "A name resolving to zero declarations is measured absence dressed as a clean "
            + "result; a name resolving to several means the verdict below is ambiguous.");

        Assert.That(
            matches[0].Shape,
            Is.EqualTo(expected),
            $"{field} classified {matches[0].Shape}, expected {expected}. {because}");
    }

    /// <summary>One observable instrument declaration and the verdict reached for it.</summary>
    internal sealed record ObservableInstrument(
        string Field,
        string RelativePath,
        EmptyStateShape Shape,
        IReadOnlyList<string> SeedSources,
        string FileText);

    /// <summary>The whole-repository scan, run once and cached.</summary>
    internal sealed class ClassificationScan
    {
        private static readonly Regex DeclarationPattern = new(
            @"(?<field>[A-Za-z_]\w*)\s*=\s*(?:[A-Za-z_]\w*\s*\.\s*)*(?<factory>CreateObservable(?:Gauge|Counter|UpDownCounter))\s*(?:<[^>()]*>)?\s*\(",
            RegexOptions.Compiled);

        private static readonly Regex CallbackDeclarationPattern = new(
            @"IEnumerable\s*<\s*Measurement\s*<[^>]*>\s*>\s+(?<name>[A-Za-z_]\w*)\s*\(",
            RegexOptions.Compiled);

        private static readonly Regex ForEachPattern = new(@"foreach\s*\(", RegexOptions.Compiled);

        // An indexer assignment of a literal zero: counts[tree] = 0;
        // Deliberately NOT "any literal zero", which the leaf-split age callback would satisfy
        // with its `seconds = 0;` clamp - a plain scalar assignment that is a floor on a
        // computed value, not a seeded key. That near-miss was found while specifying this
        // fixture and is the reason the pattern requires an indexer.
        private static readonly Regex IndexerZeroAssignmentPattern = new(
            @"[A-Za-z_]\w*\s*\[[^\];]*\]\s*=\s*0\s*;", RegexOptions.Compiled);

        private ClassificationScan(
            int filesScanned,
            IReadOnlyCollection<string> packagesReached,
            IReadOnlyList<ObservableInstrument> instruments)
        {
            FilesScanned = filesScanned;
            PackagesReached = packagesReached;
            Instruments = instruments;
        }

        public int FilesScanned { get; }

        public IReadOnlyCollection<string> PackagesReached { get; }

        public IReadOnlyList<ObservableInstrument> Instruments { get; }

        public static ClassificationScan Run()
        {
            var root = HygieneRepository.FindRepoRoot();
            var sourceRoot = Path.Combine(root, "src");
            var instruments = new List<ObservableInstrument>();
            var packages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var files = 0;

            foreach (var file in HygieneRepository.EnumerateFiles(sourceRoot, "*.cs"))
            {
                files++;
                var relative = Path.GetRelativePath(sourceRoot, file).Replace('\\', '/');
                var package = relative.Split('/')[0];
                packages.Add(package);

                var text = File.ReadAllText(file);
                if (!ObservableFactoryNames.Any(name => text.Contains(name, StringComparison.Ordinal)))
                {
                    continue;
                }

                instruments.AddRange(ClassifyFile(text, relative));
            }

            return new ClassificationScan(files, packages, instruments);
        }

        /// <summary>
        /// True when the named collection has any site in the file that can remove from it.
        /// Exposed so an arm can corroborate what a Seeded verdict is claiming.
        /// </summary>
        public static bool HasRemovalSite(string text, string collection)
        {
            foreach (var verb in new[] { "TryRemove", "Remove", "Clear", "TryTake", "TryDequeue" })
            {
                if (Regex.IsMatch(text, $@"\b{Regex.Escape(collection)}\s*\.\s*{verb}\s*\("))
                {
                    return true;
                }
            }

            return false;
        }

        private static IEnumerable<ObservableInstrument> ClassifyFile(string text, string relative)
        {
            var callbacks = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (Match declaration in CallbackDeclarationPattern.Matches(text))
            {
                var name = declaration.Groups["name"].Value;
                var body = ExtractBody(text, declaration.Index + declaration.Length);
                if (body is not null && !callbacks.ContainsKey(name))
                {
                    callbacks[name] = body;
                }
            }

            foreach (Match declaration in DeclarationPattern.Matches(text))
            {
                var openParen = declaration.Index + declaration.Length - 1;
                var arguments = ExtractDelimited(text, openParen, '(', ')');
                if (arguments is null)
                {
                    continue;
                }

                var field = declaration.Groups["field"].Value;
                var callbackName = callbacks.Keys.FirstOrDefault(
                    name => Regex.IsMatch(arguments, $@"(?<![\w.]){Regex.Escape(name)}(?![\w(])"));

                if (callbackName is null)
                {
                    yield return new ObservableInstrument(
                        field, relative, EmptyStateShape.Unresolved, Array.Empty<string>(), text);
                    continue;
                }

                yield return Classify(field, relative, text, callbacks[callbackName]);
            }
        }

        private static ObservableInstrument Classify(
            string field, string relative, string fileText, string body)
        {
            var seedSources = new List<string>();

            foreach (var loop in EnumerateForEachLoops(body))
            {
                // A sequence produced by a method call is opaque: its retention lives in code
                // this parser does not read, so the honest verdict names the blind spot rather
                // than reporting a negative it cannot support.
                if (loop.Source.Contains('(', StringComparison.Ordinal))
                {
                    return new ObservableInstrument(
                        field, relative, EmptyStateShape.Unresolved, Array.Empty<string>(), fileText);
                }

                if (!IndexerZeroAssignmentPattern.IsMatch(loop.Body))
                {
                    continue;
                }

                var collection = RootIdentifier(loop.Source);
                if (collection is not null && !HasRemovalSite(fileText, collection))
                {
                    seedSources.Add(collection);
                }
            }

            return new ObservableInstrument(
                field,
                relative,
                seedSources.Count > 0 ? EmptyStateShape.Seeded : EmptyStateShape.LiveOnly,
                seedSources,
                fileText);
        }

        private static string? RootIdentifier(string source)
        {
            var match = Regex.Match(source.Trim(), @"^(?<root>[A-Za-z_]\w*)");
            return match.Success ? match.Groups["root"].Value : null;
        }

        private static IEnumerable<(string Source, string Body)> EnumerateForEachLoops(string body)
        {
            foreach (Match loop in ForEachPattern.Matches(body))
            {
                var headerOpen = loop.Index + loop.Length - 1;
                var header = ExtractDelimited(body, headerOpen, '(', ')');
                if (header is null)
                {
                    continue;
                }

                var separator = header.LastIndexOf(" in ", StringComparison.Ordinal);
                if (separator < 0)
                {
                    continue;
                }

                var source = header[(separator + 4)..].Trim();
                var afterHeader = headerOpen + header.Length + 2;
                var loopBody = ExtractBlockOrStatement(body, afterHeader);
                if (loopBody is not null)
                {
                    yield return (source, loopBody);
                }
            }
        }

        private static string? ExtractBody(string text, int afterSignatureOpenParen)
        {
            var signature = ExtractDelimited(text, afterSignatureOpenParen - 1, '(', ')');
            if (signature is null)
            {
                return null;
            }

            var cursor = afterSignatureOpenParen + signature.Length + 1;
            return ExtractBlockOrStatement(text, cursor);
        }

        private static string? ExtractBlockOrStatement(string text, int cursor)
        {
            while (cursor < text.Length && char.IsWhiteSpace(text[cursor]))
            {
                cursor++;
            }

            if (cursor >= text.Length)
            {
                return null;
            }

            if (text[cursor] == '{')
            {
                return ExtractDelimited(text, cursor, '{', '}');
            }

            var end = text.IndexOf(';', cursor);
            return end < 0 ? null : text[cursor..end];
        }

        /// <summary>
        /// Returns the text between a delimiter pair, respecting nesting and skipping string
        /// literals and comments. Skipping strings is not optional here: instrument descriptions
        /// routinely contain unbalanced parentheses, such as "(no series when none in flight)".
        /// </summary>
        private static string? ExtractDelimited(string text, int openIndex, char open, char close)
        {
            if (openIndex < 0 || openIndex >= text.Length || text[openIndex] != open)
            {
                return null;
            }

            var depth = 0;
            for (var i = openIndex; i < text.Length; i++)
            {
                var c = text[i];

                if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
                {
                    var newline = text.IndexOf('\n', i);
                    if (newline < 0)
                    {
                        return null;
                    }

                    i = newline;
                    continue;
                }

                if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
                {
                    var end = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                    if (end < 0)
                    {
                        return null;
                    }

                    i = end + 1;
                    continue;
                }

                if (c == '\'' || c == '"')
                {
                    i = SkipLiteral(text, i);
                    if (i < 0)
                    {
                        return null;
                    }

                    continue;
                }

                if (c == open)
                {
                    depth++;
                }
                else if (c == close)
                {
                    depth--;
                    if (depth == 0)
                    {
                        return text[(openIndex + 1)..i];
                    }
                }
            }

            return null;
        }

        private static int SkipLiteral(string text, int start)
        {
            var quote = text[start];
            var verbatim = start > 0 && text[start - 1] == '@' && quote == '"';

            for (var i = start + 1; i < text.Length; i++)
            {
                if (!verbatim && text[i] == '\\')
                {
                    i++;
                    continue;
                }

                if (text[i] != quote)
                {
                    continue;
                }

                if (verbatim && i + 1 < text.Length && text[i + 1] == quote)
                {
                    i++;
                    continue;
                }

                return i;
            }

            return -1;
        }
    }
}
