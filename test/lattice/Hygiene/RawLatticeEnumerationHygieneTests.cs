using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Fails the build when a test enumerates a raw lattice grain stream -
/// <c>await foreach</c> over <c>KeysAsync(</c>, <c>EntriesAsync(</c>, or their
/// predicate forms - without a reviewed reason (issue #5282).
/// <para>
/// WHY THE RAW STREAM FLAKES. A raw grain stream against a real cluster can be
/// served by a <c>[StatelessWorker]</c> activation, or lose its enumerator to
/// idle reclamation, and then fails with an <c>EnumerationAbortedException</c>
/// ("the remote target does not have a record of this enumerator"). That is a
/// transport-level miss, not an assertion failure, so it reddens unrelated pull
/// requests under CI load. The recovering wrappers <c>ScanKeysAsync</c> /
/// <c>ScanEntriesAsync</c> re-open from the last key they yielded and keep the
/// ambient credential, so they return the same rows without the flake.
/// </para>
/// <para>
/// WHY IT MUST BE A GATE. #3355 swept the known occurrences once and the pattern
/// regrew: <c>KeysAsync</c> is the method that autocompletes and reads as the
/// obvious one, and its failure never reproduces locally. A reviewer has to
/// remember to look for it, so it is gated rather than documented.
/// </para>
/// <para>
/// THE ALLOW-LIST. Some tests exist to exercise the raw stream itself - its
/// cancellation, its access-gate filtering, its metrics, its consistency under
/// concurrent splits - or drive an in-process grain or a substitute where no
/// transport exists. Those keep the raw call and carry a
/// <c>// raw-enumeration-ok: &lt;reason&gt;</c> comment on one of the
/// <see cref="MarkerWindow"/> lines directly above the site. The reason is
/// mandatory: an empty marker is not a review.
/// </para>
/// <para>
/// WHAT IS NOT DETECTED. Only the inline shape - <c>await foreach</c> and the
/// raw call on the same line - is matched. A stream captured into a variable and
/// enumerated later is not, and neither is a call whose opening parenthesis sits
/// on a later line than <c>await foreach</c>; both are rare in this tree.
/// </para>
/// </summary>
[TestFixture]
public sealed class RawLatticeEnumerationHygieneTests
{
    /// <summary>The comment that admits a raw enumeration onto the reviewed allow-list.</summary>
    private const string Marker = "// raw-enumeration-ok:";

    /// <summary>
    /// How many lines above a site the marker may sit. Two lets one marker cover
    /// a pair of identical back-to-back enumerations without admitting a marker
    /// that belongs to an unrelated earlier statement.
    /// </summary>
    private const int MarkerWindow = 2;

    private const string GuardFile = "test/lattice/Hygiene/RawLatticeEnumerationHygieneTests.cs";

    /// <summary>
    /// Matches an inline raw enumeration. The leading dot is required, so the
    /// recovering <c>ScanKeysAsync</c> / <c>ScanEntriesAsync</c> wrappers do not match.
    /// </summary>
    private static readonly Regex RawEnumerationPattern = new(
        @"await\s+foreach\b.*\.(?<method>KeysAsync|EntriesAsync|KeysWherePredicateAsync|EntriesWherePredicateAsync)\s*\(",
        RegexOptions.Compiled);

    [Test]
    public void No_test_enumerates_a_raw_lattice_stream_without_a_reviewed_reason()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var violations = new List<string>();
        var examined = 0;
        var allowed = 0;
        var gate = new object();

        Parallel.ForEach(HygieneRepository.EnumerateFiles(repoRoot, "*.cs"), path =>
        {
            var relative = Path.GetRelativePath(repoRoot, path).Replace('\\', '/');
            if (!relative.StartsWith("test/", StringComparison.Ordinal)
                || string.Equals(relative, GuardFile, StringComparison.Ordinal))
            {
                return;
            }

            // Pre-filter on the raw bytes: spares a UTF-8 decode of every file
            // that never enumerates anything asynchronously.
            var bytes = File.ReadAllBytes(path);
            if (bytes.AsSpan().IndexOf("await foreach"u8) < 0)
            {
                return;
            }

            var lines = Encoding.UTF8.GetString(bytes).Split('\n');
            var findings = FindRawEnumerations(lines, out var allowedHere);

            lock (gate)
            {
                examined++;
                allowed += allowedHere;
                foreach (var finding in findings)
                {
                    violations.Add($"{relative}{finding}");
                }
            }
        });

        violations.Sort(StringComparer.Ordinal);

        HygieneDenominator.RequireExamined(
            examined,
            nameof(No_test_enumerates_a_raw_lattice_stream_without_a_reviewed_reason),
            "test C# files containing await foreach",
            "the C# files under test/");

        Assert.That(
            allowed,
            Is.GreaterThan(0),
            "The scan found no allow-listed raw enumeration at all, but the tree is known to keep several "
            + "(cancellation, metrics and access-gate subjects). The detector has stopped matching, so a clean "
            + "result would mean nothing.");

        Assert.That(
            violations,
            Is.Empty,
            "Raw lattice stream enumerations in tests flake with EnumerationAbortedException under CI load "
            + "(issue #5282). Use ScanKeysAsync / ScanEntriesAsync, which recover from the last key, or - when "
            + "the raw stream itself is the subject under test - add '" + Marker + " <reason>' on the line above. "
            + "Violations:" + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void Detector_flags_an_unmarked_raw_enumeration()
    {
        var lines = Lines(
            "var keys = new List<string>();",
            Raw("tree", "KeysAsync") + "keys.Add(k);");

        var findings = FindRawEnumerations(lines, out var allowedCount);

        Assert.That(findings, Has.Count.EqualTo(1));
        Assert.That(findings[0], Does.Contain(":2:").And.Contain("KeysAsync"));
        Assert.That(allowedCount, Is.Zero);
    }

    [Test]
    public void Detector_flags_a_marker_with_no_reason()
    {
        var lines = Lines(
            Marker,
            Raw("tree", "EntriesAsync") + "{ }");

        var findings = FindRawEnumerations(lines, out _);

        Assert.That(findings, Has.Count.EqualTo(1));
    }

    [Test]
    public void Detector_flags_a_marker_outside_the_window()
    {
        var lines = Lines(
            Marker + " cancellation is the subject",
            "var a = 1;",
            "var b = 2;",
            Raw("tree", "KeysAsync") + "{ }");

        var findings = FindRawEnumerations(lines, out _);

        Assert.That(findings, Has.Count.EqualTo(1));
    }

    [Test]
    public void Detector_admits_a_marked_site_and_ignores_the_recovering_wrappers()
    {
        var lines = Lines(
            "    " + Marker + " cancellation of the raw stream is the subject",
            "    " + Raw("tree", "KeysAsync") + "{ }",
            "    " + Raw("tree", "KeysAsync") + "{ }",
            Raw("tree", "ScanKeysAsync") + "{ }",
            Raw("tree", "ScanEntriesAsync") + "{ }",
            "// " + Raw("tree", "EntriesAsync"));

        var findings = FindRawEnumerations(lines, out var allowedCount);

        Assert.That(findings, Is.Empty);
        Assert.That(allowedCount, Is.EqualTo(2));
    }

    /// <summary>
    /// Returns one <c>:line: text</c> finding per unmarked raw enumeration, and
    /// counts the marked ones in <paramref name="allowedCount"/>.
    /// </summary>
    private static List<string> FindRawEnumerations(IReadOnlyList<string> lines, out int allowedCount)
    {
        var findings = new List<string>();
        allowedCount = 0;

        for (var i = 0; i < lines.Count; i++)
        {
            var trimmed = lines[i].Trim();
            if (trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith('*'))
            {
                continue;
            }

            var match = RawEnumerationPattern.Match(lines[i]);
            if (!match.Success)
            {
                continue;
            }

            if (HasReasonedMarker(lines, i))
            {
                allowedCount++;
            }
            else
            {
                findings.Add($":{i + 1}: {trimmed}");
            }
        }

        return findings;
    }

    private static bool HasReasonedMarker(IReadOnlyList<string> lines, int siteIndex)
    {
        for (var j = siteIndex - 1; j >= 0 && j >= siteIndex - MarkerWindow; j--)
        {
            var trimmed = lines[j].Trim();
            if (trimmed.StartsWith(Marker, StringComparison.Ordinal)
                && trimmed.Length > Marker.Length
                && !string.IsNullOrWhiteSpace(trimmed[Marker.Length..]))
            {
                return true;
            }
        }

        return false;
    }

    private static string Raw(string receiver, string method) =>
        string.Concat("await ", "foreach (var k in ", receiver, ".", method, "()) ");

    private static string[] Lines(params string[] lines) => lines;
}
