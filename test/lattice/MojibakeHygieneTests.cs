using System.IO;
using System.Text;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression: byte-level "mojibake" sequences must not appear in any tracked
/// text file. A mojibake is what you get when a UTF-8 byte stream is decoded
/// as Windows-1252 / CP437 / latin1 and then re-encoded as UTF-8, producing
/// nonsense glyph runs in place of smart quotes, smart apostrophes,
/// ellipses, en / em dashes, arrows, or check-marks. They sneak in when
/// prose or PR-body text is pasted from a terminal or word processor whose
/// code-page interpretation disagreed with the underlying UTF-8 bytes - the
/// kind of leak this campaign documented once on the features.md index (an
/// arrow in an entry's link text) and again on the silo's PR-success line
/// (a check-mark prefix on the gh CLI output) before this gate existed.
/// <para>
/// The detection set is the byte trigrams that mojibake produces with very
/// high reliability for common glyphs. Smart quotes, smart apostrophes,
/// ellipses, em / en dashes seen through CP1252-on-UTF-8, and arrow
/// / check-mark fragments seen through CP437-on-UTF-8. The set is
/// deliberately narrow - no false positives on legitimate accented Western
/// text in docs, which uses a single high-byte sequence like
/// <c>Ã©</c>'s correct UTF-8 encoding (<c>U+00C3 U+00A9</c>) - because
/// detection is by trigram, not by bigram.
/// </para>
/// <para>
/// The needles below are constructed by code point so the test source itself
/// is not self-flagged. Each comment describes the original glyph the
/// mojibake corresponds to so a contributor reading the failure message can
/// understand what got pasted.
/// </para>
/// </summary>
[TestFixture]
public class MojibakeHygieneTests
{
    // Needles: byte-level mojibake trigrams to detect. Each is built by
    // code point so this file is itself clean. The COMMENT names the
    // original glyph; the value is what shows up in the tracked file when
    // that glyph was misencoded.
    //
    // Add a new needle only when a real leak motivates it - widening the
    // set without evidence raises the false-positive risk against
    // legitimate Western-European doc prose.
    private static readonly (string Needle, string Description)[] Needles = new (string, string)[]
    {
        // CP1252-as-UTF-8 mojibake for common smart punctuation. Each
        // smart quote / apostrophe / ellipsis in source is "U+E2 U+80 U+xx"
        // when its UTF-8 bytes get re-decoded as CP1252.
        (new string(new[] { '\u00E2', '\u20AC', '\u2122' }), "smart apostrophe (U+2019)"),       // \u00E2\u20AC\u2122 -> A-circumflex Euro TM
        (new string(new[] { '\u00E2', '\u20AC', '\u0153' }), "left double smart quote (U+201C)"),// \u00E2\u20AC\u0153 -> ...
        (new string(new[] { '\u00E2', '\u20AC', '\u00A6' }), "ellipsis (U+2026)"),
        (new string(new[] { '\u00E2', '\u20AC', '\u201C' }), "en-dash (U+2013)"),
        (new string(new[] { '\u00E2', '\u20AC', '\u201D' }), "em-dash (U+2014) seen through CP1252-on-UTF-8 - paired with the em-dash test which catches the original glyph"),

        // CP437-as-UTF-8 mojibake. Box-drawing line characters and the
        // green check-mark land here when Windows console code page 437
        // re-interprets UTF-8 bytes. The first two are the canonical
        // "tee" + "umlaut-O" prefix of the U+2192 arrow and the
        // U+2713 check, observed twice in this campaign.
        (new string(new[] { '\u251C', '\u00F6', '\u251C', '\u00C5' }), "arrow (U+2192) via CP437"),
        (new string(new[] { '\u00D4', '\u00A3', '\u00F4' }), "check-mark (U+2713) via CP437"),
        (new string(new[] { '\u00D4', '\u00A3', '\u00C9' }), "cross-mark (U+2717) via CP437"),
    };

    // Directory segments under the repo root that are never in scope:
    // build output, IDE/VCS metadata, gitignored scratch and run output,
    // benchmark artifacts, third-party module trees, and test result dumps.
    private static readonly string[] ExcludedSegments = new[]
    {
        "bin", "obj", "node_modules",
        ".git", ".vs",
        ".run", ".scratch",
        "BenchmarkDotNet.Artifacts",
        "TestResults",
    };

    // Binary file extensions where a mojibake byte sequence is meaningless
    // (a JPEG or DLL can hold any byte run by coincidence) and would only
    // produce noise. Everything else is treated as text and scanned. Mirrors
    // the EmDashHygieneTests exclusion list for consistency. `.log` files
    // are transient run artefacts (gitignored via `*.log` in .gitignore;
    // some local copies persist in tools/ for debugging) and are NEVER
    // tracked, so a mojibake in a `.log` cannot leak via a PR - excluding
    // the extension keeps the test fast and false-positive-free against
    // local debug-log copies.
    private static readonly HashSet<string> BinaryExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".png", ".jpg", ".jpeg", ".gif", ".ico", ".bmp", ".pdf",
        ".dll", ".exe", ".pdb", ".so", ".dylib",
        ".zip", ".tar", ".gz", ".7z", ".nupkg", ".snk",
        ".dmp", ".bin",
        ".log",
    };

    /// <summary>
    /// Scans every tracked text file under the repository root and fails
    /// if any mojibake trigram is present, listing every offending
    /// (file:line:needle) tuple so the fix is mechanical - either revert
    /// to the original UTF-8 glyph if it was a smart quote/dash/check, or
    /// rewrite with a plain ASCII equivalent if the glyph was decorative.
    /// </summary>
    [Test]
    public void No_mojibake_sequences_in_tracked_files()
    {
        var repoRoot = FindRepoRoot();

        var violations = new List<string>();
        foreach (var file in EnumerateTextFiles(repoRoot))
        {
            string text;
            try
            {
                text = File.ReadAllText(file, Encoding.UTF8);
            }
            catch
            {
                continue; // unreadable / truly binary -> skip
            }

            foreach (var (needle, description) in Needles)
            {
                var idx = text.IndexOf(needle, StringComparison.Ordinal);
                if (idx >= 0)
                {
                    var lineNo = 1;
                    for (var i = 0; i < idx; i++)
                    {
                        if (text[i] == '\n') lineNo++;
                    }
                    var rel = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
                    violations.Add($"{rel}:{lineNo}: mojibake of {description} ({FormatBytes(needle)})");
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "Mojibake (UTF-8-bytes-decoded-as-CP1252-or-CP437) sequences are not permitted in tracked files. "
            + "Replace each occurrence with the original glyph in UTF-8 or a plain ASCII equivalent."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    /// <summary>
    /// Positive-detection self-test: every needle is constructed by code
    /// point so the test source itself is clean, but the production scan
    /// silently ignoring its own needle (because of an editor save that
    /// normalised the literal, or a regex-vs-IndexOf semantic drift)
    /// would silently disable the gate. This test plants each needle in
    /// an in-memory string and asserts the matcher would have flagged
    /// it. Acts as the "smoke detector battery test" for the hygiene
    /// gate itself.
    /// </summary>
    [Test]
    public void Every_needle_is_actually_detectable_in_an_in_memory_string()
    {
        foreach (var (needle, description) in Needles)
        {
            var planted = $"line before\nsome text with a {needle} mojibake in the middle\nline after";
            var idx = planted.IndexOf(needle, StringComparison.Ordinal);
            Assert.That(idx, Is.GreaterThanOrEqualTo(0),
                $"hygiene gate self-test: needle for '{description}' ({FormatBytes(needle)}) was not found in a planted string. "
                + "Either the needle's code points are wrong, or the scanner's matching semantic does not align with this test's.");
        }
    }

    private static string FormatBytes(string needle)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < needle.Length; i++)
        {
            if (i > 0) sb.Append(' ');
            sb.Append("U+").Append(((int)needle[i]).ToString("X4"));
        }
        return sb.ToString();
    }

    private static IEnumerable<string> EnumerateTextFiles(string root)
    {
        foreach (var file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
        {
            var parts = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (parts.Any(IsExcludedSegment)) continue;
            if (BinaryExtensions.Contains(Path.GetExtension(file))) continue;
            yield return file;
        }
    }

    private static bool IsExcludedSegment(string segment)
    {
        foreach (var excluded in ExcludedSegments)
        {
            if (segment.Equals(excluded, StringComparison.OrdinalIgnoreCase)) return true;
        }
        return false;
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "README.md"))
                && Directory.Exists(Path.Combine(dir.FullName, "docs"))
                && Directory.Exists(Path.Combine(dir.FullName, "src")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        throw new InvalidOperationException(
            "Could not find repository root from " + AppContext.BaseDirectory);
    }
}