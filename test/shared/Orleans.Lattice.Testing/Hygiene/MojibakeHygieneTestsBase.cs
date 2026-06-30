using System.IO;
using System.Text;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Regression: byte-level "mojibake" sequences must not appear in any tracked
/// text file. A mojibake is what you get when a UTF-8 byte stream is decoded
/// as Windows-1252 / CP437 / CP850 and then re-encoded as UTF-8, producing
/// nonsense glyph runs in place of smart quotes, smart apostrophes,
/// ellipses, en / em dashes, arrows, or check-marks. They sneak in when
/// prose or PR-body text is pasted from a terminal or word processor whose
/// code-page interpretation disagreed with the underlying UTF-8 bytes.
/// <para>
/// The needles below are constructed by code point so the test source itself
/// is not self-flagged. Each comment describes the original glyph the
/// mojibake corresponds to so a contributor reading the failure message can
/// understand what got pasted. A concrete subclass supplies a
/// <see cref="HygieneScanScope"/> so the scan covers only that project's
/// slice (plus, for the core project, the repo-level files no package owns).
/// </para>
/// </summary>
public abstract class MojibakeHygieneTestsBase
{
    // Needles: byte-level mojibake trigrams to detect. Each is built by
    // code point so this file is itself clean. The COMMENT names the
    // original glyph; the value is what shows up in the tracked file when
    // that glyph was misencoded.
    //
    // Add a new needle only when a real leak motivates it - widening the
    // set without evidence raises the false-positive risk against
    // legitimate Western-European doc prose.
    private static readonly (string Needle, string Description)[] Needles =
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
        // re-interprets UTF-8 bytes.
        (new string(new[] { '\u251C', '\u00F6', '\u251C', '\u00C5' }), "arrow (U+2192) via CP437"),
        (new string(new[] { '\u00D4', '\u00A3', '\u00F4' }), "check-mark (U+2713) via CP437"),
        (new string(new[] { '\u00D4', '\u00A3', '\u00C9' }), "cross-mark (U+2717) via CP437"),

        // CP850-as-UTF-8 mojibake. CP850 is the Western European OEM
        // code page that ships as the default on en-US Windows command
        // prompts; it differs from CP437 in the upper half and produces a
        // DIFFERENT trigram for the same source glyph.
        (new string(new[] { '\u00D4', '\u00E5', '\u00C6' }), "arrow (U+2192) via CP850"),
    };

    /// <summary>The repository slice this fixture is responsible for scanning.</summary>
    protected abstract HygieneScanScope Scope { get; }

    /// <summary>
    /// Scans every tracked text file in this fixture's scope and fails if any
    /// mojibake trigram is present, listing every offending
    /// (file:line:needle) tuple so the fix is mechanical - either revert to
    /// the original UTF-8 glyph if it was a smart quote/dash/check, or rewrite
    /// with a plain ASCII equivalent if the glyph was decorative.
    /// </summary>
    [Test]
    public void No_mojibake_sequences_in_tracked_files()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var violations = new List<string>();
        foreach (var file in HygieneFiles.EnumerateTextFiles(repoRoot, Scope))
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
    /// silently ignoring its own needle would silently disable the gate.
    /// This test plants each needle in an in-memory string and asserts the
    /// matcher would have flagged it.
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
}
