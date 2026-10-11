using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Enforces ASCII text with exact, justified Unicode line exceptions. The
/// existing em-dash and mojibake gates remain independent, unconditional checks.
/// </summary>
public abstract class AsciiHygieneTestsBase
{
    /// <summary>The exact tracked manifest containing reviewed Unicode exceptions.</summary>
    protected abstract string AllowlistRelativePath { get; }

    /// <summary>
    /// Scans the whole tracked text population, including every package rather
    /// than relying on per-package enrolment. The core CI content-gate job runs this fixture.
    /// </summary>
    [Test]
    public void Only_ASCII_or_explicitly_allowed_Unicode_in_tracked_text_files()
    {
        var root = HygieneRepository.FindRepoRoot();
        var allowances = UnicodeAllowance.Load(System.IO.Path.Combine(root, AllowlistRelativePath));
        Scan(root, HygieneFiles.EnumerateTextFiles(root,
            HygieneScanScope.ForCore(Array.Empty<string>(), Array.Empty<string>())), allowances);
    }

    /// <summary>
    /// Applies the production scanner to a file population. Unreadable files,
    /// an empty denominator, unapproved Unicode, and stale exceptions all fail.
    /// </summary>
    public static void Scan(string repoRoot, IEnumerable<string> files, IReadOnlyList<UnicodeAllowance> allowances)
    {
        ArgumentNullException.ThrowIfNull(repoRoot);
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(allowances);
        var remaining = new Dictionary<(string Path, string Digest), int>();
        foreach (var allowance in allowances)
            remaining.Add((allowance.Path, allowance.Digest), allowance.Count);

        var violations = new List<string>();
        var unreadable = new List<string>();
        var scanned = 0;
        foreach (var file in files)
        {
            var lines = HygieneFiles.TryReadLines(file, out var failure);
            if (lines is null)
            {
                unreadable.Add(failure!);
                continue;
            }
            scanned++;
            string? relative = null;
            for (var i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                var column = line.AsSpan().IndexOfAnyExceptInRange((char)0, (char)127);
                if (column < 0) continue;
                relative ??= System.IO.Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
                var key = (relative, UnicodeAllowance.DigestLine(line));
                if (remaining.TryGetValue(key, out var count) && count > 0)
                {
                    remaining[key] = count - 1;
                    continue;
                }
                var codePoint = char.IsSurrogatePair(line, column)
                    ? char.ConvertToUtf32(line, column) : line[column];
                violations.Add($"{relative}:{i + 1}:{column + 1}: unapproved U+{codePoint:X4}");
            }
        }

        HygieneDenominator.RequireExamined(scanned, nameof(AsciiHygieneTestsBase), "readable tracked text files", repoRoot);
        TestContext.Out.WriteLine($"ASCII hygiene examined {scanned} readable tracked text files; {allowances.Count} exact Unicode allowances.");
        Assert.That(unreadable, Is.Empty,
            "Tracked files could not be read; the ASCII gate reached no verdict on them."
            + Environment.NewLine + string.Join(Environment.NewLine, unreadable));
        Assert.That(violations, Is.Empty,
            "Author ASCII text, or add a reviewed exact-line exception to UnicodeAllowlist.tsv."
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
        Assert.That(remaining.Where(p => p.Value != 0).Select(p => $"{p.Key.Path}: {p.Key.Digest} ({p.Value} unused)"), Is.Empty,
            "Stale Unicode allowances must be removed or explicitly refreshed when their exact lines change.");
    }
}
