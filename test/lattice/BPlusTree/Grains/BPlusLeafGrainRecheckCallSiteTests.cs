using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the call sites of <c>BPlusLeafGrain.MaybeRunPeriodicSnapshotRecheckAsync</c>
/// against the doc comment on <c>OnCoverageLagTimerTickAsync</c>, which names
/// them by symbol (issue #3222). A count-based claim about these call sites
/// went stale twice and was read as a live invariant, so the list is scanned
/// from source: adding or removing a driver fails this fixture until the
/// comment is updated to match.
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainRecheckCallSiteTests
{
    private const string RecheckMethod = "MaybeRunPeriodicSnapshotRecheckAsync";

    private static readonly string[] ExpectedCallers =
    [
        "BankDurablePinCoreAsync",
        "CompleteCheckpointFlushTailAsync",
        "CompleteDeactivationCheckpointFlushTailAsync",
        "DriveStarvedCheckpointCoreAsync",
    ];

    private static readonly Regex MethodDeclaration = new(
        @"^\s*(?:private|internal|public|protected)\b[^=;]*?\s(?<name>\w+)\s*(?:<[^>]*>)?\s*\(",
        RegexOptions.Compiled);

    private static readonly string[] StaleCountPhrases =
    [
        "exactly ONE invocation",
        "second invocation",
        "is the third",
    ];

    /// <summary>
    /// Every non-comment invocation of the recheck in the leaf grain's
    /// partials sits inside exactly the methods the doc comment names.
    /// </summary>
    [Test]
    public void Recheck_call_sites_match_the_documented_callers()
    {
        var callers = FindEnclosingCallers();

        Assert.That(callers, Is.EquivalentTo(ExpectedCallers));
    }

    /// <summary>
    /// The <c>OnCoverageLagTimerTickAsync</c> doc comment cites every caller
    /// by <c>cref</c> and carries none of the stale count-based claims.
    /// </summary>
    [Test]
    public void Timer_tick_doc_comment_names_every_caller_and_no_count()
    {
        var comment = ReadTimerTickDocComment();

        Assert.Multiple(() =>
        {
            foreach (var caller in ExpectedCallers)
            {
                Assert.That(comment, Does.Contain($"cref=\"{caller}\""), caller);
            }

            foreach (var phrase in StaleCountPhrases)
            {
                Assert.That(comment, Does.Not.Contain(phrase).IgnoreCase, phrase);
            }
        });
    }

    private static IReadOnlyList<string> FindEnclosingCallers()
    {
        var files = Directory.GetFiles(GrainsDirectory(), "BPlusLeafGrain*.cs");
        Assert.That(files, Is.Not.Empty, "the leaf grain partials were not found");

        var callers = new List<string>();
        foreach (var file in files)
        {
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                if (line.TrimStart().StartsWith("//", StringComparison.Ordinal)
                    || !line.Contains(RecheckMethod + "(", StringComparison.Ordinal)
                    || line.Contains("Task " + RecheckMethod + "(", StringComparison.Ordinal))
                {
                    continue;
                }

                callers.Add(FindEnclosingMethod(lines, i, file));
            }
        }

        return callers;
    }

    private static string FindEnclosingMethod(string[] lines, int callLine, string file)
    {
        for (var j = callLine - 1; j >= 0; j--)
        {
            var match = MethodDeclaration.Match(lines[j]);
            if (match.Success && !lines[j].TrimStart().StartsWith("//", StringComparison.Ordinal))
            {
                return match.Groups["name"].Value;
            }
        }

        Assert.Fail($"No enclosing method found for the call at {Path.GetFileName(file)}:{callLine + 1}.");
        return string.Empty;
    }

    private static string ReadTimerTickDocComment()
    {
        var path = Path.Combine(GrainsDirectory(), "BPlusLeafGrain.Snapshot.cs");
        var lines = File.ReadAllLines(path);
        var declaration = Array.FindIndex(
            lines,
            l => l.Contains("Task OnCoverageLagTimerTickAsync(", StringComparison.Ordinal));
        Assert.That(declaration, Is.GreaterThan(0), "OnCoverageLagTimerTickAsync was not found");

        var start = declaration;
        while (start > 0 && lines[start - 1].TrimStart().StartsWith("///", StringComparison.Ordinal))
        {
            start--;
        }

        Assert.That(start, Is.LessThan(declaration), "OnCoverageLagTimerTickAsync has no doc comment");
        return string.Join('\n', lines[start..declaration]);
    }

    private static string GrainsDirectory() =>
        Path.Combine(HygieneRepository.FindRepoRoot(), "src", "lattice", "BPlusTree", "Grains");
}
