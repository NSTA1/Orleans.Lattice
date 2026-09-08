using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Source-scanning guard for issue 2233: every bounded leaf walk on the shard
/// root must consult the page-fill ceiling from <em>inside</em> the loop, before
/// it awaits any further work.
/// <para>
/// The fix for 2233 is not a single call site. The ceiling is armed once, by
/// the guard, but it can only stop the walk if each of the walk loops reads it,
/// and there are seventeen of them spread over four partial files. A loop that
/// omits the check is silently exempt: it still terminates, still respects
/// every volume bound, and still passes its own tests, but it goes on issuing
/// leaf reads after its caller has been told to retry - which is precisely the
/// defect. Nothing in the type system or the compiler notices, so this scan is
/// the only thing that does.
/// </para>
/// <para>
/// The invariant checked is deliberately <em>ordering</em>, not presence: the
/// stand-down must precede the first <c>await</c> in the loop body. A check
/// that runs after the iteration's grain calls would let each abandoned
/// iteration pay for a leaf read before noticing the ceiling had fired, which
/// leaks exactly the work the fix exists to stop. Cancellation checks that
/// already sit above it are fine and are why "before the first await" is the
/// rule rather than "the first statement".
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanPageStandDownCoverageTests
{
    /// <summary>
    /// The number of guarded leaf walks at the time this guard was written.
    /// It is a floor, not an expectation: it exists so that a scan which
    /// silently stops matching (a renamed phase, a reshaped loop) fails loudly
    /// instead of passing vacuously over nothing. Raise it when a walk is
    /// added; lower it only when one is genuinely deleted.
    /// </summary>
    private const int KnownWalkCount = 17;

    private static readonly string[] ScannedFiles =
    {
        "ShardRootGrain.cs",
        "ShardRootGrain.Diagnostics.cs",
        "ShardRootGrain.ProjectionAdmin.cs",
    };

    private static readonly Regex WalkPhase = new(
        @"Phase\s*=\s*ScanPagePhase\.(LeafWalk|BaselineFold)\s*;",
        RegexOptions.Compiled);

    [Test]
    public void Every_leaf_walk_loop_stands_down_before_it_awaits_again()
    {
        var root = HygieneRepository.FindRepoRoot();
        var grainsDir = Path.Combine(
            root, "src", "lattice", "BPlusTree", "Grains");

        var walks = 0;
        var offenders = new List<string>();

        foreach (var name in ScannedFiles)
        {
            var path = Path.Combine(grainsDir, name);
            Assert.That(
                File.Exists(path),
                Is.True,
                $"{name} is missing. This guard scans it by name, so a rename " +
                "must update ScannedFiles or the scan goes vacuous.");

            var lines = File.ReadAllLines(path);
            foreach (Match phase in WalkPhase.Matches(File.ReadAllText(path)))
            {
                var phaseLine = LineOf(lines, phase.Index);
                if (!TryFindWalkBody(lines, phaseLine, out var bodyStart, out var bodyEnd))
                {
                    offenders.Add(
                        $"{name}:{phaseLine + 1} arms a walk phase but no braced " +
                        "loop body follows it, so this guard cannot check it.");
                    continue;
                }

                walks++;
                if (!StandsDownBeforeFirstAwait(lines, bodyStart, bodyEnd))
                {
                    offenders.Add(
                        $"{name}:{bodyStart + 1} is a bounded leaf walk whose body " +
                        "awaits before calling StandDownIfCeilingFired(scan). The " +
                        "page-fill ceiling cannot stop this walk, so it keeps " +
                        "reading leaves after its caller was told to retry " +
                        "(issue 2233).");
                }
            }
        }

        Assert.That(
            offenders,
            Is.Empty,
            "Leaf walks that do not consult the page-fill ceiling:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, offenders));

        Assert.That(
            walks,
            Is.GreaterThanOrEqualTo(KnownWalkCount),
            $"Only {walks} leaf walks were found, below the known floor of " +
            $"{KnownWalkCount}. The scan has stopped matching what it is meant " +
            "to check, so its silence is meaningless. Fix the scan rather than " +
            "lowering the floor, unless a walk really was deleted.");
    }

    private static int LineOf(string[] lines, int charIndex)
    {
        var seen = 0;
        for (var i = 0; i < lines.Length; i++)
        {
            seen += lines[i].Length + Environment.NewLine.Length;
            if (seen > charIndex) return i;
        }
        return lines.Length - 1;
    }

    /// <summary>
    /// Finds the braced body of the first real loop at or after
    /// <paramref name="from"/>. Brace-less single-statement loops are skipped:
    /// they cannot hold a walk (the dispatch primer ahead of the baseline
    /// fold's ring buffer is one), so treating them as the walk body would
    /// report a false offender.
    /// </summary>
    private static bool TryFindWalkBody(
        string[] lines, int from, out int bodyStart, out int bodyEnd)
    {
        bodyStart = 0;
        bodyEnd = 0;

        for (var i = from; i < lines.Length; i++)
        {
            var text = lines[i].TrimStart();
            var isLoop = text.StartsWith("while (", StringComparison.Ordinal)
                || text.StartsWith("for (", StringComparison.Ordinal)
                || text.StartsWith("foreach (", StringComparison.Ordinal);
            if (!isLoop) continue;

            var brace = NextNonBlank(lines, i + 1);
            if (brace < 0) return false;
            if (lines[brace].Trim() != "{") continue;

            bodyStart = brace + 1;
            bodyEnd = MatchingBrace(lines, brace);
            return bodyEnd > bodyStart;
        }

        return false;
    }

    private static int NextNonBlank(string[] lines, int from)
    {
        for (var i = from; i < lines.Length; i++)
        {
            if (lines[i].Trim().Length > 0) return i;
        }
        return -1;
    }

    private static int MatchingBrace(string[] lines, int openLine)
    {
        var depth = 0;
        for (var i = openLine; i < lines.Length; i++)
        {
            foreach (var c in lines[i])
            {
                if (c == '{') depth++;
                else if (c == '}')
                {
                    depth--;
                    if (depth == 0) return i;
                }
            }
        }
        return lines.Length - 1;
    }

    private static bool StandsDownBeforeFirstAwait(
        string[] lines, int bodyStart, int bodyEnd)
    {
        for (var i = bodyStart; i < bodyEnd; i++)
        {
            var text = lines[i].Trim();
            if (text.StartsWith("//", StringComparison.Ordinal)) continue;

            if (text.Contains("StandDownIfCeilingFired(scan)", StringComparison.Ordinal))
            {
                return true;
            }
            if (text.Contains("await ", StringComparison.Ordinal)) return false;
        }
        return false;
    }
}
