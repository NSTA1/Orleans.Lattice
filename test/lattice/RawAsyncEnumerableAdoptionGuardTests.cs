using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Repository-wide guard that keeps production code off the raw
/// <c>IAsyncEnumerable</c> members of <c>ILattice</c> / <c>ISystemLattice</c>
/// / <c>ILatticeView</c>.
/// </summary>
/// <remarks>
/// <para>
/// Those members surface <c>EnumerationAbortedException</c> when a
/// <c>MoveNext</c> is routed to a stateless worker that never saw the matching
/// <c>StartEnumeration</c>. That cause is load-proportional rather than
/// environmental, so it cannot be bounded away by keeping a scan short, and
/// the resilient <c>Scan*</c> wrappers are the only correct default.
/// </para>
/// <para>
/// The wrappers themselves must call the raw member - that is what they wrap -
/// and a handful of call sites are genuinely exempt. Both are opted out with a
/// <c>// raw-enumeration-ok:</c> marker carrying a justification, on the call
/// line or within the preceding few lines. The marker is deliberately noisy so
/// that adding one is a decision rather than an accident.
/// </para>
/// </remarks>
[TestFixture]
[Category("Unit")]
public sealed class RawAsyncEnumerableAdoptionGuardTests
{
    private const string Marker = "raw-enumeration-ok:";

    private const int MarkerLookbackLines = 8;

    private static readonly Regex RawCall = new(
        @"\.(KeysAsync|EntriesAsync|KeysWherePredicateAsync|EntriesWherePredicateAsync)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Test]
    public void EveryRawAsyncEnumerableCallSiteInSrcIsWrappedOrJustified()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var violations = new List<string>();
        var filesScanned = 0;
        var markedSites = 0;

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            filesScanned++;
            var lines = File.ReadAllLines(file);
            var relative = Path.GetRelativePath(root, file);

            for (var i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                if (!RawCall.IsMatch(line)) continue;
                if (IsCommentary(line)) continue;
                if (IsDeclaration(line)) continue;

                if (HasMarker(lines, i))
                {
                    markedSites++;
                    continue;
                }

                violations.Add($"{relative}({i + 1}): {line.Trim()}");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                filesScanned,
                Is.GreaterThan(0),
                "Scanned no files under src/. The guard cannot be trusted while its own " +
                "scan is empty - check HygieneRepository.FindRepoRoot().");

            Assert.That(
                markedSites,
                Is.GreaterThan(0),
                $"Found no '{Marker}' marker anywhere under src/. The wrapper " +
                "implementations must each carry one, so a zero count means the " +
                "detection regex has stopped matching and the guard is vacuous.");

            Assert.That(
                violations,
                Is.Empty,
                "Production code must call the resilient Scan* wrappers, not the raw " +
                $"IAsyncEnumerable members. Add a '// {Marker} <reason>' comment on or " +
                "just above the call if it is genuinely exempt.\n" +
                string.Join("\n", violations));
        });
    }

    private static bool IsCommentary(string line)
    {
        var trimmed = line.TrimStart();
        return trimmed.StartsWith("//", StringComparison.Ordinal)
            || trimmed.StartsWith('*');
    }

    /// <summary>
    /// A declaration or explicit interface implementation of one of these
    /// members is not a call site. Both return <c>IAsyncEnumerable&lt;...&gt;</c>
    /// and neither assigns, which distinguishes them from an invocation that
    /// stores or awaits the stream.
    /// </summary>
    private static bool IsDeclaration(string line) =>
        line.Contains("IAsyncEnumerable<", StringComparison.Ordinal)
        && !line.Contains('=');

    private static bool HasMarker(string[] lines, int index)
    {
        var first = Math.Max(0, index - MarkerLookbackLines);
        for (var i = first; i <= index; i++)
        {
            if (lines[i].Contains(Marker, StringComparison.Ordinal)) return true;
        }
        return false;
    }
}
