using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// The structural half of the post-commit fault-isolation contract. The
/// behavioural fixture beside this one proves the paths that exist today report a
/// committed write rather than throwing; this one derives the population those
/// paths belong to <b>from the source</b>, so a path added later is enrolled the
/// moment it exists rather than when somebody remembers to add it.
/// <para>
/// An inventory is a snapshot of what somebody remembered; it is not a detector.
/// Every declared list below is therefore asserted against a set derived by
/// scanning, so the list is a report of the scan rather than a substitute for it,
/// and a new post-commit region reddens this fixture instead of shipping
/// uncovered.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextStorePostCommitStructureTests
{
    /// <summary>
    /// Methods that perform post-commit enrichment. Declared, then asserted equal
    /// to the set derived from the source; it is never trusted on its own.
    /// </summary>
    private static readonly string[] DeclaredPostCommitMethods =
        ["RememberAsync", "UpdateAsync", "ForgetAsync"];

    /// <summary>
    /// Calls permitted inside a post-commit region. Both absorb their own faults,
    /// so neither can turn a durable write into a reported failure.
    /// </summary>
    private static readonly string[] PostCommitSafeCalls =
        ["TryReadCommittedExpiryAsync", "InvalidateMemoryVectorAsync"];

    /// <summary>
    /// Calls that make a write durable. A post-commit region runs from one of these
    /// to the vector invalidation that closes it.
    /// </summary>
    private static readonly Regex DurableWrite = new(
        @"\.(SetAsync|DeleteAsync|DeleteRangeAsync|SetRangeAsync)\(",
        RegexOptions.Compiled);

    /// <summary>The expected number of post-commit regions, as an anti-vacuity floor.</summary>
    private const int KnownPostCommitRegions = 4;

    /// <summary>
    /// Declaration headers at type-member indentation, capturing the member name.
    /// The segment between the modifier and the parameter list must contain no
    /// <c>=</c>, which excludes field and property initialisers that happen to call
    /// a constructor.
    /// </summary>
    private static readonly Regex MemberHeader = new(
        @"^    (?:public|internal|protected|private)[^=;]*?\s([A-Za-z_]\w*)\s*\(",
        RegexOptions.Compiled);

    /// <summary>
    /// Every source file of the (partial) store type, discovered by pattern rather
    /// than listed, so a new partial is scanned automatically.
    /// </summary>
    private static IReadOnlyList<string> StoreSourceFiles()
    {
        var directory = Path.Combine(
            HygieneRepository.FindRepoRoot(), "src", "lattice.api.mcp.repocontext", "Capture");
        return Directory.GetFiles(directory, "RepoContextStore*.cs");
    }

    /// <summary>
    /// Maps each line of the store's source to the member it sits in, then returns
    /// the distinct member names whose body contains <paramref name="call"/>.
    /// </summary>
    private static IReadOnlyCollection<string> MembersCalling(string call)
    {
        var callers = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var file in StoreSourceFiles())
        {
            var member = "<file scope>";
            foreach (var line in File.ReadLines(file))
            {
                var header = MemberHeader.Match(line);
                if (header.Success)
                {
                    member = header.Groups[1].Value;
                }

                if (line.Contains(call + "(", StringComparison.Ordinal) && !header.Success)
                {
                    callers.Add(member);
                }
            }
        }

        return callers;
    }

    [Test]
    public void The_store_source_is_actually_found_and_scanned()
    {
        var files = StoreSourceFiles();

        Assert.Multiple(() =>
        {
            Assert.That(files, Is.Not.Empty,
                "Anti-vacuity: every gate in this fixture quantifies over these files, so an empty "
                + "set would make all of them pass on nothing.");
            Assert.That(
                files.Any(f => Path.GetFileName(f) == "RepoContextStore.cs"), Is.True,
                "The primary partial must be among the scanned files.");
        });
    }

    /// <summary>
    /// The post-commit population, derived. Every memory write retires the entry's
    /// stale vector after committing, so the invalidation call marks the
    /// post-commit region of each write path - and unlike a comment marker it
    /// cannot be omitted without breaking vector correctness, which other fixtures
    /// already cover. That is what makes it a usable structural discriminator
    /// rather than another hand-maintained list.
    /// </summary>
    [Test]
    public void Every_post_commit_region_is_in_a_method_the_behavioural_fixture_covers()
    {
        var derived = MembersCalling("InvalidateMemoryVectorAsync");

        Assert.Multiple(() =>
        {
            Assert.That(derived, Has.Count.GreaterThanOrEqualTo(3),
                "Anti-vacuity: the derivation must find the known write paths, or the comparison "
                + "below would pass by matching nothing against nothing.");
            Assert.That(derived, Is.EquivalentTo(DeclaredPostCommitMethods),
                "A write path gained or lost post-commit enrichment. Cover the new path in "
                + "RepoContextStorePostCommitFaultIsolationTests and update the declared list - do "
                + "not relax this assertion.");
        });
    }

    /// <summary>
    /// The regression guard proper, and the reason this fixture exists rather than
    /// a hand-written list of call sites.
    /// <para>
    /// A post-commit region is derived, not declared: it runs from a durable write
    /// to the vector invalidation that closes it. Every <c>await</c> inside one must
    /// be a call that absorbs its own faults. A read-back added raw reinstates the
    /// defect <b>silently</b> - it succeeds on every green path and discards a
    /// committed write only when the read happens to fault, which no ordinary test
    /// exercises.
    /// </para>
    /// <para>
    /// Note the boundary this predicate deliberately does NOT use: a comment marker.
    /// A marker covers exactly the regions somebody remembered to mark, which is the
    /// hand-maintained list the gate exists to replace, wearing a marker. Both
    /// anchors here are functional calls that cannot be omitted without breaking
    /// something else.
    /// </para>
    /// </summary>
    [Test]
    public void Every_await_in_a_post_commit_region_absorbs_its_own_faults()
    {
        var regions = 0;
        var offences = new List<string>();

        foreach (var file in StoreSourceFiles())
        {
            var lines = File.ReadAllLines(file);
            var name = Path.GetFileName(file);

            for (var close = 0; close < lines.Length; close++)
            {
                if (!lines[close].Contains("await InvalidateMemoryVectorAsync(", StringComparison.Ordinal))
                {
                    continue;
                }

                var open = LastDurableWriteBefore(lines, close);
                if (open < 0)
                {
                    offences.Add(
                        $"{name}:{close + 1}: a vector invalidation with no preceding durable write. "
                        + "The region anchors no longer describe this code; re-derive them.");
                    continue;
                }

                regions++;
                for (var i = open + 1; i < close; i++)
                {
                    var line = lines[i];
                    if (!line.Contains("await ", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (!PostCommitSafeCalls.Any(c => line.Contains(c + "(", StringComparison.Ordinal)))
                    {
                        offences.Add(
                            $"{name}:{i + 1}: {line.Trim()}"
                            + " -- awaited after a durable write without absorbing its faults.");
                    }
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(regions, Is.GreaterThanOrEqualTo(KnownPostCommitRegions),
                "Anti-vacuity: fewer post-commit regions were derived than are known to exist, so "
                + "this gate would pass by scanning almost nothing.");
            Assert.That(offences, Is.Empty,
                "A fault on any of these lines fails a call whose write has already committed, "
                + "reporting no effect about a durable one. Route it through a helper that absorbs "
                + "its own faults and degrades the reported field instead:"
                + Environment.NewLine + string.Join(Environment.NewLine, offences));
        });
    }

    /// <summary>
    /// Scans backwards from <paramref name="close"/> for the durable write opening
    /// that region, stopping at the enclosing member's declaration so a region can
    /// never straddle two methods.
    /// </summary>
    private static int LastDurableWriteBefore(IReadOnlyList<string> lines, int close)
    {
        for (var i = close - 1; i >= 0; i--)
        {
            if (DurableWrite.IsMatch(lines[i]))
            {
                return i;
            }

            if (MemberHeader.IsMatch(lines[i]))
            {
                return -1;
            }
        }

        return -1;
    }
}
