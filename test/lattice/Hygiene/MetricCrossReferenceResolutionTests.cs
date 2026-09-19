using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that every metric name an instrument description or doc comment
/// quotes in prose resolves to a metric name actually declared somewhere in
/// <c>src/</c>. A citation that names no real instrument sends an operator to a
/// series that can never return data.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> Issue #3075 split one planned instrument into two by
/// attribution subject (<c>orleans.lattice.wal.gc.pass.reach</c> for the pass,
/// which belongs to no tenant, and <c>orleans.lattice.wal.gc.tree.reach</c> for
/// the tree visit, which does). The neighbouring <c>WalGcBlockingPinStates</c>
/// description already pointed at the pre-split name
/// <c>orleans.lattice.wal.gc.reach</c>, and the split left that pointer behind.
/// Nothing in the repository noticed: eleven repository-wide gates, the
/// dashboards suite and a twelve-thousand-test sweep were all green with a
/// description citing a metric that did not exist.
/// </para>
/// <para>
/// <b>Why it is worth gating when the neighbouring class is not.</b> The wider
/// family - prose that states something false about an instrument - is mostly
/// not mechanically checkable, because a claim like "six arms" is only wrong
/// relative to a meaning no compiler can read. A cited <i>name</i> is the
/// exception: it is checkable against the set of declared names, exactly. This
/// gate deliberately covers only that subset and makes no attempt at the rest.
/// </para>
/// <para>
/// <b>Where it is weak, stated plainly.</b> At the time of writing the whole
/// repository contains two such citations, so the floor below is 2 and this
/// gate is close to vacuous. It is worth having anyway because its value is
/// entirely prospective - it costs nothing until someone renames or splits a
/// cited instrument, which is precisely the operation that produced the defect
/// it was written for. Read a green result here as "no citation dangles",
/// never as "the descriptions are accurate".
/// </para>
/// <para>
/// <b>Convention.</b> A metric name written in prose is single-quoted
/// (<c>'orleans.lattice.foo.bar'</c>); a declaration is the double-quoted
/// string literal passed as the instrument's name. That is what makes the two
/// populations separable by a scanner at all, so keep citing names that way.
/// </para>
/// </remarks>
[TestFixture]
public class MetricCrossReferenceResolutionTests
{
    /// <summary>
    /// A fully-qualified lattice metric name. Anchored on a trailing segment so
    /// a family prefix written with a trailing dot is not mistaken for a name.
    /// </summary>
    private const string NamePattern = @"orleans\.lattice\.[a-z0-9_]+(?:\.[a-z0-9_]+)*";

    private static readonly Regex DeclaredName = new("\"(" + NamePattern + ")\"", RegexOptions.Compiled);

    private static readonly Regex CitedName = new("'(" + NamePattern + ")'", RegexOptions.Compiled);

    /// <summary>
    /// Collects the declared and prose-cited metric names in <paramref name="text"/>.
    /// Shared by the repository scan and by the tests that prove these patterns
    /// detect what they claim to.
    /// </summary>
    private static (IReadOnlyCollection<string> Declared, IReadOnlyCollection<string> Cited) Partition(string text)
    {
        var declared = DeclaredName.Matches(text).Select(m => m.Groups[1].Value).ToHashSet();
        var cited = CitedName.Matches(text).Select(m => m.Groups[1].Value).ToHashSet();
        return (declared, cited);
    }

    [Test]
    public void Every_metric_name_cited_in_prose_resolves_to_a_declared_instrument()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var declared = new HashSet<string>();
        var citations = new List<(string Name, string File)>();

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            var text = File.ReadAllText(file);
            var (fileDeclared, fileCited) = Partition(text);

            foreach (var name in fileDeclared) declared.Add(name);
            foreach (var name in fileCited) citations.Add((name, Path.GetRelativePath(root, file)));
        }

        Assert.That(citations, Is.Not.Empty,
            "Found no single-quoted metric name anywhere in src/. Either the citation convention "
            + "has changed or the scan pattern has drifted from the source, so this guard is "
            + "silently vacuous and is asserting nothing.");

        var dangling = citations
            .Where(c => !declared.Contains(c.Name))
            .Select(c => $"{c.File}: cites '{c.Name}', which no instrument in src/ declares.")
            .Distinct()
            .ToList();

        Assert.That(dangling, Is.Empty,
            "An instrument description or doc comment cites a metric name that does not exist. "
            + "An operator who follows that pointer queries a series which can never return data, "
            + "and - because an absent series is exactly how this repository signals 'not deployed' - "
            + "reads the emptiness as a finding about the system rather than as a typo. "
            + "Renaming or splitting an instrument means updating every prose citation of the old "
            + "name.\n" + string.Join("\n", dangling));
    }

    /// <summary>
    /// The scan above reports a clean repository. This proves that result is a
    /// measured zero rather than a scanner that cannot see anything: the same
    /// partition applied to a citation of an undeclared name must flag it.
    /// </summary>
    [Test]
    public void The_scan_detects_a_citation_that_names_no_declared_instrument()
    {
        const string sample =
            "Counter(name: \"orleans.lattice.wal.gc.pass.reach\", "
            + "description: \"see 'orleans.lattice.wal.gc.reach' for the advancing layer\");";

        var (declared, cited) = Partition(sample);

        Assert.That(declared, Does.Contain("orleans.lattice.wal.gc.pass.reach"),
            "The declaration pattern failed to read a double-quoted instrument name.");
        Assert.That(cited, Does.Contain("orleans.lattice.wal.gc.reach"),
            "The citation pattern failed to read a single-quoted name from prose.");
        Assert.That(cited.Where(c => !declared.Contains(c)), Is.Not.Empty,
            "The partition did not surface the dangling citation, so a green repository scan "
            + "would prove nothing. This is the exact shape of the #3075 defect.");
    }

    /// <summary>
    /// The converse: a citation whose name is declared must not be reported.
    /// Without this, a scanner that flagged every citation would also pass the
    /// detection test above while reddening the whole repository.
    /// </summary>
    [Test]
    public void The_scan_accepts_a_citation_that_names_a_declared_instrument()
    {
        const string sample =
            "Counter(name: \"orleans.lattice.wal.gc.tree.reach\", "
            + "description: \"see 'orleans.lattice.wal.gc.tree.reach' for the tree arm\");";

        var (declared, cited) = Partition(sample);

        Assert.That(cited.Where(c => !declared.Contains(c)), Is.Empty,
            "A citation naming a declared instrument was reported as dangling, so this gate "
            + "would redden on correct prose.");
    }
}
