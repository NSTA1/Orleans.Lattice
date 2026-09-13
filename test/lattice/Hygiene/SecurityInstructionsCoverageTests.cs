using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The security-instruction coverage gate: every package directory in
/// <c>src/</c> whose name extends a governed package name by a dot suffix must
/// itself be named in the <c>applyTo</c> list of
/// <c>.github/instructions/security.instructions.md</c>.
/// <para>
/// An <c>applyTo</c> glob matches PATH SEGMENTS. <c>src/lattice.api.mcp/**</c>
/// requires a segment equal to <c>lattice.api.mcp</c> followed by a separator,
/// so it descends into that directory and does not match the SIBLING directory
/// <c>src/lattice.api.mcp.repocontext/</c>. The dots read as nesting to a human
/// and as an ordinary character to the matcher. Every member of a governed
/// family therefore has to be listed by name, and the list is not
/// self-maintaining.
/// </para>
/// <para>
/// Four packages were missing when this fixture was written (issue #2881),
/// covering 264 source files - among them the 247-file
/// <c>lattice.api.mcp.repocontext</c>, which carries the MCP tool authorization
/// seam, and <c>lattice.explorer.entra.web</c>, a browser-facing identity
/// surface. They had been under active development for weeks with the security
/// invariants silently not attaching.
/// </para>
/// <para>
/// <b>Teaching the convention fixes nothing here, because the convention was
/// already held.</b> Four of the six governed families enumerate every sibling
/// correctly, including <c>lattice.membership</c>, which lists three of them and
/// reaches the two-level <c>lattice.membership.entra.graph</c>. Whoever wrote
/// the list understood exactly that <c>X/**</c> does not match <c>X.Y</c> and
/// applied it correctly two thirds of the time. A reviewer who knows the rule
/// still misses a package about a third of the time, and the miss produces no
/// symptom at all: nothing goes red, no warning is emitted, and no artefact
/// anywhere records that the guidance failed to arrive. Only an executable check
/// converts that silence into a failure, which is why this fixture exists and
/// why it must not be removed as redundant with the prose.
/// </para>
/// <para>
/// The population is derived from the filesystem rather than from a
/// hand-maintained list, so a new package joins the denominator automatically
/// instead of having to be remembered by the same person who just forgot to
/// remember it. Following <c>MeterFieldDeclarationOrderTests</c>, the scan fails
/// loudly when it matches nothing, so it cannot go vacuous and report green.
/// </para>
/// </summary>
[TestFixture]
public sealed class SecurityInstructionsCoverageTests
{
    private const string InstructionsPath = ".github/instructions/security.instructions.md";

    /// <summary>
    /// Governed roots known to enumerate their families correctly, used as
    /// positive controls.
    /// <para>
    /// They are the vacuity control for the dot-suffix relation itself. The
    /// coverage assertion is a search for violations, so it passes trivially if
    /// the relation never fires - a parser that silently returns no entries, a
    /// changed directory layout, or an off-by-one in the prefix test would all
    /// produce an empty violation set and a green run. Requiring each of these
    /// roots to be listed AND to have at least one descendant discovered on disk
    /// proves the relation is live before any conclusion is drawn from its
    /// silence. Each is a family the repository already enumerates completely,
    /// so a control failing here means this fixture broke, not that the
    /// repository did.
    /// </para>
    /// </summary>
    private static readonly string[] PositiveControlRoots =
    [
        "lattice.replication",
        "lattice.api.auth",
        "lattice.membership",
    ];

    private static readonly Regex ApplyToDeclaration = new(
        @"^applyTo:\s*""(?<value>[^""]*)""\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>
    /// The one entry shape this gate can reason about: a whole-package glob
    /// naming a literal directory under <c>src/</c>. Wildcards inside the
    /// package segment are deliberately excluded, so a change of form cannot
    /// silently defeat the parser.
    /// </summary>
    private static readonly Regex PackageGlob = new(
        @"^src/(?<package>[^/*?\[\]]+)/\*\*$",
        RegexOptions.Compiled);

    [Test]
    public void Every_member_of_a_governed_family_is_named_in_the_apply_to_list()
    {
        var listed = ListedPackages();
        var packages = SourcePackageDirectories();
        var listedSet = new HashSet<string>(listed, StringComparer.Ordinal);

        // Family membership is a dot-suffix relation over directory NAMES,
        // which is precisely the relation the glob does not express. The
        // trailing dot is load-bearing: without it 'lattice.api.auth' would
        // claim a hypothetical 'lattice.api.authoring' as family.
        var members = packages
            .SelectMany(package => listed
                .Where(root => package.StartsWith(root + ".", StringComparison.Ordinal))
                .Select(root => (Package: package, Root: root)))
            .ToArray();

        Assert.That(
            members,
            Is.Not.Empty,
            "the scan found no package in src/ extending any listed package name by a dot suffix. "
            + "The repository is known to contain several, so the relation this gate is built on is "
            + "no longer firing and every assertion below it would pass vacuously. Either the src/ "
            + $"layout changed or the applyTo parser has drifted from the form used in {InstructionsPath}.");

        foreach (var root in PositiveControlRoots)
        {
            Assert.That(
                listedSet.Contains(root),
                Is.True,
                $"the positive control '{root}' is no longer listed in {InstructionsPath}. Either a "
                + "governed family was deliberately dropped (in which case update this control and say "
                + "why in the change), or a whole family just lost its security guidance silently.");

            Assert.That(
                members.Any(member => member.Root == root),
                Is.True,
                $"found no package in src/ extending the governed root '{root}', which is known to have "
                + "at least one. This gate cannot distinguish 'no violations' from 'the scan is broken', "
                + "so it refuses to report green until the control fires.");
        }

        var uncovered = members
            .Where(member => !listedSet.Contains(member.Package))
            .GroupBy(member => member.Package, StringComparer.Ordinal)
            .Select(group =>
                $"src/{group.Key}/ extends governed "
                + (group.Skip(1).Any() ? "packages " : "package ")
                + string.Join(", ", group
                    .Select(member => $"'{member.Root}'")
                    .OrderBy(root => root, StringComparer.Ordinal))
                + " but is not listed")
            .OrderBy(entry => entry, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            uncovered,
            Is.Empty,
            $"these packages belong to a family governed by {InstructionsPath} but are absent from its "
            + "applyTo list, so the security invariants never attach when they are edited. An applyTo "
            + "glob matches path segments: 'src/X/**' descends into src/X/ and does NOT match the "
            + "sibling directory src/X.Y/. Add one 'src/<package>/**' entry per package. This failure "
            + "is the only signal the omission produces - nothing else anywhere goes red when guidance "
            + "silently fails to arrive."
            + Environment.NewLine
            + string.Join(Environment.NewLine, uncovered));
    }

    /// <summary>
    /// The reverse drift: a package that is renamed or deleted leaves an entry
    /// behind that governs nothing. That is quiet in the opposite direction -
    /// the list looks longer and more thorough than the coverage it actually
    /// provides, and a stale entry is indistinguishable from a live one by
    /// reading.
    /// </summary>
    [Test]
    public void Every_apply_to_entry_names_a_package_directory_that_exists()
    {
        var src = SourceRoot();
        var listed = ListedPackages();

        var missing = listed
            .Where(package => !Directory.Exists(Path.Combine(src, package)))
            .Select(package => $"src/{package}/**")
            .OrderBy(entry => entry, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            missing,
            Is.Empty,
            $"these applyTo entries in {InstructionsPath} name a directory that does not exist under "
            + "src/, so they govern nothing. A renamed or deleted package un-governs itself silently "
            + "while leaving the list looking complete. Remove the entry, or correct it to the new name."
            + Environment.NewLine
            + string.Join(Environment.NewLine, missing));
    }

    /// <summary>
    /// The parser's own assumption, asserted rather than trusted.
    /// <para>
    /// Every assertion in this fixture derives governed package names by
    /// matching <see cref="PackageGlob"/> against the raw entries. An entry in
    /// any other form - a prefix wildcard such as <c>src/lattice.api.mcp*/**</c>,
    /// a path outside <c>src/</c>, a single-star descent - would simply not
    /// match, and the fixture would carry on reasoning about the subset that
    /// did, reporting green over a list it had only partly understood. That is
    /// the same silent-underenforcement failure the gate exists to catch, so the
    /// unparsed remainder is a failure rather than a skip.
    /// </para>
    /// <para>
    /// This is not a prohibition on changing the form. It is a requirement that
    /// the gate be taught the new form in the same change, so coverage is never
    /// asserted by a parser that cannot read the list.
    /// </para>
    /// </summary>
    [Test]
    public void Every_apply_to_entry_uses_the_whole_package_glob_form_this_gate_understands()
    {
        var entries = ApplyToEntries();

        var unparsed = entries
            .Where(entry => !PackageGlob.IsMatch(entry))
            .OrderBy(entry => entry, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            unparsed,
            Is.Empty,
            $"these applyTo entries in {InstructionsPath} are not whole-package globs of the form "
            + "'src/<package>/**', which is the only form this gate can derive a package name from. "
            + "Entries it cannot parse are excluded from every coverage assertion above, so the gate "
            + "would report green while checking less than the list claims. If the form is being "
            + "changed deliberately, update this fixture in the same change."
            + Environment.NewLine
            + string.Join(Environment.NewLine, unparsed));
    }

    /// <summary>
    /// The raw, comma-separated <c>applyTo</c> entries from the front matter.
    /// </summary>
    private static IReadOnlyList<string> ApplyToEntries()
    {
        var file = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            InstructionsPath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(
            File.Exists(file),
            Is.True,
            $"expected {InstructionsPath} to exist. If the security instructions moved, this gate must "
            + "move with them; a coverage check pointed at a missing file proves nothing.");

        var declaration = ApplyToDeclaration.Match(File.ReadAllText(file));

        Assert.That(
            declaration.Success,
            Is.True,
            $"could not find a quoted 'applyTo:' line in the front matter of {InstructionsPath}. Without "
            + "it there is no glob list to check and this fixture cannot assert anything at all.");

        var entries = declaration.Groups["value"].Value
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        Assert.That(
            entries,
            Is.Not.Empty,
            $"the applyTo list in {InstructionsPath} parsed to no entries, so the security instructions "
            + "attach to nothing at all.");

        return entries;
    }

    /// <summary>
    /// The governed package names, derived from the entries this gate can parse.
    /// <c>Every_apply_to_entry_uses_the_whole_package_glob_form_this_gate_understands</c>
    /// is what guarantees that set is the whole list rather than a subset.
    /// </summary>
    private static IReadOnlyList<string> ListedPackages()
    {
        var listed = ApplyToEntries()
            .Select(entry => PackageGlob.Match(entry))
            .Where(match => match.Success)
            .Select(match => match.Groups["package"].Value)
            .ToArray();

        Assert.That(
            listed,
            Is.Not.Empty,
            $"parsed no 'src/<package>/**' entries out of the applyTo list in {InstructionsPath}, so "
            + "every coverage assertion downstream would have nothing to compare against.");

        return listed;
    }

    private static string SourceRoot()
    {
        var src = Path.Combine(HygieneRepository.FindRepoRoot(), "src");
        Assert.That(Directory.Exists(src), Is.True, "expected a src/ directory at the repository root");
        return src;
    }

    /// <summary>
    /// Every package directory name under <c>src/</c>. Derived from the
    /// filesystem so a newly added package joins the denominator without anyone
    /// having to remember it.
    /// </summary>
    private static IReadOnlyList<string> SourcePackageDirectories()
    {
        var packages = Directory
            .EnumerateDirectories(SourceRoot())
            .Select(Path.GetFileName)
            .Where(name => !string.IsNullOrEmpty(name))
            .Select(name => name!)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            packages,
            Is.Not.Empty,
            "the src/ scan found no package directories at all, so this gate would report green having "
            + "compared the applyTo list against an empty repository.");

        return packages;
    }
}
