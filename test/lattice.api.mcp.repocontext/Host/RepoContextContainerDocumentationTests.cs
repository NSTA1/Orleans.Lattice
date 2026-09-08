using System.Reflection;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Asserts that every <c>LATTICE_*</c> environment variable this host actually
/// reads is named in <c>docs/lattice.api.mcp.repocontext/container.md</c>, the
/// document an operator configures a deployment from.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this guard exists (issue #2279).</b> That issue's first-choice remedy
/// for an oversubscribed WAL replay gate was "pin the ceiling in the container
/// configuration". The knob that makes it possible,
/// <see cref="RepoContextReplayConcurrency.MaxConcurrentReplaysKey"/>, shipped
/// with its parsing, its validation, its silo wiring, and its startup report -
/// and was named in no operator-facing document at all. A remedy nobody can
/// find is not meaningfully different from a remedy that was never built, and
/// the failure is silent in exactly the way #2279 itself is: nothing in the
/// build, the tests, or startup compared the set of variables the host reads
/// against the set the documentation offers.
/// </para>
/// <para>
/// So this is the #2275 remedy applied to configuration surface rather than to
/// a comment: the durable fix for an assertion nobody checks is to make
/// something check it. When this fails, the fix is to document the variable,
/// not to relax the guard.
/// </para>
/// <para>
/// <b>Reflection rather than a source scan</b>, because the keys are already
/// <c>public const string</c> fields on the host's own types. Reading them from
/// the compiled assembly cannot drift from what the host reads, whereas a
/// regular expression over source files can silently stop matching after a
/// formatting change and report a clean result it never earned.
/// </para>
/// <para>
/// Carries no category, matching <c>MetricsDocCoverageTests</c> - the closest
/// precedent, and likewise a cheap documentation-drift guard rather than the
/// heavy Roslyn snippet compilation that <c>[Category("Docs")]</c> exists to
/// keep out of the fast tier. That places it in the default non-chaos lane,
/// where a missing row is reported in seconds instead of in the slower
/// docs-and-integration tier.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextContainerDocumentationTests
{
    /// <summary>The operator-facing configuration reference for the container.</summary>
    private const string ContainerDocRelativePath = "docs/lattice.api.mcp.repocontext/container.md";

    /// <summary>
    /// Anti-vacuity floor (issue #2275). Asserted on the DENOMINATOR, never on
    /// the violation list, so a reflection scan that silently stopped finding
    /// keys - a moved type, a renamed assembly, a changed field shape - fails
    /// here rather than reporting a fully documented host it never examined.
    /// The host declared 21 keys when this guard was written; the floor sits
    /// well below that so ordinary removals do not trip it, and far above zero
    /// so a broken scan cannot pass.
    /// </summary>
    private const int MinimumExpectedKeys = 15;

    /// <summary>
    /// Every <c>LATTICE_*</c> key constant declared by the host assembly, excluding
    /// the bare <see cref="RepoContextEffectiveConfiguration.LatticePrefix"/>, which
    /// is a prefix used to scan configuration rather than a variable anyone sets.
    /// </summary>
    private static IReadOnlyList<string> DeclaredKeys() =>
        typeof(RepoContextReplayConcurrency).Assembly
            .GetTypes()
            .SelectMany(t => t.GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy))
            .Where(f => f.IsLiteral && !f.IsInitOnly && f.FieldType == typeof(string))
            .Select(f => f.GetRawConstantValue() as string)
            .Where(v => v is not null
                && v.StartsWith("LATTICE_", StringComparison.Ordinal)
                && v.Length > "LATTICE_".Length)
            .Select(v => v!)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Reports whether <paramref name="line"/> names <paramref name="key"/> as a whole
    /// token. The trailing-character check stops a shorter key being counted as
    /// documented merely because a longer one that starts with it is - which would
    /// let exactly the omission this guard exists to catch slip through.
    /// </summary>
    private static bool NamesKey(string line, string key)
    {
        var from = 0;
        while (true)
        {
            var at = line.IndexOf(key, from, StringComparison.Ordinal);
            if (at < 0)
            {
                return false;
            }

            var end = at + key.Length;
            if (end >= line.Length || !(char.IsAsciiLetterOrDigit(line[end]) || line[end] == '_'))
            {
                return true;
            }

            from = at + 1;
        }
    }

    /// <summary>
    /// Reports whether the document describes <paramref name="key"/> in one of its
    /// configuration <b>tables</b>, rather than merely mentioning it somewhere in
    /// prose.
    /// <para>
    /// The distinction is the point. A variable named only in a passing sentence
    /// ("see the replay ceiling below") tells an operator that something exists
    /// without telling them its default, its accepted range, or what it does, which
    /// is most of what they need in order to set it. Every one of the host's keys
    /// satisfies the stricter form today, so requiring it costs nothing now and
    /// stops the weaker form being introduced later.
    /// </para>
    /// </summary>
    private static bool DocumentsInTable(IEnumerable<string> lines, string key) =>
        lines.Any(l => l.TrimStart().StartsWith('|') && NamesKey(l, key));

    [Test]
    public void Every_environment_variable_the_host_reads_is_documented_for_operators()
    {
        var keys = DeclaredKeys();

        Assert.That(
            keys.Count,
            Is.GreaterThanOrEqualTo(MinimumExpectedKeys),
            $"The reflection scan found only {keys.Count} LATTICE_* key constant(s) on the host assembly, "
            + $"below the {MinimumExpectedKeys} floor. That is a broken scan, not a small host: fix the scan "
            + "rather than lowering the floor, otherwise this guard silently passes without checking anything.");

        var root = HygieneRepository.FindRepoRoot();
        var path = Path.Combine(root, ContainerDocRelativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.That(File.Exists(path), Is.True, $"The container reference doc was not found at '{path}'.");

        var doc = File.ReadAllLines(path);
        var missing = keys.Where(k => !DocumentsInTable(doc, k)).ToList();

        Assert.That(
            missing,
            Is.Empty,
            "The host reads these environment variables, but "
            + $"{ContainerDocRelativePath} has no configuration-table row naming them, so an operator has no "
            + $"way to discover them:{Environment.NewLine}  - "
            + string.Join(Environment.NewLine + "  - ", missing)
            + $"{Environment.NewLine}Add a row giving each variable's default and purpose. Do not delete this "
            + "assertion: an undocumented knob is indistinguishable, from outside the process, from a knob "
            + "that does not exist.");
    }
}
