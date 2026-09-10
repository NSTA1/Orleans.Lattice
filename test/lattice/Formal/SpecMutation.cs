using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The class of TLC property a mutation targets. TLC words its violation
/// banner differently for each, and only two of the three name the property,
/// so the harness has to know which it is expecting.
/// </summary>
public enum SpecPropertyClass
{
    /// <summary>A state predicate in the cfg's INVARIANTS block.</summary>
    Invariant,

    /// <summary>A box-of-action formula: a safety property in PROPERTIES.</summary>
    Action,

    /// <summary>A true liveness property, needing the fairness in Spec.</summary>
    Temporal,
}

/// <summary>
/// One mutation of <c>spec/AtomicCommit.tla</c>, paired with the single
/// property it must make TLC report as violated.
/// <para>
/// A mutation is stored as a set of anchored edits rather than as a mutated
/// copy of the specification, and this is the whole point of the type. The
/// obvious design - check in a full mutant module beside the base - has a
/// defect that only shows up months later: an edit to the base does not
/// propagate, nothing detects that it did not, and a mutant that has drifted
/// far enough from the base has quietly stopped being evidence about the base
/// while still passing. That is the same shape of failure as the artefacts the
/// atomicity audit found, reintroduced by the fix for them.
/// </para>
/// <para>
/// A unit test comparing mutant against base could DETECT that drift. Deriving
/// the mutant from the base at run time makes it UNEXPRESSIBLE, which is
/// strictly better: there is no second copy to fall behind. Every edit is
/// anchored on exact text from the base and
/// <see cref="Apply"/> requires each anchor to match exactly once, so a base
/// change that touches an anchored region fails loudly at the point where the
/// mutation needs re-deriving, and a base change that does not touch one is
/// absorbed silently and correctly.
/// </para>
/// </summary>
public sealed record SpecMutation
{
    private const string BaseModuleName = "AtomicCommit";

    /// <summary>File stem of the mutation, used as the test case name.</summary>
    public required string Name { get; init; }

    /// <summary>TLA+ module name of the generated mutant. Must equal its filename.</summary>
    public required string Module { get; init; }

    /// <summary>The single property this mutation must make fire.</summary>
    public required string Target { get; init; }

    /// <summary>Which kind of property <see cref="Target"/> is.</summary>
    public required SpecPropertyClass PropertyClass { get; init; }

    /// <summary>A one-line description of the mutation, for failure messages.</summary>
    public required string Summary { get; init; }

    /// <summary>The anchored edits, applied in order.</summary>
    public required IReadOnlyList<SpecEdit> Edits { get; init; }

    /// <summary>
    /// The exact text TLC emits when <see cref="Target"/> is violated.
    /// <para>
    /// Note the asymmetry in the <see cref="SpecPropertyClass.Temporal"/> case:
    /// TLC does NOT name the property for a true liveness violation, only for
    /// invariants and action properties. So a liveness mutation cannot be
    /// checked against the property's name, and the guard against a
    /// misattributed violation has to come from elsewhere - specifically from
    /// the generated cfg naming exactly one property, which the caller also
    /// asserts by counting violation lines. Worth stating rather than leaving
    /// as a silent gap, because "assert the banner names the property" is the
    /// rule everywhere else here and it simply cannot be applied to two of the
    /// twelve.
    /// </para>
    /// </summary>
    public string ExpectedBanner => PropertyClass switch
    {
        SpecPropertyClass.Invariant => $"Invariant {Target} is violated.",
        SpecPropertyClass.Action => $"Action property {Target} is violated.",
        SpecPropertyClass.Temporal => "Temporal properties were violated.",
        _ => throw new InvalidOperationException($"unhandled property class {PropertyClass}"),
    };

    /// <summary>
    /// Produces the mutant module from the current base specification.
    /// Throws with a specific message if any anchor no longer matches exactly
    /// once, which is the drift signal.
    /// </summary>
    public string Apply(string baseSpecification)
    {
        ArgumentNullException.ThrowIfNull(baseSpecification);

        // Normalised before anchoring. The anchors are stored with \n endings,
        // so on a CRLF checkout an un-normalised compare would fail to match
        // every multi-line anchor and report the whole catalogue as drifted -
        // a confusing failure with a cause nowhere near the message.
        var normalised = baseSpecification.ReplaceLineEndings("\n");

        var text = ReplaceExactlyOnce(
            normalised,
            $"MODULE {BaseModuleName} ",
            $"MODULE {Module} ",
            "the module header");

        for (var i = 0; i < Edits.Count; i++)
        {
            text = ReplaceExactlyOnce(text, Edits[i].Find, Edits[i].Replace, $"edit {i + 1}");
        }

        if (text == normalised)
        {
            throw new InvalidOperationException(
                $"mutation '{Name}' produced a module identical to the base specification. "
                + "A mutation that changes nothing cannot make anything fire.");
        }

        return text;
    }

    /// <summary>
    /// Builds the cfg for this mutation: exactly one target property, written
    /// whole rather than appended to the base model's list.
    /// <para>
    /// The cfg is generated rather than checked in for the same reason the
    /// module is. It also removes a specific trap the audit hit: a cfg that
    /// PREPENDED a property to the base list instead of replacing it produced
    /// six satisfiability branches rather than two, and reported two violations
    /// attributed to the wrong properties. It read as a confident result.
    /// Generating the cfg from the target makes that shape unexpressible.
    /// </para>
    /// <para>
    /// <c>TypeOK</c> is carried alongside every non-<c>TypeOK</c> target on
    /// purpose. A mutation that accidentally puts a variable out of its
    /// declared domain could otherwise make the target fire for a reason that
    /// has nothing to do with the property, and the run would look like a
    /// successful pairing. With <c>TypeOK</c> present that mutation reports
    /// <c>TypeOK</c> instead, the banner assertion fails, and the mistake
    /// surfaces as a mistake.
    /// </para>
    /// </summary>
    public string BuildConfig(string baseConfig)
    {
        ArgumentNullException.ThrowIfNull(baseConfig);

        var builder = new StringBuilder();
        builder.AppendLine($"\\* Generated for mutation {Name}. Do not check this file in.");
        builder.AppendLine($"\\* Target: {Target} ({PropertyClass}).");
        builder.AppendLine();
        builder.AppendLine("SPECIFICATION Spec");
        builder.AppendLine();
        builder.AppendLine(ExtractConstants(baseConfig));
        builder.AppendLine();

        builder.AppendLine("INVARIANTS");
        builder.AppendLine("    TypeOK");
        if (PropertyClass == SpecPropertyClass.Invariant && Target != "TypeOK")
        {
            builder.AppendLine($"    {Target}");
        }

        if (PropertyClass is SpecPropertyClass.Action or SpecPropertyClass.Temporal)
        {
            builder.AppendLine();
            builder.AppendLine("PROPERTIES");
            builder.AppendLine($"    {Target}");
        }

        return builder.ToString();
    }

    private static string ExtractConstants(string baseConfig)
    {
        var lines = baseConfig.ReplaceLineEndings("\n").Split('\n');
        var start = Array.FindIndex(lines, line => line.TrimStart().StartsWith("CONSTANTS", StringComparison.Ordinal));
        if (start < 0)
        {
            throw new InvalidOperationException("spec/AtomicCommit.cfg has no CONSTANTS block.");
        }

        var captured = new List<string> { lines[start] };
        for (var i = start + 1; i < lines.Length; i++)
        {
            var trimmed = lines[i].TrimStart();
            if (trimmed.StartsWith("INVARIANTS", StringComparison.Ordinal)
                || trimmed.StartsWith("PROPERTIES", StringComparison.Ordinal)
                || trimmed.StartsWith("SPECIFICATION", StringComparison.Ordinal))
            {
                break;
            }

            if (trimmed.Length > 0 && !trimmed.StartsWith("\\*", StringComparison.Ordinal))
            {
                captured.Add(lines[i]);
            }
        }

        return string.Join(Environment.NewLine, captured).TrimEnd();
    }

    private string ReplaceExactlyOnce(string text, string find, string replace, string what)
    {
        var occurrences = CountOccurrences(text, find);
        if (occurrences != 1)
        {
            var diagnosis = occurrences == 0
                ? "The base specification no longer contains this text, so the mutation has drifted "
                  + "and must be re-derived against the current spec/AtomicCommit.tla."
                : $"The base specification contains this text {occurrences} times, so the anchor is "
                  + "ambiguous and the mutation would be applied somewhere unintended. Widen it with "
                  + "surrounding context until it is unique.";

            throw new InvalidOperationException(
                $"mutation '{Name}' could not apply {what}: expected exactly one match, found "
                + $"{occurrences}.{Environment.NewLine}{diagnosis}{Environment.NewLine}"
                + $"Anchor text was:{Environment.NewLine}{find}");
        }

        return text.Replace(find, replace, StringComparison.Ordinal);
    }

    private static int CountOccurrences(string text, string needle)
    {
        if (needle.Length == 0)
        {
            return 0;
        }

        var count = 0;
        var index = text.IndexOf(needle, StringComparison.Ordinal);
        while (index >= 0)
        {
            count++;
            index = text.IndexOf(needle, index + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }

    /// <summary>
    /// The mutation's name, which is what NUnit renders as the test-case label
    /// for each entry in the pairing test's source.
    /// </summary>
    public override string ToString() => Name;
}

/// <summary>One anchored find/replace edit within a mutation.</summary>
public sealed record SpecEdit(string Find, string Replace);

/// <summary>
/// Reads the <c>*.mutation</c> files in <c>spec/mutations/</c>.
/// <para>
/// The format is deliberately dull: <c>KEY: value</c> metadata, then one or
/// more <c>--- FIND</c> / <c>--- REPLACE</c> / <c>--- END</c> blocks holding
/// verbatim TLA+ text. Anything cleverer (a real patch format, a template
/// language) would make a mutation harder to read in review than the thing it
/// mutates, and a mutation nobody can review is not evidence.
/// </para>
/// </summary>
public static class SpecMutationCatalogue
{
    private const string FindMarker = "--- FIND";
    private const string ReplaceMarker = "--- REPLACE";
    private const string EndMarker = "--- END";

    /// <summary>Parses every mutation file in the directory, ordered by name.</summary>
    public static IReadOnlyList<SpecMutation> Load(string directory)
    {
        ArgumentNullException.ThrowIfNull(directory);

        return Directory
            .EnumerateFiles(directory, "*.mutation")
            .OrderBy(Path.GetFileName, StringComparer.Ordinal)
            .Select(Parse)
            .ToList();
    }

    private static SpecMutation Parse(string path)
    {
        var name = Path.GetFileNameWithoutExtension(path);
        var lines = File.ReadAllText(path).ReplaceLineEndings("\n").Split('\n');

        var metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var edits = new List<SpecEdit>();

        var index = 0;
        for (; index < lines.Length; index++)
        {
            var line = lines[index];
            if (line.StartsWith(FindMarker, StringComparison.Ordinal))
            {
                break;
            }

            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var separator = line.IndexOf(':', StringComparison.Ordinal);
            if (separator <= 0)
            {
                throw new InvalidOperationException(
                    $"{name}.mutation line {index + 1} is neither metadata ('KEY: value'), a comment, "
                    + $"nor a block marker: '{line}'");
            }

            metadata[line[..separator].Trim()] = line[(separator + 1)..].Trim();
        }

        while (index < lines.Length)
        {
            if (!lines[index].StartsWith(FindMarker, StringComparison.Ordinal))
            {
                index++;
                continue;
            }

            var find = Collect(lines, ref index, FindMarker, ReplaceMarker, name);
            var replace = Collect(lines, ref index, ReplaceMarker, EndMarker, name);
            edits.Add(new SpecEdit(find, replace));
        }

        if (edits.Count == 0)
        {
            throw new InvalidOperationException($"{name}.mutation declares no edits.");
        }

        return new SpecMutation
        {
            Name = name,
            Module = Require(metadata, "MODULE", name),
            Target = Require(metadata, "TARGET", name),
            PropertyClass = Enum.Parse<SpecPropertyClass>(Require(metadata, "CLASS", name), ignoreCase: true),
            Summary = Require(metadata, "SUMMARY", name),
            Edits = edits,
        };
    }

    /// <summary>
    /// Collects the verbatim lines between two markers. Line endings are
    /// normalised to <c>\n</c> on both sides of the comparison so that a
    /// checkout with CRLF endings (or a mutation file committed with them)
    /// cannot turn an anchor into a silent non-match.
    /// </summary>
    private static string Collect(string[] lines, ref int index, string open, string close, string name)
    {
        if (!lines[index].StartsWith(open, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"{name}.mutation expected '{open}' at line {index + 1}.");
        }

        index++;
        var body = new List<string>();
        while (index < lines.Length && !lines[index].StartsWith(close, StringComparison.Ordinal))
        {
            body.Add(lines[index]);
            index++;
        }

        if (index >= lines.Length)
        {
            throw new InvalidOperationException($"{name}.mutation has an unterminated block; expected '{close}'.");
        }

        return string.Join("\n", body);
    }

    private static string Require(IDictionary<string, string> metadata, string key, string name) =>
        metadata.TryGetValue(key, out var value) && value.Length > 0
            ? value
            : throw new InvalidOperationException($"{name}.mutation is missing required metadata '{key}:'.");

    /// <summary>The name of the TLC config block holding state predicates.</summary>
    public const string InvariantsBlock = "INVARIANTS";

    /// <summary>The name of the TLC config block holding temporal formulas.</summary>
    public const string PropertiesBlock = "PROPERTIES";

    /// <summary>
    /// Reads the property names the base model actually checks, so the
    /// completeness gate is driven by the model rather than by a list somebody
    /// has to remember to update. Adding a property to
    /// <c>spec/AtomicCommit.cfg</c> without pairing it therefore fails.
    /// <para>
    /// Flattens both blocks. Use
    /// <see cref="ReadCheckedPropertiesByBlock"/> when the distinction matters,
    /// which it does for any check on WHERE a property is declared.
    /// </para>
    /// </summary>
    public static IReadOnlyList<string> ReadCheckedProperties(string baseConfig) =>
        ReadCheckedPropertiesByBlock(baseConfig).SelectMany(kv => kv.Value).ToArray();

    /// <summary>
    /// Reads the checked property names partitioned by the config block that
    /// declares them.
    /// <para>
    /// The partition is load-bearing rather than cosmetic. TLC evaluates an
    /// entry in the INVARIANTS block as a state predicate, so a temporal
    /// formula placed there is checked per state instead of over behaviours and
    /// the run still succeeds - a green that is weaker than, and different
    /// from, the one the property's name promises. Nothing in TLC objects, so
    /// the only way to keep liveness properties under PROPERTIES is to assert
    /// it, which is what issue #2323's supporting controls ask for.
    /// </para>
    /// <para>
    /// Both keys are always present, mapping to an empty list when the block is
    /// absent, so callers need no null or missing-key handling.
    /// </para>
    /// </summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> ReadCheckedPropertiesByBlock(string baseConfig)
    {
        ArgumentNullException.ThrowIfNull(baseConfig);

        var invariants = new List<string>();
        var properties = new List<string>();
        List<string>? current = null;

        foreach (var raw in baseConfig.ReplaceLineEndings("\n").Split('\n'))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith("\\*", StringComparison.Ordinal))
            {
                continue;
            }

            if (line.StartsWith(InvariantsBlock, StringComparison.Ordinal))
            {
                current = invariants;
                continue;
            }

            if (line.StartsWith(PropertiesBlock, StringComparison.Ordinal))
            {
                current = properties;
                continue;
            }

            if (line.StartsWith("SPECIFICATION", StringComparison.Ordinal)
                || line.StartsWith("CONSTANTS", StringComparison.Ordinal))
            {
                current = null;
                continue;
            }

            // A CONSTANTS assignment line ('t1 = t1') keeps capturing off; only
            // bare identifiers inside an INVARIANTS / PROPERTIES block count.
            if (current is not null && Regex.IsMatch(line, "^[A-Za-z][A-Za-z0-9_]*$"))
            {
                current.Add(line);
            }
            else
            {
                current = null;
            }
        }

        return new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
        {
            [InvariantsBlock] = invariants,
            [PropertiesBlock] = properties,
        };
    }
}
