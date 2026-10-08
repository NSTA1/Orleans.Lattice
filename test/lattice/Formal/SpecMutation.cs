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

    /// <summary>
    /// A formula TLC checks over whole behaviours rather than single steps: a
    /// liveness property needing the fairness in Spec, or a safety property
    /// stated with nested <c>[]</c> (<c>MonotonicVisibility</c>). TLC reports a
    /// violation of either without naming the property.
    /// </summary>
    Temporal,
}

/// <summary>
/// One mutation of a module under <c>spec/</c> (see <see cref="SpecModule"/>),
/// paired with the single property it must make TLC report as violated.
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
    /// The protocol actions in <c>Next</c> whose definitions this mutation
    /// edits, from the optional <c>PERTURBS:</c> header. Empty for a mutation
    /// that perturbs a read definition, a fairness assumption, or that splices
    /// in an action the protocol does not have.
    /// <para>
    /// A declaration, and so a claim: <see cref="SpecActionMutationCoverageTests"/>
    /// checks each name against the edits rather than trusting it, because a
    /// coverage table whose entries nobody verifies is the artefact this
    /// directory was written to replace.
    /// </para>
    /// </summary>
    public IReadOnlyList<string> Perturbs { get; init; } = [];

    /// <summary>
    /// Whether TLC's deadlock check is switched off for this mutation, from
    /// the optional <c>DEADLOCK: off</c> header.
    /// <para>
    /// A mutant can be left with no enabled action, and TLC then reports
    /// <c>Deadlock reached</c> before it ever evaluates a temporal target (it
    /// checks those only after the state search completes), so the experiment
    /// would be about the deadlock rather than the property. Switching the check
    /// off lets TLC treat the stuck state as stuttering forever, which is the
    /// behaviour the property then has to reject. It is a claim, and
    /// <see cref="TlcModelCheckTests"/> checks it: a mutation that declares it
    /// is also run once with the check left on and must report a deadlock, so
    /// the switch cannot be left on a mutation that does not need it.
    /// </para>
    /// </summary>
    public bool DeadlockCheckDisabled { get; init; }

    /// <summary>
    /// The extra command-line options TLC is run with for both arms of this
    /// mutation's experiment: <c>-deadlock</c> when
    /// <see cref="DeadlockCheckDisabled"/> is set, otherwise none. The control
    /// arm uses the same options as the mutant so the two arms differ only in
    /// the module.
    /// </summary>
    public IReadOnlyList<string> TlcOptions => DeadlockCheckDisabled ? [DeadlockSwitch] : [];

    /// <summary>TLC's switch that turns its deadlock check off.</summary>
    public const string DeadlockSwitch = "-deadlock";

    /// <summary>
    /// The model-size overrides applied to the mutant arm only, from the
    /// optional <c>BOUNDS:</c> header: comma-separated <c>Name = value</c>
    /// assignments to a bound the specification declares or defines.
    /// <para>
    /// A mutant has to show one counterexample, not hold over the whole
    /// instance, and a temporal mutant pays for the full state graph before TLC
    /// reports it. Running it on the smallest instance that still exhibits the
    /// violation saves that cost without weakening anything the experiment
    /// asserts: the control arm, which is where "the property holds on the
    /// base" is decided, is built by <see cref="Orleans.Lattice.Tests.Formal.SpecMutation.BuildConfig(string)"/> and never sees
    /// these assignments, so it still checks the module's own bounds; and the
    /// mutant arm still has to report exactly its target, so an override that
    /// shrinks the instance below the violation leaves the mutant clean and
    /// fails the experiment rather than passing it. A name the specification
    /// does not have would be accepted by TLC and silently ignored, so
    /// <see cref="SpecMutationCatalogueTests"/> refuses one without a toolchain.
    /// </para>
    /// </summary>
    public IReadOnlyList<CfgAssignment> Bounds { get; init; } = [];

    /// <summary>
    /// The exact text TLC emits when <see cref="Target"/> is violated.
    /// <para>
    /// Note the asymmetry in the <see cref="SpecPropertyClass.Temporal"/> case:
    /// TLC does NOT name the property for a temporal violation, only for
    /// invariants and action properties. So a temporal mutation cannot be
    /// checked against the property's name, and the guard against a
    /// misattributed violation has to come from elsewhere - specifically from
    /// the generated cfg naming exactly one property, which the caller also
    /// asserts by counting violation lines. Worth stating rather than leaving
    /// as a silent gap, because "assert the banner names the property" is the
    /// rule everywhere else here and it simply cannot be applied to the
    /// temporal properties.
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
    /// <param name="baseSpecification">The base module's text.</param>
    /// <param name="baseModuleName">
    /// The base module's name, whose <c>MODULE</c> header the mutant renames to
    /// <see cref="Module"/>.
    /// </param>
    public string Apply(string baseSpecification, string baseModuleName)
    {
        ArgumentNullException.ThrowIfNull(baseSpecification);
        ArgumentException.ThrowIfNullOrEmpty(baseModuleName);

        // Normalised before anchoring. The anchors are stored with \n endings,
        // so on a CRLF checkout an un-normalised compare would fail to match
        // every multi-line anchor and report the whole catalogue as drifted -
        // a confusing failure with a cause nowhere near the message.
        var normalised = baseSpecification.ReplaceLineEndings("\n");

        var text = ReplaceExactlyOnce(
            normalised,
            $"MODULE {baseModuleName} ",
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
    /// <para>
    /// Everything else in the base cfg is carried over unchanged - the
    /// <c>SPECIFICATION</c> (or <c>INIT</c>/<c>NEXT</c>), <c>CONSTANTS</c>,
    /// <c>CONSTRAINT</c>, <c>SYMMETRY</c> and so on - so both arms check the
    /// same bounded instance the base model does, whatever directives a module
    /// uses to bound it. Only the checked-property blocks are replaced.
    /// </para>
    /// </summary>
    public string BuildConfig(string baseConfig) => BuildConfig(baseConfig, []);

    /// <summary>
    /// Builds the cfg for the mutant arm: <see cref="Orleans.Lattice.Tests.Formal.SpecMutation.BuildConfig(string)"/> plus a
    /// <c>CONSTANTS</c> block holding <see cref="Bounds"/>, so the mutant runs
    /// on the smaller instance its header declares while the control arm keeps
    /// the module's own bounds. Identical to <see cref="Orleans.Lattice.Tests.Formal.SpecMutation.BuildConfig(string)"/> for a
    /// mutation that declares no bounds.
    /// </summary>
    public string BuildMutantConfig(string baseConfig) => BuildConfig(baseConfig, Bounds);

    private string BuildConfig(string baseConfig, IReadOnlyList<CfgAssignment> bounds)
    {
        ArgumentNullException.ThrowIfNull(baseConfig);

        var builder = new StringBuilder();
        builder.AppendLine($"\\* Generated for mutation {Name}. Do not check this file in.");
        builder.AppendLine($"\\* Target: {Target} ({PropertyClass}).");
        builder.AppendLine();
        builder.AppendLine(SpecMutationCatalogue.CarriedConfiguration(baseConfig));
        builder.AppendLine();

        if (bounds.Count > 0)
        {
            builder.AppendLine("CONSTANTS");
            foreach (var bound in bounds)
            {
                builder.AppendLine($"    {bound.Name} = {bound.Value}");
            }

            builder.AppendLine();
        }

        builder.AppendLine("INVARIANTS");
        builder.AppendLine($"    {SpecMutationCatalogue.TypeInvariant}");
        if (PropertyClass == SpecPropertyClass.Invariant && Target != SpecMutationCatalogue.TypeInvariant)
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

    private string ReplaceExactlyOnce(string text, string find, string replace, string what)
    {
        var occurrences = CountOccurrences(text, find);
        if (occurrences != 1)
        {
            var diagnosis = occurrences == 0
                ? "The base specification no longer contains this text, so the mutation has drifted "
                  + "and must be re-derived against the current base module."
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
/// Reads the <c>*.mutation</c> files in a module's mutation directory.
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

    private static SpecMutation Parse(string path) =>
        Parse(Path.GetFileNameWithoutExtension(path), File.ReadAllText(path));

    /// <summary>
    /// Parses one mutation from its text. Separate from the file overload so
    /// the format's rules - in particular the ones that reject a malformed
    /// header - can be exercised without writing files.
    /// </summary>
    /// <param name="name">The mutation's name, used in error messages.</param>
    /// <param name="content">The full text of the <c>.mutation</c> file.</param>
    public static SpecMutation Parse(string name, string content)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(content);

        var lines = content.ReplaceLineEndings("\n").Split('\n');

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
            Perturbs = metadata.TryGetValue("PERTURBS", out var perturbs)
                ? perturbs.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                : [],
            DeadlockCheckDisabled = ParseDeadlock(metadata, name),
            Bounds = ParseBounds(metadata, name),
        };
    }

    /// <summary>
    /// Reads the optional <c>BOUNDS:</c> header: one or more comma-separated
    /// <c>Name = value</c> assignments, where the value is an integer or an
    /// identifier. A definition override (<c>&lt;-</c>), an empty entry, a
    /// repeated name or anything else is refused, because a malformed bound
    /// that TLC silently ignored would run the mutant at full size and look
    /// like a bound that worked.
    /// </summary>
    private static IReadOnlyList<CfgAssignment> ParseBounds(IDictionary<string, string> metadata, string name)
    {
        if (!metadata.TryGetValue("BOUNDS", out var value))
        {
            return [];
        }

        var bounds = new List<CfgAssignment>();
        foreach (var entry in value.Split(','))
        {
            var match = Regex.Match(entry.Trim(), @"^([A-Za-z][A-Za-z0-9_]*)\s*=\s*([0-9]+|[A-Za-z][A-Za-z0-9_]*)$");
            if (!match.Success)
            {
                throw new InvalidOperationException(
                    $"{name}.mutation declares 'BOUNDS: {value}'. Each entry must be 'Name = value', with an integer "
                    + "or identifier value, separated by commas.");
            }

            var bound = new CfgAssignment(match.Groups[1].Value, IsOverride: false, match.Groups[2].Value);
            if (bounds.Any(b => string.Equals(b.Name, bound.Name, StringComparison.Ordinal)))
            {
                throw new InvalidOperationException($"{name}.mutation declares the bound '{bound.Name}' twice.");
            }

            bounds.Add(bound);
        }

        return bounds;
    }

    /// <summary>
    /// Reads the optional <c>DEADLOCK:</c> header. The only accepted value is
    /// <c>off</c>: deadlock checking is on by default, so <c>on</c> would be a
    /// no-op that reads like a decision, and anything else is a typo that must
    /// not silently fall back to the default.
    /// </summary>
    private static bool ParseDeadlock(IDictionary<string, string> metadata, string name)
    {
        if (!metadata.TryGetValue("DEADLOCK", out var value))
        {
            return false;
        }

        return string.Equals(value, "off", StringComparison.Ordinal)
            ? true
            : throw new InvalidOperationException(
                $"{name}.mutation declares 'DEADLOCK: {value}'. The only accepted value is 'off'; omit the "
                + "header to keep TLC's deadlock check on.");
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
    /// The type invariant every module's base model must check, and that every
    /// generated cfg carries alongside its target so an out-of-domain mutation
    /// reports as itself rather than as the target.
    /// </summary>
    public const string TypeInvariant = "TypeOK";

    /// <summary>
    /// Every directive a TLC cfg may open a block with. Recognising all of them,
    /// not only the property blocks, is what stops a <c>CONSTRAINT</c> or
    /// <c>SYMMETRY</c> line after an <c>INVARIANTS</c> block being read as two
    /// more invariants.
    /// </summary>
    private static readonly Regex Directive = new(
        @"^(SPECIFICATION|INIT|NEXT|CONSTANTS?|INVARIANTS?|PROPERTY|PROPERTIES|CONSTRAINTS?|ACTION_CONSTRAINTS?|SYMMETRY|VIEW|ALIAS|POSTCONDITION|CHECK_DEADLOCK)\b(.*)$");

    /// <summary>
    /// The base cfg with its checked-property blocks and comments removed:
    /// everything a generated cfg carries over so that it checks the same
    /// bounded instance. Throws when the cfg names no behaviour to check, since
    /// a generated cfg without one would not run at all.
    /// </summary>
    public static string CarriedConfiguration(string baseConfig)
    {
        ArgumentNullException.ThrowIfNull(baseConfig);

        var kept = new List<string>();
        var skipping = false;
        foreach (var raw in baseConfig.ReplaceLineEndings("\n").Split('\n'))
        {
            var line = StripCfgComment(raw).TrimEnd();
            if (line.Trim().Length == 0)
            {
                continue;
            }

            var directive = Directive.Match(line.Trim());
            if (directive.Success)
            {
                skipping = directive.Groups[1].Value.StartsWith("INVARIANT", StringComparison.Ordinal)
                    || directive.Groups[1].Value.StartsWith("PROPERT", StringComparison.Ordinal);
            }

            if (!skipping)
            {
                kept.Add(line);
            }
        }

        if (!kept.Any(l => Regex.IsMatch(l.Trim(), @"^(SPECIFICATION|INIT)\b")))
        {
            throw new InvalidOperationException(
                "the base cfg declares neither SPECIFICATION nor INIT, so a generated cfg would name no "
                + "behaviour for TLC to check.");
        }

        return string.Join(Environment.NewLine, kept);
    }

    private static string StripCfgComment(string line)
    {
        var comment = line.IndexOf("\\*", StringComparison.Ordinal);
        return comment >= 0 ? line[..comment] : line;
    }

    /// <summary>
    /// Reads the property names the base model actually checks, so the
    /// completeness gate is driven by the model rather than by a list somebody
    /// has to remember to update. Adding a property to
    /// a module's base cfg without pairing it therefore fails.
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
            var line = StripCfgComment(raw).Trim();
            if (line.Length == 0)
            {
                continue;
            }

            var directive = Directive.Match(line);
            if (directive.Success)
            {
                var keyword = directive.Groups[1].Value;
                if (!keyword.StartsWith("INVARIANT", StringComparison.Ordinal)
                    && !keyword.StartsWith("PROPERT", StringComparison.Ordinal))
                {
                    current = null;
                    continue;
                }

                current = keyword.StartsWith("INVARIANT", StringComparison.Ordinal) ? invariants : properties;
                line = directive.Groups[2].Value.Trim();
                if (line.Length == 0)
                {
                    continue;
                }
            }

            // A CONSTANTS assignment line ('t1 = t1') keeps capturing off; only
            // bare identifier lists inside a checked-property block count.
            if (current is not null && Regex.IsMatch(line, @"^[A-Za-z][A-Za-z0-9_]*(\s+[A-Za-z][A-Za-z0-9_]*)*$"))
            {
                current.AddRange(Regex.Split(line, @"\s+"));
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

    /// <summary>
    /// Reads every assignment a cfg's <c>CONSTANT</c> / <c>CONSTANTS</c> blocks
    /// make: <c>Name = value</c> (a value for a declared constant, or for a
    /// defined operator, which TLC also accepts) and <c>Name &lt;- Other</c>
    /// (a definition override). Used by the variant gates, because TLC accepts
    /// an assignment to a name the specification does not have when it is
    /// written <c>Name = value</c>, and silently checks the unchanged model.
    /// </summary>
    /// <param name="config">The cfg text.</param>
    public static IReadOnlyList<CfgAssignment> ReadConstantAssignments(string config)
    {
        ArgumentNullException.ThrowIfNull(config);

        var assignments = new List<CfgAssignment>();
        var inConstants = false;

        foreach (var raw in config.ReplaceLineEndings("\n").Split('\n'))
        {
            var line = StripCfgComment(raw).Trim();
            if (line.Length == 0)
            {
                continue;
            }

            var directive = Directive.Match(line);
            if (directive.Success)
            {
                inConstants = directive.Groups[1].Value.StartsWith("CONSTANT", StringComparison.Ordinal);
                line = directive.Groups[2].Value.Trim();
                if (!inConstants || line.Length == 0)
                {
                    continue;
                }
            }

            if (!inConstants)
            {
                continue;
            }

            var assignment = Regex.Match(line, @"^([A-Za-z][A-Za-z0-9_]*)\s*(=|<-)\s*(\S.*)$");
            if (assignment.Success)
            {
                assignments.Add(new CfgAssignment(
                    assignment.Groups[1].Value,
                    assignment.Groups[2].Value == "<-",
                    assignment.Groups[3].Value.Trim()));
            }
        }

        return assignments;
    }
}
