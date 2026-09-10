using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Resolves the <c>Fixture.TestMethod</c> names cited as detectors by the
/// Detector column of <c>spec/Refinement.md</c> against the C# under
/// <c>test/</c>, using nothing but the source text.
/// <para>
/// WHY THIS EXISTS. A refinement row claims the spec abstracts a production
/// behaviour. Issue #2527 asks whether that claim is falsifiable: if the
/// production behaviour broke, would anything go red? The note answers that per
/// row by naming a detecting test. A named test that does not exist, or that
/// stops existing after a rename, turns the answer back into unchecked prose -
/// which is the failure the parent audit (#2299) keeps finding. This resolver
/// is what stops that happening silently.
/// </para>
/// <para>
/// A NAMED TEST IS NOT A TLA+ MUTATION, AND THE DISTINCTION IS THE POINT. Every
/// property the base model checks already has a paired <c>spec/mutations</c>
/// file, guaranteed by
/// <c>SpecMutationCatalogueTests.Every_property_the_base_model_checks_has_a_mutation</c>.
/// Those mutations perturb the SPEC. They would not notice a production
/// regression at all, so they cannot serve as a row's detector however
/// convenient it would be to cite them. Only a test over production code can,
/// which is why this resolver indexes <c>test/</c> and not <c>spec/</c>.
/// </para>
/// <para>
/// SOURCE TEXT RATHER THAN REFLECTION, for the same reasons as
/// <see cref="RefinementSymbolResolver"/>: the fixtures are ordinary internal
/// classes, and a reflection-based answer depends on which assemblies happen to
/// be loaded, which is a poor property for a check whose entire job is to be
/// trustworthy when it fails.
/// </para>
/// <para>
/// SCOPING. A method is attributed to the innermost type whose braces enclose
/// it, tracked by scanning brace depth over source with comments and string
/// literals removed. The obvious cheaper rule - "the nearest preceding type
/// declaration" - is WRONG here and was measurably so: fixtures in this
/// repository routinely declare a private helper class near the top of the
/// file, after which every test below it is attributed to the helper. Two of
/// the first five fixtures cited by the note hit exactly that
/// (<c>TxRegistryGrainTests</c> declares <c>ManualTimeProvider</c>,
/// <c>ShardRootGrainTxTerminalTests</c> declares <c>Harness</c>), so the
/// heuristic reported real, correctly-named tests as missing. A gate that
/// cries wolf is worse than no gate, because the repair is to delete the
/// claim.
/// </para>
/// </summary>
internal sealed class RefinementDetectorResolver
{
    /// <summary>
    /// Any NUnit attribute that makes the method below it an executable test.
    /// <c>TestCase</c> and <c>TestCaseSource</c> are included because a
    /// parameterised fixture is every bit as much a detector as a bare
    /// <c>[Test]</c>, and excluding them would push authors away from the
    /// better-factored form.
    /// </summary>
    private static readonly Regex TestAttribute = new(
        @"^\s*\[\s*(Test|TestCase|TestCaseSource|Theory)\b",
        RegexOptions.Compiled);

    private static readonly Regex TypeDeclaration = new(
        @"\b(?:class|struct|record(?:\s+(?:class|struct))?)\s+([A-Za-z_][A-Za-z0-9_]*)",
        RegexOptions.Compiled);

    private static readonly Regex MethodDeclaration = new(
        @"\b([A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^>()]*>)?\s*\(",
        RegexOptions.Compiled);

    private readonly string _testRoot;
    private readonly HashSet<string> _tests = new(StringComparer.Ordinal);
    private readonly HashSet<string> _fixtures = new(StringComparer.Ordinal);

    /// <summary>
    /// Indexes every NUnit-attributed method under <paramref name="testRoot"/>
    /// in a single pass. The whole tree is read once because the answer is a
    /// set membership test and the set is small; there is no lazy path to be
    /// clever about.
    /// </summary>
    public RefinementDetectorResolver(string testRoot)
    {
        ArgumentNullException.ThrowIfNull(testRoot);
        _testRoot = testRoot;

        foreach (var path in HygieneRepository.EnumerateFiles(testRoot, "*.cs"))
        {
            IndexFile(path);
        }
    }

    /// <summary>A resolver over the repository's own <c>test/</c> tree.</summary>
    public static RefinementDetectorResolver ForRepository() =>
        new(Path.Combine(HygieneRepository.FindRepoRoot(), "test"));

    /// <summary>The test root this resolver was built over.</summary>
    public string TestRoot => _testRoot;

    /// <summary>How many attributed test methods were indexed.</summary>
    public int IndexedTestCount => _tests.Count;

    /// <summary>True when a fixture of that name declares an attributed test of that name.</summary>
    public bool TestExists(string fixture, string method)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        ArgumentNullException.ThrowIfNull(method);
        return _tests.Contains($"{fixture}.{method}");
    }

    /// <summary>True when any file declares a type of that name.</summary>
    public bool FixtureExists(string fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        return _fixtures.Contains(fixture);
    }

    /// <summary>
    /// Explains a failed resolution, distinguishing "no such fixture" from
    /// "fixture exists but declares no such test". The two call for different
    /// repairs, so a message that conflates them makes the reader redo the
    /// bisection the resolver already did.
    /// </summary>
    public string Explain(string fixture, string method)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        ArgumentNullException.ThrowIfNull(method);

        if (TestExists(fixture, method))
        {
            return $"'{fixture}.{method}' resolves to an attributed test method.";
        }

        return FixtureExists(fixture)
            ? $"fixture '{fixture}' exists under {_testRoot}, but declares no NUnit-attributed " +
              $"method named '{method}' (renamed, removed, or the attribute was dropped)."
            : $"no fixture named '{fixture}' is declared anywhere under {_testRoot}.";
    }

    private void IndexFile(string path)
    {
        var lines = StripCommentsAndLiterals(File.ReadAllText(path))
            .ReplaceLineEndings("\n")
            .Split('\n');

        var scopes = new Stack<(string Name, int Depth)>();
        var depth = 0;
        string? pendingType = null;
        var attributed = false;

        foreach (var raw in lines)
        {
            var typeMatch = TypeDeclaration.Match(raw);
            if (typeMatch.Success)
            {
                pendingType = typeMatch.Groups[1].Value;
                _fixtures.Add(pendingType);
            }
            else if (TestAttribute.IsMatch(raw))
            {
                attributed = true;
            }
            else if (attributed)
            {
                var trimmed = raw.Trim();
                if (trimmed.Length > 0 && !trimmed.StartsWith('['))
                {
                    var methodMatch = MethodDeclaration.Match(raw);
                    if (methodMatch.Success && scopes.Count > 0)
                    {
                        _tests.Add($"{scopes.Peek().Name}.{methodMatch.Groups[1].Value}");
                    }

                    attributed = false;
                }
            }

            foreach (var c in raw)
            {
                if (c == '{')
                {
                    depth++;
                    if (pendingType is not null)
                    {
                        scopes.Push((pendingType, depth));
                        pendingType = null;
                    }
                }
                else if (c == '}')
                {
                    if (scopes.Count > 0 && scopes.Peek().Depth == depth)
                    {
                        scopes.Pop();
                    }

                    depth--;
                }
            }
        }
    }

    /// <summary>
    /// Replaces comments and string/char literals with nothing, preserving line
    /// structure, so that brace counting sees only real block delimiters. Done
    /// with a scanner rather than regexes because the two interact: a quote
    /// inside a comment breaks naive literal stripping, and a <c>//</c> inside
    /// a string (a URL, say) breaks naive comment stripping. Either mistake
    /// unbalances the brace count and silently mis-scopes the rest of a file.
    /// </summary>
    private static string StripCommentsAndLiterals(string text)
    {
        var sb = new System.Text.StringBuilder(text.Length);
        var i = 0;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                while (i < text.Length && text[i] != '\n') { i++; }
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                i += 2;
                while (i + 1 < text.Length && !(text[i] == '*' && text[i + 1] == '/'))
                {
                    if (text[i] == '\n') { sb.Append('\n'); }
                    i++;
                }

                i = Math.Min(i + 2, text.Length);
                continue;
            }

            if (c == '"' && i + 2 < text.Length && text[i + 1] == '"' && text[i + 2] == '"')
            {
                var fence = 0;
                while (i + fence < text.Length && text[i + fence] == '"') { fence++; }
                i += fence;

                while (i < text.Length)
                {
                    if (text[i] == '"')
                    {
                        var run = 0;
                        while (i + run < text.Length && text[i + run] == '"') { run++; }
                        i += run;
                        if (run >= fence) { break; }
                        continue;
                    }

                    if (text[i] == '\n') { sb.Append('\n'); }
                    i++;
                }

                continue;
            }

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                i += 2;
                while (i < text.Length)
                {
                    if (text[i] == '"')
                    {
                        if (i + 1 < text.Length && text[i + 1] == '"') { i += 2; continue; }
                        i++;
                        break;
                    }

                    if (text[i] == '\n') { sb.Append('\n'); }
                    i++;
                }

                continue;
            }

            if (c is '"' or '\'')
            {
                var quote = c;
                i++;
                while (i < text.Length && text[i] != quote && text[i] != '\n')
                {
                    i += text[i] == '\\' ? 2 : 1;
                }

                if (i < text.Length && text[i] == quote) { i++; }
                continue;
            }

            sb.Append(c);
            i++;
        }

        return sb.ToString();
    }
}
