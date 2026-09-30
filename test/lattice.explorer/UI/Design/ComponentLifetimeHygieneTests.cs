using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// The Explorer's lifetime gate (issue #4011): nothing under the Explorer UI disposes a
/// <see cref="CancellationTokenSource"/> by hand, and no component declares one of its own.
/// </summary>
/// <remarks>
/// <para>
/// A read already on its way when its component is left resumes after <c>Dispose</c>. If
/// <c>Dispose</c> disposed the component's source, the next read of its token throws
/// <see cref="ObjectDisposedException"/> out of a lifecycle method, and the Blazor circuit is
/// terminated: the console stays on screen and answers nothing. Components hand their work
/// the token of an <c>ComponentLifetime</c>, which cancels and never disposes.
/// </para>
/// <para>
/// A source a method creates with <c>using var</c> and awaits within its own scope is safe
/// and allowed: nothing outlives the method to read it.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ComponentLifetimeHygieneTests
{
    private const string ShellSourceRoot = "src/lattice.explorer/UI";

    // A field, parameter or local declared with the source's type.
    private static readonly Regex Declared = new(
        @"\bCancellationTokenSource\??\s+(?<name>@?\w+)\s*(?:=|;|,|\))",
        RegexOptions.Compiled);

    // A source created into a var local, possibly also assigned to a field.
    private static readonly Regex Created = new(
        @"\bvar\s+(?<name>\w+)\s*=\s*(?:\w+\s*=\s*)?(?:new\s+CancellationTokenSource\b|CancellationTokenSource\.CreateLinkedTokenSource\b)",
        RegexOptions.Compiled);

    // A source whose disposal is the method's own scope.
    private static readonly Regex Scoped = new(
        @"\busing\s+var\s+(?<name>\w+)\s*=\s*(?:new\s+CancellationTokenSource\b|CancellationTokenSource\.CreateLinkedTokenSource\b)",
        RegexOptions.Compiled);

    // A component field of the source's type: components use ComponentLifetime.
    private static readonly Regex ComponentField = new(
        @"^\s*(?:private|internal|protected|public)\s+(?:readonly\s+)?CancellationTokenSource\??\s+\w+",
        RegexOptions.Compiled | RegexOptions.Multiline);

    [Test]
    public void Nothing_under_the_explorer_ui_disposes_a_cancellation_token_source_by_hand()
    {
        var violations = new List<string>();
        var scanned = 0;
        foreach (var file in Sources())
        {
            scanned++;
            violations.AddRange(FindDisposals(File.ReadAllText(file)).Select(line => $"{Relative(file)}: {line}"));
        }

        Assert.That(scanned, Is.GreaterThan(100), "the scan must reach the Explorer UI's sources");
        Assert.That(violations, Is.Empty,
            "A component's work is cancelled, never disposed: a read that resumes after the component is gone "
            + "reads the token again, and a disposed source then throws and ends the circuit. Hold the work's token "
            + "in a ComponentLifetime (Token, Renew, IsLeft, Leave) instead."
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void No_explorer_component_declares_its_own_cancellation_token_source()
    {
        var violations = new List<string>();
        var components = 0;
        foreach (var file in Sources().Where(IsComponent))
        {
            components++;
            foreach (Match match in ComponentField.Matches(File.ReadAllText(file)))
            {
                violations.Add($"{Relative(file)}: {match.Value.Trim()}");
            }
        }

        Assert.That(components, Is.GreaterThan(50), "the scan must reach the Explorer UI's components");
        Assert.That(violations, Is.Empty,
            "A component cancels its work through a ComponentLifetime, which is never disposed."
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_detector_finds_the_disposals_it_is_shown()
    {
        // Battery test for the smoke detector.
        Assert.Multiple(() =>
        {
            Assert.That(FindDisposals("private readonly CancellationTokenSource _lifetime = new();\n_lifetime.Cancel();\n_lifetime.Dispose();"), Has.Count.EqualTo(1));
            Assert.That(FindDisposals("private CancellationTokenSource? _load;\n_load?.Dispose();"), Has.Count.EqualTo(1));
            Assert.That(FindDisposals("CancellationTokenSource _x = new();\nawait _x.DisposeAsync();"), Has.Count.EqualTo(1));
            Assert.That(FindDisposals("var run = CancellationTokenSource.CreateLinkedTokenSource(token);\nrun.Dispose();"), Has.Count.EqualTo(1));
            Assert.That(FindDisposals("var load = _load = new CancellationTokenSource();\nload.Dispose();"), Has.Count.EqualTo(1));
            Assert.That(FindDisposals("private CancellationTokenSource? _run;\nvar run = _run;\nrun.Dispose();"), Has.Count.EqualTo(1));
            Assert.That(FindDisposals("void F(CancellationTokenSource following) { following.Dispose(); }"), Has.Count.EqualTo(1));
            Assert.That(FindDisposals("private readonly CancellationTokenSource _x = new();\nusing (_x) { }"), Has.Count.EqualTo(1));

            Assert.That(FindDisposals("using var deadline = new CancellationTokenSource(timeout);\nawait Task.Delay(1, deadline.Token);"), Is.Empty);
            Assert.That(FindDisposals("private readonly ComponentLifetime _lifetime = new();\n_lifetime.Leave();\n_module.Dispose();"), Is.Empty);
            Assert.That(FindDisposals("private CancellationTokenSource Replace() => new();\n_timer.Dispose();"), Is.Empty);

            Assert.That(ComponentField.IsMatch("    private readonly CancellationTokenSource _lifetime = new();"), Is.True);
            Assert.That(ComponentField.IsMatch("    private CancellationTokenSource? _load;"), Is.True);
            Assert.That(ComponentField.IsMatch("    private readonly ComponentLifetime _lifetime = new();"), Is.False);
        });
    }

    private static List<string> FindDisposals(string source)
    {
        var scoped = Scoped.Matches(source).Select(match => match.Groups["name"].Value).ToHashSet(StringComparer.Ordinal);
        var names = Declared.Matches(source)
            .Concat(Created.Matches(source))
            .Select(match => match.Groups["name"].Value.TrimStart('@'))
            .Where(name => !scoped.Contains(name))
            .ToHashSet(StringComparer.Ordinal);

        // Follow aliases (var run = _run;) until no new name appears.
        int before;
        do
        {
            before = names.Count;
            foreach (var name in names.ToArray())
            {
                foreach (Match alias in Regex.Matches(source, @"\bvar\s+(?<alias>\w+)\s*=\s*" + Regex.Escape(name) + @"\s*;"))
                {
                    names.Add(alias.Groups["alias"].Value);
                }
            }
        }
        while (names.Count != before);

        var found = new List<string>();
        var lines = source.Split('\n');
        foreach (var name in names)
        {
            var disposal = new Regex(
                @"(?:\b" + Regex.Escape(name) + @"\??\.Dispose(?:Async)?\s*\()|(?:\busing\s*\(\s*" + Regex.Escape(name) + @"\s*\))");
            for (var i = 0; i < lines.Length; i++)
            {
                if (disposal.IsMatch(lines[i]))
                {
                    found.Add($"{i + 1}: {lines[i].Trim()}");
                }
            }
        }

        return found;
    }

    private static IEnumerable<string> Sources()
    {
        var root = Path.Combine(HygieneRepository.FindRepoRoot(), ShellSourceRoot.Replace('/', Path.DirectorySeparatorChar));
        return HygieneRepository.EnumerateFiles(root, "*.cs").Concat(HygieneRepository.EnumerateFiles(root, "*.razor"));
    }

    private static bool IsComponent(string file) =>
        file.EndsWith(".razor", StringComparison.OrdinalIgnoreCase)
        || file.EndsWith(".razor.cs", StringComparison.OrdinalIgnoreCase);

    private static string Relative(string file) =>
        Path.GetRelativePath(HygieneRepository.FindRepoRoot(), file).Replace('\\', '/');
}
