using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Every parameter a tracked PowerShell script documents in its comment-based
/// help (<c>.PARAMETER Name</c>) must be declared in that script's
/// <c>param(...)</c> block.
/// </summary>
/// <remarks>
/// Regression for <c>benchmark/performance-report.ps1</c>: #3597 deleted its
/// <c>[string] $NamePrefix</c> declaration but kept the help entry and two
/// reads of <c>$NamePrefix</c>. The script runs under
/// <c>Set-StrictMode -Version Latest</c>, where reading an undeclared variable
/// throws, so every Layer 1 and Layer 2 run and every self-provisioning
/// Layer 3 run stopped at startup, and passing <c>-NamePrefix</c> failed
/// parameter binding. Nothing compiles a script, so a dropped declaration is
/// invisible until somebody pays for a cloud run to find it.
/// </remarks>
[TestFixture]
public sealed class PowerShellScriptParameterHygieneTests
{
    private const string RegressionScript = "benchmark/performance-report.ps1";

    private static readonly Regex DocumentedParameterRegex = new(
        @"^[ \t]*\.PARAMETER[ \t]+(?<name>[A-Za-z_][A-Za-z0-9_]*)",
        RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

    private static readonly Regex ScriptParamOpenRegex = new(
        @"^[ \t]*param[ \t]*\(",
        RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

    private static readonly Regex FirstFunctionRegex = new(
        @"^[ \t]*(function|filter)[ \t]+",
        RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

    private static readonly Regex VariableRegex = new(
        @"\$(?<name>[A-Za-z_][A-Za-z0-9_]*)",
        RegexOptions.Compiled);

    [Test]
    public void Every_documented_script_parameter_is_declared()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var violations = new List<string>();
        var examined = new List<string>();

        foreach (var path in HygieneRepository.EnumerateFiles(repoRoot, "*.ps1"))
        {
            var relative = Path.GetRelativePath(repoRoot, path).Replace('\\', '/');
            var result = Analyse(File.ReadAllText(path, Encoding.UTF8));
            if (result is null)
            {
                continue;
            }

            examined.Add(relative);
            foreach (var missing in result.Undeclared)
            {
                violations.Add($"{relative}: .PARAMETER {missing} is documented but not declared in param(...)");
            }
        }

        Assert.That(examined, Is.Not.Empty,
            "no tracked PowerShell script with comment-based parameter help was examined; the scan is vacuous");
        Assert.That(examined, Does.Contain(RegressionScript),
            $"{RegressionScript} is the regression target of this gate and must be examined");
        Assert.That(violations, Is.Empty,
            "Declare every documented parameter (or delete its help entry and every read of it). Under "
            + "Set-StrictMode -Version Latest a read of an undeclared variable throws at run time:\n"
            + string.Join("\n", violations));
    }

    [Test]
    public void Analyse_reports_a_documented_parameter_the_param_block_does_not_declare()
    {
        const string script = """
            <#
            .SYNOPSIS
                Probe.
            .PARAMETER Kept
                Declared.
            .PARAMETER Dropped
                Documented only.
            #>
            [CmdletBinding()]
            param(
                # A comment naming $Dropped (and an unbalanced ')' ) must not count as a declaration.
                [ValidateSet('a', 'b)')]
                [string] $Kept = 'x'
            )
            Set-StrictMode -Version Latest
            if ($Dropped) { 'read' }
            """;

        var result = Analyse(script);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Undeclared, Is.EqualTo(new[] { "Dropped" }));
    }

    [Test]
    public void Analyse_accepts_a_script_whose_documented_parameters_are_all_declared()
    {
        const string script = """
            <#
            .PARAMETER Name
                Declared.
            #>
            param([string] $name)
            """;

        var result = Analyse(script);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Undeclared, Is.Empty);
    }

    [Test]
    public void Analyse_skips_help_that_belongs_to_a_function()
    {
        const string script = """
            function Invoke-Thing {
                <#
                .PARAMETER Missing
                    Belongs to the function, not the script.
                #>
                param([string] $Other)
            }
            """;

        Assert.That(Analyse(script), Is.Null);
    }

    /// <summary>
    /// Returns the documented-but-undeclared parameters of a script whose
    /// comment-based help documents at least one parameter, or <see langword="null"/>
    /// when the script carries no script-level parameter help to check.
    /// </summary>
    private static AnalysisResult? Analyse(string text)
    {
        var helpStart = text.IndexOf("<#", StringComparison.Ordinal);
        if (helpStart < 0)
        {
            return null;
        }

        var helpEnd = text.IndexOf("#>", helpStart + 2, StringComparison.Ordinal);
        if (helpEnd < 0)
        {
            return null;
        }

        // Blank the help block so neither its prose nor its examples can be
        // mistaken for a function definition or a param block.
        var code = new StringBuilder(text);
        for (var i = helpStart; i < helpEnd + 2; i++)
        {
            if (code[i] != '\n')
            {
                code[i] = ' ';
            }
        }
        var codeText = code.ToString();

        var firstFunction = FirstFunctionRegex.Match(codeText);
        var scriptLevelEnd = firstFunction.Success ? firstFunction.Index : codeText.Length;
        if (helpStart >= scriptLevelEnd)
        {
            // Help that follows a function definition documents that function.
            return null;
        }

        var documented = DocumentedParameterRegex
            .Matches(text[helpStart..helpEnd])
            .Select(m => m.Groups["name"].Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (documented.Count == 0)
        {
            return null;
        }

        var declared = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var open = ScriptParamOpenRegex.Match(codeText);
        if (open.Success && open.Index < scriptLevelEnd)
        {
            var body = ExtractCodeOfBalancedGroup(codeText, open.Index + open.Length);
            foreach (Match variable in VariableRegex.Matches(body))
            {
                declared.Add(variable.Groups["name"].Value);
            }
        }

        return new AnalysisResult(documented.Where(name => !declared.Contains(name)).ToList());
    }

    /// <summary>
    /// Returns the code inside the parenthesised group opened just before
    /// <paramref name="start"/>, with comments and string literals blanked so a
    /// parenthesis or a <c>$name</c> inside either is neither counted nor read
    /// as a declaration.
    /// </summary>
    private static string ExtractCodeOfBalancedGroup(string text, int start)
    {
        var code = new StringBuilder();
        var depth = 1;
        var i = start;
        while (i < text.Length)
        {
            var c = text[i];
            if (c == '#')
            {
                while (i < text.Length && text[i] != '\n')
                {
                    i++;
                }
                code.Append('\n');
                continue;
            }

            if (c == '<' && i + 1 < text.Length && text[i + 1] == '#')
            {
                var close = text.IndexOf("#>", i + 2, StringComparison.Ordinal);
                i = close < 0 ? text.Length : close + 2;
                code.Append(' ');
                continue;
            }

            if (c == '\'' || c == '"')
            {
                i++;
                while (i < text.Length && text[i] != c)
                {
                    i++;
                }
                i++;
                code.Append(' ');
                continue;
            }

            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
                if (depth == 0)
                {
                    break;
                }
            }

            code.Append(c);
            i++;
        }

        return code.ToString();
    }

    private sealed record AnalysisResult(IReadOnlyList<string> Undeclared);
}
