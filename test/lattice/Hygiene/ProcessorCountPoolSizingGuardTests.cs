using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Guards the class of defect behind issue #2613: a pool sized from
/// <see cref="System.Environment.ProcessorCount"/> while the process runs under a
/// fractional cgroup CPU grant will oversubscribe and be throttled. One instance
/// (the ONNX Runtime intra-op pool) cost a measured 33-110x embedding-throughput
/// loss before issue #2610 corrected it. This fixture makes the whole class
/// visible: every <c>Environment.ProcessorCount</c> code site under <c>src/</c>
/// and <c>apps/</c> must either consult the shared
/// <see cref="Api.Mcp.RepoContext.ContainerCpuGrant"/> reader in the same
/// statement, or carry a documented, reasoned <c>// grant-exempt: &lt;reason&gt;</c>
/// marker on its own line or the line immediately above it.
/// </summary>
/// <remarks>
/// <para>
/// The scan is scoped to production code (<c>src/</c> and <c>apps/</c>) because a
/// pool oversubscribed in a deployed container is the defect; a test sizing a
/// pool is not a deployment concern. It strips comments and string literals
/// before matching, so a <c>&lt;see cref&gt;</c> reference or a diagnostic string
/// that merely names the API is not a site.
/// </para>
/// <para>
/// Two properties make this a guard rather than a decoration, and both are
/// tested here. It fails loudly when its own enumeration matches nothing
/// (non-vacuity), copying the shape of <c>MeterFieldDeclarationOrderTests</c>;
/// and <see cref="Scanner_distinguishes_a_consulted_exempt_and_unguarded_site"/>
/// exercises the classifier against synthetic bodies so the passing repository
/// corpus cannot be mistaken for a scanner that never fires.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ProcessorCountPoolSizingGuardTests
{
    private const string Token = "Environment.ProcessorCount";
    private const string GrantReader = "ContainerCpuGrant";

    private static readonly Regex GrantExemptMarker = new(
        @"//\s*grant-exempt:\s*\S", RegexOptions.Compiled);

    [Test]
    public void Every_processor_count_site_consults_the_grant_or_is_documented_exempt()
    {
        var root = HygieneRepository.FindRepoRoot();
        var scanRoots = new[] { Path.Combine(root, "src"), Path.Combine(root, "apps") };

        var violations = new List<string>();
        var filesScanned = 0;
        var totalSites = 0;

        foreach (var scanRoot in scanRoots)
        {
            foreach (var file in HygieneRepository.EnumerateFiles(scanRoot, "*.cs"))
            {
                filesScanned++;
                var raw = File.ReadAllText(file);
                var (total, unguarded) = Evaluate(raw);
                totalSites += total;
                foreach (var line in unguarded)
                {
                    violations.Add($"{Path.GetRelativePath(root, file)}:{line}");
                }
            }
        }

        Assert.That(filesScanned, Is.GreaterThan(0),
            "The scan found no .cs files under src/ or apps/. The scope has drifted, so this "
            + "guard is silently vacuous.");

        Assert.That(totalSites, Is.GreaterThan(0),
            "The scan found no Environment.ProcessorCount code site anywhere under src/ or apps/. "
            + "Either the token or the comment/string stripping has drifted, so this guard is "
            + "silently vacuous - it would pass no matter what a new pool-sizing site did.");

        Assert.That(violations, Is.Empty,
            "A pool sized from Environment.ProcessorCount ignores the enforced container CPU grant. "
            + "Under a fractional cgroup grant this oversubscribes and is throttled (issue #2613). "
            + "Fix the site to consult Orleans.Lattice.Api.Mcp.RepoContext.ContainerCpuGrant.Read() "
            + "in the same statement, e.g. Math.Max(1, ContainerCpuGrant.Read() ?? "
            + "Environment.ProcessorCount); or, if the site genuinely must not (a reporting or "
            + "diagnostic use, or a deliberate architectural exception), add a "
            + "'// grant-exempt: <reason>' marker on that line or the line above.\n"
            + string.Join("\n", violations));
    }

    /// <summary>
    /// Proves the classifier flags an unguarded site and clears a consulted or
    /// exempt one, and that occurrences inside comments and string literals are
    /// not counted as sites. Without this arm the repository scan passing would
    /// be indistinguishable from a classifier that never fires.
    /// </summary>
    [Test]
    public void Scanner_distinguishes_a_consulted_exempt_and_unguarded_site()
    {
        const string consulted =
            "class A {\n"
            + "  void M() {\n"
            + "    var o = new ParallelOptions {\n"
            + "      MaxDegreeOfParallelism = Math.Max(1, ContainerCpuGrant.Read() ?? Environment.ProcessorCount),\n"
            + "    };\n"
            + "  }\n"
            + "}\n";

        const string exemptSameLine =
            "class A {\n"
            + "  void M() {\n"
            + "    int n = Environment.ProcessorCount; // grant-exempt: diagnostic value, not a pool.\n"
            + "  }\n"
            + "}\n";

        const string exemptLineAbove =
            "class A {\n"
            + "  void M() {\n"
            + "    // grant-exempt: the library must not read the cgroup here (issue #2279).\n"
            + "    int n = Environment.ProcessorCount;\n"
            + "  }\n"
            + "}\n";

        const string unguarded =
            "class A {\n"
            + "  void M() {\n"
            + "    int n = Environment.ProcessorCount;\n"
            + "  }\n"
            + "}\n";

        const string bareMarker =
            "class A {\n"
            + "  void M() {\n"
            + "    int n = Environment.ProcessorCount; // grant-exempt:\n"
            + "  }\n"
            + "}\n";

        const string inComment =
            "class A {\n"
            + "  // sizes from Environment.ProcessorCount by default\n"
            + "  void M() { }\n"
            + "}\n";

        const string inString =
            "class A {\n"
            + "  string s = \"Environment.ProcessorCount reports {n}\";\n"
            + "}\n";

        Assert.Multiple(() =>
        {
            Assert.That(Evaluate(consulted).Unguarded, Is.Empty,
                "A site consulting ContainerCpuGrant in the same statement must be satisfied.");
            Assert.That(Evaluate(exemptSameLine).Unguarded, Is.Empty,
                "A grant-exempt marker on the same line must satisfy the site.");
            Assert.That(Evaluate(exemptLineAbove).Unguarded, Is.Empty,
                "A grant-exempt marker on the line above must satisfy the site.");

            Assert.That(Evaluate(unguarded).Total, Is.EqualTo(1),
                "The unguarded site must be counted as a code site.");
            Assert.That(Evaluate(unguarded).Unguarded, Is.Not.Empty,
                "An unguarded Environment.ProcessorCount site must be flagged - this is the whole point.");

            Assert.That(Evaluate(bareMarker).Unguarded, Is.Not.Empty,
                "A grant-exempt marker with no reason after the colon must not satisfy the site.");

            Assert.That(Evaluate(inComment).Total, Is.EqualTo(0),
                "An occurrence inside a comment is not a code site.");
            Assert.That(Evaluate(inString).Total, Is.EqualTo(0),
                "An occurrence inside a string literal is not a code site.");
        });
    }

    /// <summary>
    /// Classifies every <see cref="Token"/> code site in <paramref name="raw"/>,
    /// returning the total number of code sites and the 1-based line numbers of
    /// those that neither consult the grant reader nor carry a documented
    /// exemption marker.
    /// </summary>
    private static (int Total, IReadOnlyList<int> Unguarded) Evaluate(string raw)
    {
        var text = raw.Replace("\r\n", "\n").Replace('\r', '\n');
        var code = StripCommentsAndStrings(text);
        var rawLines = text.Split('\n');

        var unguarded = new List<int>();
        var total = 0;

        var from = 0;
        int idx;
        while ((idx = code.IndexOf(Token, from, StringComparison.Ordinal)) >= 0)
        {
            total++;
            var lineIndex = CountNewlines(code, idx);

            var start = idx;
            while (start > 0 && !IsStatementBoundary(code[start - 1])) start--;
            var end = idx;
            while (end < code.Length && !IsStatementBoundary(code[end])) end++;
            var statement = code.Substring(start, end - start);

            var consults = statement.Contains(GrantReader, StringComparison.Ordinal);
            var exempt = HasMarker(rawLines, lineIndex)
                || (lineIndex > 0 && HasMarker(rawLines, lineIndex - 1));

            if (!consults && !exempt)
            {
                unguarded.Add(lineIndex + 1);
            }

            from = idx + Token.Length;
        }

        return (total, unguarded);
    }

    private static bool IsStatementBoundary(char c) => c is ';' or '{' or '}';

    private static bool HasMarker(string[] rawLines, int lineIndex) =>
        lineIndex >= 0 && lineIndex < rawLines.Length && GrantExemptMarker.IsMatch(rawLines[lineIndex]);

    private static int CountNewlines(string s, int upTo)
    {
        var count = 0;
        for (var i = 0; i < upTo; i++)
        {
            if (s[i] == '\n') count++;
        }
        return count;
    }

    /// <summary>
    /// Replaces every comment, string literal, and character literal with spaces
    /// (preserving newlines and overall length) so a token search sees only
    /// executable code. Handles line and block comments, regular and verbatim and
    /// interpolated string literals, and char literals. Raw string literals are
    /// not used by any scanned site and are not special-cased.
    /// </summary>
    private static string StripCommentsAndStrings(string s)
    {
        var sb = new StringBuilder(s.Length);
        var n = s.Length;
        var i = 0;

        while (i < n)
        {
            var c = s[i];

            if (c == '/' && i + 1 < n && s[i + 1] == '/')
            {
                while (i < n && s[i] != '\n') { sb.Append(' '); i++; }
                continue;
            }

            if (c == '/' && i + 1 < n && s[i + 1] == '*')
            {
                sb.Append("  ");
                i += 2;
                while (i < n && !(s[i] == '*' && i + 1 < n && s[i + 1] == '/'))
                {
                    sb.Append(s[i] == '\n' ? '\n' : ' ');
                    i++;
                }
                if (i < n) { sb.Append("  "); i += 2; }
                continue;
            }

            if (c == '@' || c == '$')
            {
                var j = i;
                var verbatim = false;
                while (j < n && (s[j] == '@' || s[j] == '$'))
                {
                    if (s[j] == '@') verbatim = true;
                    j++;
                }
                if (j < n && s[j] == '"')
                {
                    for (var p = i; p <= j; p++) sb.Append(' ');
                    i = j + 1;
                    i = ConsumeStringBody(s, i, sb, verbatim);
                    continue;
                }
            }

            if (c == '"')
            {
                sb.Append(' ');
                i++;
                i = ConsumeStringBody(s, i, sb, verbatim: false);
                continue;
            }

            if (c == '\'')
            {
                sb.Append(' ');
                i++;
                while (i < n && s[i] != '\'')
                {
                    if (s[i] == '\\' && i + 1 < n) { sb.Append("  "); i += 2; continue; }
                    sb.Append(s[i] == '\n' ? '\n' : ' ');
                    i++;
                }
                if (i < n) { sb.Append(' '); i++; }
                continue;
            }

            sb.Append(c);
            i++;
        }

        return sb.ToString();
    }

    /// <summary>
    /// Consumes a string body starting at <paramref name="i"/> (just past the
    /// opening quote), appending spaces for its content and the closing quote,
    /// and returns the index just past the closing quote.
    /// </summary>
    private static int ConsumeStringBody(string s, int i, StringBuilder sb, bool verbatim)
    {
        var n = s.Length;
        while (i < n)
        {
            var c = s[i];
            if (verbatim)
            {
                if (c == '"')
                {
                    if (i + 1 < n && s[i + 1] == '"') { sb.Append("  "); i += 2; continue; }
                    sb.Append(' ');
                    i++;
                    return i;
                }
                sb.Append(c == '\n' ? '\n' : ' ');
                i++;
            }
            else
            {
                if (c == '\\' && i + 1 < n) { sb.Append("  "); i += 2; continue; }
                if (c == '"') { sb.Append(' '); i++; return i; }
                sb.Append(c == '\n' ? '\n' : ' ');
                i++;
            }
        }
        return i;
    }
}
