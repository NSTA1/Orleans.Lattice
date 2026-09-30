using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Fails the build when a test drives a child process and reads its redirected
/// output pipes SEQUENTIALLY - one stream read to completion before the read of
/// the other stream has started.
/// <para>
/// WHY THIS IS A HANG AND NOT A SLOW TEST. A child process writing to a
/// redirected pipe blocks once that pipe's buffer fills and nobody is draining
/// it. A parent that reads stdout to EOF before touching stderr therefore waits
/// for a child that is itself blocked writing stderr, and neither side can move.
/// The deadlock happens INSIDE the first read, which is before
/// <c>WaitForExit(timeout)</c> is ever reached, so the timeout that looks like
/// it bounds the call bounds nothing at all.
/// </para>
/// <para>
/// WHY IT MUST BE A GATE. The failure mode is the least diagnosable one CI can
/// produce: a <c>--blame-hang</c> abort with no failing assertion and no fixture
/// name, on a lane that was green yesterday. It is also invisible to every local
/// run in which the child happens to stay under the buffer high-water mark, so
/// it ships looking correct and only bites once some future change makes the
/// child chattier.
/// </para>
/// <para>
/// THE AWAITED FORM IS EQUALLY DEFECTIVE. <c>await ReadToEndAsync()</c> on one
/// stream followed by <c>await ReadToEndAsync()</c> on the other is the same
/// bug: the await drains the first pipe to EOF before the second read starts.
/// It merely looks asynchronous.
/// </para>
/// <para>
/// THE CORRECT SHAPE, which this gate deliberately permits: start BOTH
/// <c>ReadToEndAsync()</c> tasks, then <c>WaitForExit</c>, then harvest both.
/// Both pipes are draining for the whole life of the child, so neither can
/// back up. Reading one stream through a started-but-not-yet-awaited task while
/// the other is read synchronously is equally safe and is also permitted.
/// </para>
/// </summary>
[TestFixture]
public sealed class ChildProcessPipeDrainHygieneTests
{
    /// <summary>
    /// Maximum line distance at which two stream reads are treated as one
    /// drain sequence. Reads further apart than this are separate operations
    /// (typically different methods), and pairing them would invent a defect
    /// that is not there.
    /// </summary>
    private const int MaxLineGap = 12;

    /// <summary>
    /// A file the gate must have examined. It runs a child process and drains
    /// both pipes concurrently, so it pins the scan to a real, correct site:
    /// if this file stops being examined the enumeration has broken.
    /// </summary>
    private const string RegressionFile = "test/lattice.api.state.grpc/Security/CredentialScriptParityTests.cs";

    private static readonly Regex ReadSitePattern = new(
        @"(?<await>await\s+)?(?<receiver>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)"
        + @"\.Standard(?<stream>Output|Error)\s*\.\s*ReadToEnd(?<async>Async)?\s*\(",
        RegexOptions.Compiled);

    [Test]
    public void No_test_reads_one_child_process_pipe_to_completion_before_draining_the_other()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var violations = new List<string>();
        var examined = 0;
        var examinedRegressionFile = false;
        var gate = new object();

        Parallel.ForEach(HygieneRepository.EnumerateFiles(repoRoot, "*.cs"), path =>
        {
            // Pre-filter on the raw bytes: the marker is ASCII, the search is
            // vectorised, and it spares a UTF-8 decode of the ~99% of files that
            // never drive a child process at all.
            var bytes = File.ReadAllBytes(path);
            if (bytes.AsSpan().IndexOf("ReadToEnd"u8) < 0)
            {
                return;
            }

            var relative = Path.GetRelativePath(repoRoot, path).Replace('\\', '/');
            var findings = FindSequentialDrains(Encoding.UTF8.GetString(bytes));

            lock (gate)
            {
                examined++;
                examinedRegressionFile |= string.Equals(relative, RegressionFile, StringComparison.Ordinal);
                foreach (var finding in findings)
                {
                    violations.Add($"{relative}{finding}");
                }
            }
        });

        violations.Sort(StringComparer.Ordinal);

        HygieneDenominator.RequireExamined(
            examined,
            nameof(No_test_reads_one_child_process_pipe_to_completion_before_draining_the_other),
            "tracked C# files that mention ReadToEnd",
            "the tracked C# files of the repository");

        Assert.That(examinedRegressionFile, Is.True,
            $"{RegressionFile} drives a child process and is the regression target of this gate, "
            + "so it must appear in the scan. Its absence means the enumeration stopped reaching it, "
            + "not that the repository is clean.");

        Assert.That(violations, Is.Empty,
            "Drain both child-process pipes concurrently: start BOTH ReadToEndAsync() tasks, then "
            + "WaitForExit, then harvest both. Reading one stream to completion first deadlocks once "
            + "the child fills the undrained pipe, and it deadlocks INSIDE the read, so WaitForExit's "
            + "timeout never runs and bounds nothing:\n"
            + string.Join("\n", violations));
    }

    /// <summary>
    /// Reports every place in <paramref name="text"/> where one standard stream
    /// is read to completion before a read of the other stream begins.
    /// </summary>
    internal static IReadOnlyList<string> FindSequentialDrains(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        var code = StripCommentsAndStringLiterals(text);
        var sites = new List<(int Line, string Stream, bool Completing)>();

        foreach (Match match in ReadSitePattern.Matches(code))
        {
            var isAsync = match.Groups["async"].Success;
            var completing = !isAsync
                || match.Groups["await"].Success
                || HarvestsImmediately(code, match.Index + match.Length - 1);

            sites.Add((LineOf(code, match.Index), match.Groups["stream"].Value, completing));
        }

        var violations = new List<string>();
        for (var i = 0; i + 1 < sites.Count; i++)
        {
            var first = sites[i];
            var second = sites[i + 1];

            if (!first.Completing
                || string.Equals(first.Stream, second.Stream, StringComparison.Ordinal)
                || second.Line - first.Line > MaxLineGap)
            {
                continue;
            }

            violations.Add(
                $"({first.Line}): Standard{first.Stream} is read to completion before the "
                + $"Standard{second.Stream} read on line {second.Line} has started");
        }

        return violations;
    }

    /// <summary>
    /// Determines whether the call whose opening parenthesis sits at
    /// <paramref name="openParenIndex"/> is harvested synchronously at the call
    /// site, via <c>.Result</c> or <c>.GetAwaiter().GetResult()</c>. Such a call
    /// completes the read then and there, exactly as an <c>await</c> would.
    /// </summary>
    private static bool HarvestsImmediately(string code, int openParenIndex)
    {
        var depth = 0;
        for (var i = openParenIndex; i < code.Length; i++)
        {
            if (code[i] == '(')
            {
                depth++;
            }
            else if (code[i] == ')')
            {
                depth--;
                if (depth != 0)
                {
                    continue;
                }

                var tailLength = Math.Min(24, code.Length - i - 1);
                var tail = code.Substring(i + 1, tailLength);
                return tail.StartsWith(".Result", StringComparison.Ordinal)
                    || tail.StartsWith(".GetAwaiter()", StringComparison.Ordinal);
            }
        }

        return false;
    }

    private static int LineOf(string text, int index)
    {
        var line = 1;
        for (var i = 0; i < index; i++)
        {
            if (text[i] == '\n')
            {
                line++;
            }
        }

        return line;
    }

    /// <summary>
    /// Blanks every comment and string literal in <paramref name="text"/>,
    /// preserving length and line breaks so reported line numbers stay true.
    /// <para>
    /// Without this the gate would flag its own analyser fixtures below, and
    /// any documentation sample that quotes the defective shape in order to
    /// warn about it. A detector that cannot tell code from prose about code
    /// reports the warning as the offence.
    /// </para>
    /// </summary>
    internal static string StripCommentsAndStringLiterals(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        var output = new StringBuilder(text.Length);
        var i = 0;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                while (i < text.Length && text[i] != '\n')
                {
                    output.Append(' ');
                    i++;
                }

                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                while (i < text.Length && !(text[i] == '*' && i + 1 < text.Length && text[i + 1] == '/'))
                {
                    output.Append(text[i] == '\n' ? '\n' : ' ');
                    i++;
                }

                for (var k = 0; k < 2 && i < text.Length; k++, i++)
                {
                    output.Append(' ');
                }

                continue;
            }

            if (c == '\'')
            {
                i = BlankSimpleLiteral(text, i, '\'', output);
                continue;
            }

            if (c == '"')
            {
                var fence = 0;
                while (i + fence < text.Length && text[i + fence] == '"')
                {
                    fence++;
                }

                i = fence >= 3
                    ? BlankRawLiteral(text, i, fence, output)
                    : BlankSimpleLiteral(text, i, '"', output);
                continue;
            }

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                i = BlankVerbatimLiteral(text, i, output);
                continue;
            }

            output.Append(c);
            i++;
        }

        return output.ToString();
    }

    private static int BlankSimpleLiteral(string text, int start, char quote, StringBuilder output)
    {
        output.Append(' ');
        var i = start + 1;

        while (i < text.Length && text[i] != quote && text[i] != '\n')
        {
            if (text[i] == '\\' && i + 1 < text.Length)
            {
                output.Append("  ");
                i += 2;
                continue;
            }

            output.Append(' ');
            i++;
        }

        if (i < text.Length && text[i] == quote)
        {
            output.Append(' ');
            i++;
        }

        return i;
    }

    private static int BlankVerbatimLiteral(string text, int start, StringBuilder output)
    {
        output.Append("  ");
        var i = start + 2;

        while (i < text.Length)
        {
            if (text[i] == '"')
            {
                if (i + 1 < text.Length && text[i + 1] == '"')
                {
                    output.Append("  ");
                    i += 2;
                    continue;
                }

                output.Append(' ');
                return i + 1;
            }

            output.Append(text[i] == '\n' ? '\n' : ' ');
            i++;
        }

        return i;
    }

    private static int BlankRawLiteral(string text, int start, int fence, StringBuilder output)
    {
        for (var k = 0; k < fence; k++)
        {
            output.Append(' ');
        }

        var i = start + fence;

        while (i < text.Length)
        {
            if (text[i] == '"')
            {
                var run = 0;
                while (i + run < text.Length && text[i + run] == '"')
                {
                    run++;
                }

                if (run >= fence)
                {
                    for (var k = 0; k < run; k++)
                    {
                        output.Append(' ');
                    }

                    return i + run;
                }

                for (var k = 0; k < run; k++)
                {
                    output.Append(' ');
                }

                i += run;
                continue;
            }

            output.Append(text[i] == '\n' ? '\n' : ' ');
            i++;
        }

        return i;
    }

    [Test]
    public void Analyse_reports_a_synchronous_read_of_one_pipe_before_the_other()
    {
        const string source = """
            var process = Process.Start(startInfo)!;
            var standardOutput = process.StandardOutput.ReadToEnd();
            var standardError = process.StandardError.ReadToEnd();
            process.WaitForExit(5000);
            """;

        Assert.That(FindSequentialDrains(source), Has.Exactly(1).Contains("StandardOutput"));
    }

    [Test]
    public void Analyse_reports_a_single_expression_that_concatenates_both_reads()
    {
        const string source = """
            var combined = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
            """;

        Assert.That(FindSequentialDrains(source), Is.Not.Empty,
            "concatenating both reads in one expression still evaluates them left to right, so the "
            + "first pipe is drained to EOF before the second read starts");
    }

    [Test]
    public void Analyse_reports_sequentially_awaited_reads()
    {
        const string source = """
            var standardOutput = await process.StandardOutput.ReadToEndAsync();
            var standardError = await process.StandardError.ReadToEndAsync();
            """;

        Assert.That(FindSequentialDrains(source), Is.Not.Empty,
            "awaiting the first read to completion before starting the second is the same deadlock; "
            + "it merely looks asynchronous");
    }

    [Test]
    public void Analyse_reports_reads_harvested_synchronously_at_the_call_site()
    {
        const string source = """
            var standardOutput = process.StandardOutput.ReadToEndAsync().GetAwaiter().GetResult();
            var standardError = process.StandardError.ReadToEndAsync().GetAwaiter().GetResult();
            """;

        Assert.That(FindSequentialDrains(source), Is.Not.Empty,
            "harvesting the task at the call site completes the read there, exactly as await would");
    }

    [Test]
    public void Analyse_accepts_two_reads_started_before_either_is_harvested()
    {
        const string source = """
            var standardOutputTask = process.StandardOutput.ReadToEndAsync();
            var standardErrorTask = process.StandardError.ReadToEndAsync();
            process.WaitForExit(5000);
            var standardOutput = standardOutputTask.GetAwaiter().GetResult();
            var standardError = standardErrorTask.GetAwaiter().GetResult();
            """;

        Assert.That(FindSequentialDrains(source), Is.Empty,
            "both pipes drain for the whole life of the child, so neither can back up");
    }

    [Test]
    public void Analyse_accepts_one_deferred_task_alongside_one_synchronous_read()
    {
        const string source = """
            var standardErrorTask = process.StandardError.ReadToEndAsync();
            var standardOutput = process.StandardOutput.ReadToEnd();
            var standardError = standardErrorTask.GetAwaiter().GetResult();
            """;

        Assert.That(FindSequentialDrains(source), Is.Empty,
            "the deferred task keeps stderr draining while stdout is read, so neither pipe backs up");
    }

    [Test]
    public void Analyse_ignores_the_defective_shape_when_it_appears_inside_a_comment_or_literal()
    {
        const string source = """
            // var standardOutput = process.StandardOutput.ReadToEnd();
            // var standardError = process.StandardError.ReadToEnd();
            var documentation = "process.StandardOutput.ReadToEnd(); process.StandardError.ReadToEnd();";
            """;

        Assert.That(FindSequentialDrains(source), Is.Empty,
            "prose that warns about the defective shape is not itself the defect");
    }

    [Test]
    public void Analyse_does_not_pair_reads_that_belong_to_different_operations()
    {
        var source = "var a = process.StandardOutput.ReadToEnd();"
            + string.Concat(Enumerable.Repeat("\n", MaxLineGap + 2))
            + "var b = other.StandardError.ReadToEnd();";

        Assert.That(FindSequentialDrains(source), Is.Empty,
            "reads separated by more than the drain-sequence window are separate operations");
    }
}
