using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Fails the build when code waits on a child process WITHOUT a bound -
/// <c>WaitForExit()</c> with no timeout, or <c>WaitForExitAsync()</c> with no
/// cancellation token.
/// <para>
/// WHY THIS IS A SEPARATE DEFECT FROM THE PIPE DRAIN.
/// <see cref="ChildProcessPipeDrainHygieneTests"/> gates the case where the
/// parent deadlocks <em>inside a read</em> because it drains one pipe at a time.
/// Draining both concurrently fixes that, and it is the shape this repository
/// now uses everywhere - but it fixes only the parent's half. The child can
/// still fail to exit on its own: a <c>git</c> blocked on an index.lock left by
/// a crashed process, a credential prompt on a non-interactive runner, a docker
/// daemon that is still starting, a smudge filter waiting on the network. With
/// both pipes draining, the parent sits in <c>WaitForExit</c> forever.
/// </para>
/// <para>
/// WHY IT MUST BE A GATE. The two defects fail identically and in the worst way
/// CI can fail: a <c>--blame-hang</c> abort that names no assertion and no
/// fixture, on a lane that was green yesterday. Neither reproduces locally,
/// because locally the lock file is not there and the daemon is already up.
/// A bound does not make the hang impossible - it converts it into a failure
/// that says which child overran and by how much, which is the difference
/// between an afternoon of bisection and a one-line diagnosis.
/// </para>
/// <para>
/// WHY THE UNBOUNDED FORM KEEPS COMING BACK. <c>WaitForExit()</c> is the
/// overload that autocompletes first and reads as the obvious one, and it is
/// correct in a console application where hanging is the user's problem. It is
/// only wrong in an unattended suite, which is exactly where nobody is watching
/// it. That is a shape a reviewer has to remember to look for, so it is gated
/// rather than documented.
/// </para>
/// <para>
/// THE CORRECT SHAPE, which this gate permits: pass a timeout to
/// <c>WaitForExit(milliseconds)</c> and act on the <see langword="false"/>
/// return - kill the process tree and fail with a diagnosis - or pass a
/// cancellation token to <c>WaitForExitAsync(token)</c>. Note that the bound is
/// only meaningful once both pipes are draining concurrently, because a parent
/// stuck in a read never reaches the wait at all; the two gates are load-bearing
/// together.
/// </para>
/// <para>
/// A BOUND NOBODY READS IS NO BOUND. <c>WaitForExit(milliseconds)</c> whose
/// <see langword="bool"/> is discarded - written as a bare statement, or assigned
/// to <c>_</c> - is also a finding (issue #4211). When the wait overruns, the
/// next <c>Process.ExitCode</c> read throws an
/// <see cref="InvalidOperationException"/> that names neither the timeout nor
/// the child, and the still-running child is never killed. That is the hung-child
/// failure this gate exists to prevent, in a different costume: a diagnosis of
/// the wrong condition plus a leaked process. Only the statement forms are
/// detected; a call whose result flows into a condition, a variable, an argument,
/// or a <c>return</c> is assumed to be acted on.
/// </para>
/// </summary>
[TestFixture]
public sealed class ChildProcessWaitBoundHygieneTests
{
    private const string TlcExceptionFile = "test/lattice/Formal/TlcModelCheckTests.cs";
    private static readonly string TlcWaitMarker = string.Concat("TLC_", "UNBOUNDED_WAIT");

    /// <summary>
    /// A file the gate must have examined. It waits on a child process with an
    /// explicit timeout, so it pins the scan to a real, correct site: if this
    /// file stops being examined the enumeration has broken and a clean result
    /// would mean nothing.
    /// </summary>
    private const string RegressionFile = "test/lattice.api.mcp.repocontext/Host/ScriptSuiteProcess.cs";

    /// <summary>
    /// Matches a wait on a child process whose argument list is empty. The
    /// receiver is captured only to name it in the violation; what makes the
    /// site a defect is the empty parentheses, which is the whole of the
    /// condition.
    /// </summary>
    private static readonly Regex UnboundedWaitPattern = new(
        @"(?<receiver>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)"
        + @"\.WaitForExit(?<async>Async)?\s*\(\s*\)",
        RegexOptions.Compiled);

    /// <summary>
    /// Matches a bounded synchronous wait used as a whole statement - at the start
    /// of a line or directly after <c>;</c>, <c>{</c> or <c>}</c>, optionally as
    /// a discard <c>_ = </c> - so its <see langword="bool"/> return reaches nothing.
    /// The argument list is matched with balanced parentheses so a computed
    /// timeout such as <c>(int)t.TotalMilliseconds</c> is captured whole.
    /// </summary>
    private static readonly Regex DiscardedBoundedWaitPattern = new(
        @"(?<=^|[;{}])[ \t]*(?:_[ \t]*=[ \t]*)?"
        + @"(?<receiver>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)"
        + @"\.WaitForExit\s*\((?<args>(?>[^()]+|\((?<depth>)|\)(?<-depth>))*(?(depth)(?!)))\)\s*;",
        RegexOptions.Compiled | RegexOptions.Multiline);

    [Test]
    public void No_code_waits_on_a_child_process_without_a_timeout_or_cancellation_token()
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
            if (bytes.AsSpan().IndexOf("WaitForExit"u8) < 0)
            {
                return;
            }

            var relative = Path.GetRelativePath(repoRoot, path).Replace('\\', '/');
            var text = Encoding.UTF8.GetString(bytes);
            var findings = FindUnboundedWaits(text, relative).Concat(FindDiscardedBoundedWaits(text)).ToList();

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
            nameof(No_code_waits_on_a_child_process_without_a_timeout_or_cancellation_token),
            "tracked C# files that mention WaitForExit",
            "the tracked C# files of the repository");

        Assert.That(examinedRegressionFile, Is.True,
            $"{RegressionFile} waits on a child process and is the regression target of this gate, "
            + "so it must appear in the scan. Its absence means the enumeration stopped reaching it, "
            + "not that the repository is clean.");

        Assert.That(violations, Is.Empty,
            "Bound every wait on a child process. Pass a timeout to WaitForExit(milliseconds) and act on "
            + "a false return by killing the process tree and failing with a diagnosis, or pass a "
            + "cancellation token to WaitForExitAsync(token). An unbounded wait cannot fail - it hangs, "
            + "and CI reports that as a --blame-hang abort naming no fixture and no assertion. A bounded "
            + "wait whose bool is discarded is no better: the overrun surfaces as an ExitCode "
            + "InvalidOperationException naming neither the timeout nor the child, and the child leaks:\n"
            + string.Join("\n", violations));
    }

    /// <summary>
    /// Reports every place in <paramref name="text"/> that waits on a child
    /// process with an empty argument list.
    /// </summary>
    /// <param name="text">The C# source to analyse.</param>
    /// <returns>
    /// One entry per violation, each beginning with <c>:</c> and the 1-based line
    /// number so a caller can prefix it with the file path. Ordered by position.
    /// </returns>
    internal static IReadOnlyList<string> FindUnboundedWaits(string text, string? relativePath = null)
    {
        // Comments and string literals are blanked first. This repository
        // documents its own gotchas at length, so the defective shape appears in
        // prose - including in this very file - far more often than in code, and
        // an unstripped scan reports those instead of real defects.
        var code = ChildProcessPipeDrainHygieneTests.StripCommentsAndStringLiterals(text);

        var findings = new List<string>();
        foreach (Match match in UnboundedWaitPattern.Matches(code))
        {
            var call = match.Groups["async"].Success ? "WaitForExitAsync()" : "WaitForExit()";
            var remedy = match.Groups["async"].Success
                ? "pass a cancellation token"
                : "pass a timeout in milliseconds and handle the false return";

            findings.Add(
                $":{LineOf(code, match.Index)}: {match.Groups["receiver"].Value}.{call} "
                + $"is unbounded - {remedy}.");
        }

        var markerMatches = Regex.Matches(text, Regex.Escape(TlcWaitMarker));
        if (markerMatches.Count == 1
            && string.Equals(relativePath, TlcExceptionFile, StringComparison.Ordinal)
            && findings.Count == 1
            && findings[0].Contains("process.WaitForExit()", StringComparison.Ordinal))
        {
            return [];
        }

        if (markerMatches.Count > 0)
        {
            findings.Add(
                $":{LineOf(text, markerMatches[0].Index)}: {TlcWaitMarker} must identify exactly one "
                + $"unbounded process.WaitForExit() in {TlcExceptionFile}.");
        }

        return findings;
    }

    /// <summary>
    /// Reports every place in <paramref name="text"/> that waits on a child
    /// process with a timeout but discards the <see langword="bool"/> saying
    /// whether the child exited.
    /// </summary>
    /// <param name="text">The C# source to analyse.</param>
    /// <returns>
    /// One entry per violation, each beginning with <c>:</c> and the 1-based line
    /// number so a caller can prefix it with the file path. Ordered by position.
    /// </returns>
    internal static IReadOnlyList<string> FindDiscardedBoundedWaits(string text)
    {
        var code = ChildProcessPipeDrainHygieneTests.StripCommentsAndStringLiterals(text);

        var findings = new List<string>();
        foreach (Match match in DiscardedBoundedWaitPattern.Matches(code))
        {
            // An empty argument list is the unbounded defect, reported by
            // FindUnboundedWaits; reporting it here too would double-count it.
            if (string.IsNullOrWhiteSpace(match.Groups["args"].Value))
            {
                continue;
            }

            var receiver = match.Groups["receiver"];
            findings.Add(
                $":{LineOf(code, receiver.Index)}: {receiver.Value}.WaitForExit(...) discards its result - "
                + "act on a false return by killing the process tree and failing with a diagnosis "
                + "naming the child.");
        }

        return findings;
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

    // ---------------------------------------------------------------------
    // Self-tests. A gate whose healthy outcome is an empty violation list
    // proves nothing on its own: an analyser that always returns nothing would
    // pass the scan above forever. These drive the analyser directly, over both
    // the shapes it must report and the shapes it must not.
    // ---------------------------------------------------------------------

    [Test]
    public void Analyse_reports_a_synchronous_wait_with_no_timeout()
    {
        const string source = """
            using var process = Process.Start(startInfo)!;
            process.WaitForExit();
            """;

        Assert.That(FindUnboundedWaits(source), Has.Exactly(1).Contains("process.WaitForExit()"));
    }

    [Test]
    public void Analyse_reports_an_asynchronous_wait_with_no_cancellation_token()
    {
        const string source = """
            using var process = Process.Start(startInfo)!;
            await process.WaitForExitAsync();
            """;

        var findings = FindUnboundedWaits(source);

        Assert.Multiple(() =>
        {
            Assert.That(findings, Has.Exactly(1).Contains("process.WaitForExitAsync()"));
            Assert.That(findings[0], Does.Contain("cancellation token"),
                "the remedy offered for the async overload must be a cancellation token, not a timeout: "
                + "WaitForExitAsync has no timeout overload, so the wrong advice sends the reader looking "
                + "for an API that does not exist.");
        });
    }

    [Test]
    public void Analyse_reports_a_wait_whose_empty_argument_list_spans_whitespace()
    {
        const string source = """
            process.WaitForExit(
            );
            """;

        Assert.That(FindUnboundedWaits(source), Is.Not.Empty,
            "the defect is an empty argument list, not a particular spelling of one. Reformatting the "
            + "call across two lines must not hide it.");
    }

    [Test]
    public void Analyse_reports_a_wait_on_a_dotted_receiver()
    {
        const string source = "_harness.Process.WaitForExit();";

        Assert.That(FindUnboundedWaits(source), Has.Exactly(1).Contains("_harness.Process.WaitForExit()"));
    }

    [Test]
    public void Analyse_accepts_a_wait_bounded_by_a_literal_timeout()
    {
        const string source = "process.WaitForExit(30000);";

        Assert.That(FindUnboundedWaits(source), Is.Empty);
    }

    [Test]
    public void The_TLC_marker_exempts_exactly_its_one_unbounded_wait_in_the_TLC_fixture()
    {
        var marker = string.Concat("TLC_", "UNBOUNDED_WAIT");
        var source = $"// {marker}\nprocess.WaitForExit();";

        Assert.Multiple(() =>
        {
            Assert.That(
                FindUnboundedWaits(source, TlcExceptionFile),
                Is.Empty,
                "the marked TLC wait relies on the outer CI job timeout, not a short per-run deadline");
            Assert.That(
                FindUnboundedWaits(source, "test/other.cs"),
                Has.Count.EqualTo(2),
                "a marker in any other file must not suppress an unbounded wait");
        });
    }

    [Test]
    public void The_TLC_marker_does_not_exempt_multiple_waits_or_a_wait_in_another_fixture()
    {
        var marker = string.Concat("TLC_", "UNBOUNDED_WAIT");
        var source = $"// {marker}\nfirst.WaitForExit();\nsecond.WaitForExit();";
        var findings = FindUnboundedWaits(source, TlcExceptionFile);

        Assert.That(findings, Has.Count.EqualTo(3),
            "the exception is valid only while the marker identifies the fixture's single expected wait");
    }

    [Test]
    public void Analyse_accepts_a_wait_bounded_by_a_computed_timeout()
    {
        const string source = "process.WaitForExit((int)Timeout.TotalMilliseconds);";

        Assert.That(FindUnboundedWaits(source), Is.Empty);
    }

    [Test]
    public void Analyse_accepts_an_asynchronous_wait_bounded_by_a_token()
    {
        const string source = "await process.WaitForExitAsync(cts.Token);";

        Assert.That(FindUnboundedWaits(source), Is.Empty);
    }

    [Test]
    public void Analyse_ignores_the_defective_shape_inside_a_comment_or_literal()
    {
        const string source = """
            // Never write process.WaitForExit(); here.
            /* process.WaitForExit(); */
            var advice = "process.WaitForExit();";
            process.WaitForExit(5000);
            """;

        Assert.That(FindUnboundedWaits(source), Is.Empty,
            "prose and literals are not code. This repository documents this exact defect in XML doc "
            + "comments, so a scan that did not strip them would report its own documentation and bury "
            + "any real finding.");
    }

    [Test]
    public void Analyse_reports_every_site_in_a_file_in_positional_order()
    {
        const string source = """
            first.WaitForExit();
            second.WaitForExit(1000);
            third.WaitForExit();
            """;

        var findings = FindUnboundedWaits(source);

        Assert.Multiple(() =>
        {
            Assert.That(findings, Has.Count.EqualTo(2),
                "the bounded middle call must not be reported, and neither unbounded call may be dropped: "
                + "a scan that stops at the first finding hides the rest of the file.");
            Assert.That(findings[0], Does.Contain(":1:").And.Contains("first"));
            Assert.That(findings[1], Does.Contain(":3:").And.Contains("third"));
        });
    }

    [Test]
    public void Analyse_reports_nothing_for_source_that_drives_no_child_process()
    {
        const string source = "var total = values.Sum();";

        Assert.That(FindUnboundedWaits(source), Is.Empty);
    }

    [Test]
    public void AnalyseDiscarded_reports_a_bounded_wait_used_as_a_bare_statement()
    {
        // The exact shape of issue #4211: the bound is passed, its bool is dropped,
        // and the next ExitCode read throws on a child that is still running.
        const string source = """
            var stdoutTask = process.StandardOutput.ReadToEndAsync();
            process.WaitForExit(timeoutMilliseconds);
            return process.ExitCode;
            """;

        Assert.That(FindDiscardedBoundedWaits(source),
            Has.Exactly(1).Contains(":2:").And.Contains("process.WaitForExit(...) discards its result"));
    }

    [Test]
    public void AnalyseDiscarded_reports_a_named_argument_a_computed_timeout_and_an_explicit_discard()
    {
        const string source = """
            first.WaitForExit(milliseconds: 120_000);
            second.WaitForExit((int)Timeout.TotalMilliseconds);
            _ = third.WaitForExit(5000);
            { fourth.WaitForExit(
                5000); }
            """;

        var findings = FindDiscardedBoundedWaits(source);

        Assert.Multiple(() =>
        {
            Assert.That(findings, Has.Count.EqualTo(4),
                "a named argument, a nested-parenthesis timeout, an explicit discard, and a call spread "
                + "across lines are all the same defect.");
            Assert.That(findings[0], Does.Contain(":1:").And.Contains("first"));
            Assert.That(findings[1], Does.Contain(":2:").And.Contains("second"));
            Assert.That(findings[2], Does.Contain(":3:").And.Contains("third"));
            Assert.That(findings[3], Does.Contain(":4:").And.Contains("fourth"));
        });
    }

    [Test]
    public void AnalyseDiscarded_accepts_a_bounded_wait_whose_result_is_consumed()
    {
        const string source = """
            if (!process.WaitForExit(5000)) { Kill(process); }
            var exited = process.WaitForExit(5000);
            Assert.That(process.WaitForExit(5000), Is.True);
            return process.WaitForExit(5000);
            await process.WaitForExitAsync(cts.Token);
            """;

        Assert.That(FindDiscardedBoundedWaits(source), Is.Empty,
            "a result that flows into a condition, a variable, an argument or a return is acted on, and "
            + "WaitForExitAsync returns no bool at all.");
    }

    [Test]
    public void AnalyseDiscarded_leaves_an_unbounded_wait_to_the_unbounded_analyser()
    {
        const string source = "process.WaitForExit();";

        Assert.Multiple(() =>
        {
            Assert.That(FindDiscardedBoundedWaits(source), Is.Empty,
                "an empty argument list is the unbounded defect; reporting it twice would double-count it.");
            Assert.That(FindUnboundedWaits(source), Has.Exactly(1).Items);
        });
    }

    [Test]
    public void AnalyseDiscarded_ignores_the_defective_shape_inside_a_comment_or_literal()
    {
        const string source = """
            // process.WaitForExit(5000);
            var advice = "process.WaitForExit(5000);";
            """;

        Assert.That(FindDiscardedBoundedWaits(source), Is.Empty);
    }
}
