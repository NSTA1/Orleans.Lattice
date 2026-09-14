using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The integration-branch trigger gate: a workflow that gates pull requests
/// INTO an epic or bucket branch must also run ON that branch, and no step
/// reachable on that push lane may read pull-request context without saying so.
/// <para>
/// <b>The gap this closes.</b> <c>ci.yml</c> triggers on <c>pull_request</c> for
/// <c>[main, '*/epic/**', 'release/**']</c>, which gates a pull request into a
/// bucket. Nothing gated the bucket ITSELF: with no <c>push</c> trigger for
/// <c>'*/epic/**'</c>, an integration branch's combined state is built and
/// tested only through the epic's own pull request into <c>main</c>.
/// </para>
/// <para>
/// <b>That claim is weaker than it first appears, and the weaker version is the
/// real one.</b> If the epic's pull request is already open - raised early as a
/// draft, which is common - its head IS the bucket, so every merge fires a
/// <c>synchronize</c> event and CI does run against the combined state. While
/// such a pull request is open, this trigger is redundant with it. What is
/// missing is not the coverage but the GUARANTEE: nothing requires that pull
/// request to exist yet, and the documented convention is that the epic reaches
/// <c>main</c> as a single fully-gated pull request once its integration item
/// passes, which is to say at the END. A bucket run that way has no coverage for
/// its entire working life, and the two cases are indistinguishable from the
/// workflow configuration, which is the only artefact a reader can inspect.
/// Apply the diagnostic question this repository uses for health fields - name
/// the thing that can make "this bucket is sound" report false - and without
/// this trigger the answer is "a draft pull request somebody happened to open",
/// which is a fact about one bucket's process history rather than a property of
/// CI.
/// </para>
/// <para>
/// <b>Why that matters and is not merely a thin spot.</b> A gate that
/// quantifies over a structurally-derived set is only as current as the last run
/// that evaluated it. Two changes are therefore individually safe and jointly
/// unsafe when one ADDS A MEMBER to the set the other QUANTIFIES OVER, and
/// textual mergeability cannot see it, because the two changes need share no
/// file: the coupling runs entirely through the gate's own predicate. The worked
/// near-miss is #2972. One pull request shipped a three-site inventory gate
/// asserting that every WAL replay site constructs its reader with the
/// configured slice budget; a sibling then added a new replay entry point. Had
/// that entry point constructed its own reader it would have been a FOURTH
/// replay site bypassing the budget while the three-site gate kept passing - the
/// exact defect the gate was written to prevent, reintroduced by a green
/// sibling. It was safe, and on that particular bucket the open draft epic pull
/// request would in fact have caught it on its next synchronize; the point is
/// that nobody could have known that from the workflow, and the manual source
/// trace that actually settled it was run only because the adjacency happened to
/// be noticed.
/// </para>
/// <para>
/// <b>This is advisory and must never become branch protection.</b> A bucket
/// exists so members merge in any order without invalidating each other. A
/// <c>strict</c> required check on an integration branch would cost N(N+1)/2 CI
/// cycles instead of N+1 and destroy the entire benefit. A push-triggered run
/// blocks nothing: it REPORTS, so a collision is attributed to the merge that
/// introduced it instead of surfacing late at the epic's pull request mixed with
/// every other member's changes. This fixture asserts the trigger exists; it
/// says nothing about protection, and nothing here should be read as asking for
/// it.
/// </para>
/// <para>
/// <b>Why the population is derived and not listed.</b> An inventory is a
/// snapshot of what somebody remembered; it is not a detector. Both tests below
/// quantify over a set read out of the workflow files themselves - the workflows
/// that DECLARE epic coverage, and the steps that ACTUALLY read pull-request
/// context - so the list is a report of the scan rather than a substitute for
/// it. A workflow added later that gates epic pull requests is in the population
/// the moment it exists, with no edit here.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiIntegrationBranchTriggerTests
{
    /// <summary>
    /// The branch pattern that denotes an epic or bucket integration branch.
    /// Mirrors <c>.github/copilot-instructions.md</c>, which is the single
    /// source of truth for the shape.
    /// </summary>
    private const string IntegrationBranchPattern = "*/epic/**";

    /// <summary>
    /// Expressions that are only populated on a <c>pull_request</c> event. On a
    /// push every one of them evaluates to the EMPTY STRING rather than
    /// failing, which is what makes an unguarded read dangerous: it does not
    /// error, it silently compares or diffs against nothing.
    /// </summary>
    private static readonly string[] PullRequestOnlyContext =
    [
        "github.event.pull_request",
        "github.head_ref",
    ];

    private const string EventGuard = "github.event_name == 'pull_request'";

    /// <summary>
    /// Non-vacuity floor for the pull-request-context scan. <c>ci.yml</c> reads
    /// that context in the branch-name guard, the inert-closing-keyword guard,
    /// and four change-detection steps, plus the two change-detection steps in
    /// the <c>extras</c> job, so the true count is comfortably above this. The
    /// floor exists so that a scan which silently stopped matching - a changed
    /// step indentation, a renamed key - reddens instead of reporting a clean
    /// zero. A gate that finds nothing to check must never pass.
    /// </summary>
    private const int MinimumPullRequestContextReaders = 6;

    /// <summary>
    /// Anti-vacuity floor for the workflow enumeration. The repository has seven
    /// workflow files at the time of writing; the floor sits below that so
    /// deleting one does not red the gate, but far enough above zero that a
    /// broken directory walk cannot pass as a clean scan.
    /// </summary>
    private const int MinimumWorkflowCount = 5;

    /// <summary>
    /// The plan output a push-lane job reads to tell a member push from an
    /// integration-branch push. Named once so the gate and the workflow cannot
    /// drift apart silently.
    /// </summary>
    private const string MemberPushFlag = "needs.plan.outputs.push_is_member";

    [Test]
    public void Every_workflow_gating_integration_branch_pull_requests_also_runs_on_push()
    {
        var declaring = new List<string>();
        var missing = new List<string>();

        foreach (var workflow in Workflows())
        {
            var on = OnBlock(File.ReadAllText(workflow.Path));
            if (!BranchesOf(on, "pull_request").Contains(IntegrationBranchPattern))
            {
                continue;
            }

            declaring.Add(workflow.Name);

            if (!BranchesOf(on, "push").Contains(IntegrationBranchPattern))
            {
                missing.Add(workflow.Name);
            }
        }

        // Anti-vacuity, both directions. A population that came back empty, or
        // one that lost the workflow this gate was written for, would let the
        // assertion below pass while checking nothing.
        Assert.That(
            declaring,
            Is.Not.Empty,
            "no workflow declares '" + IntegrationBranchPattern
                + "' in its pull_request branch list; the scan is not matching and this gate is vacuous");

        Assert.That(
            declaring,
            Does.Contain("ci.yml"),
            "ci.yml is the workflow this gate exists for and it is not in the derived population; the scan is broken");

        Assert.That(
            missing,
            Is.Empty,
            "these workflows gate pull requests INTO an integration branch but never run ON one, so the branch's "
                + "combined state is never evaluated: " + string.Join(", ", missing)
                + ". Add a push trigger for '" + IntegrationBranchPattern
                + "'. It is advisory and must not be added to a branch protection rule.");
    }

    [Test]
    public void No_push_reachable_step_reads_pull_request_context_without_an_event_guard()
    {
        var guarded = new List<string>();
        var unguarded = new List<string>();

        foreach (var workflow in Workflows())
        {
            var text = File.ReadAllText(workflow.Path);
            if (!BranchesOf(OnBlock(text), "push").Contains(IntegrationBranchPattern))
            {
                continue;
            }

            foreach (var (jobId, jobBlock) in Jobs(text))
            {
                var jobGuarded = Condition(jobBlock, indent: 4).Contains(EventGuard, StringComparison.Ordinal);

                foreach (var (stepName, stepBlock) in Steps(jobBlock))
                {
                    if (!ReadsPullRequestContext(stepBlock))
                    {
                        continue;
                    }

                    var site = workflow.Name + " :: " + jobId + " :: " + stepName;

                    if (jobGuarded || Condition(stepBlock, indent: 8).Contains(EventGuard, StringComparison.Ordinal))
                    {
                        guarded.Add(site);
                    }
                    else
                    {
                        unguarded.Add(site);
                    }
                }
            }
        }

        // Anti-vacuity. The failure mode this floor catches is the one that
        // matters most here: a scan that no longer recognises a step at all
        // reports zero unguarded readers, which is indistinguishable from a
        // clean result. Requiring that the scan SEE readers before believing it
        // found no bad ones is the difference between a detector and decoration.
        Assert.That(
            guarded.Count + unguarded.Count,
            Is.GreaterThanOrEqualTo(MinimumPullRequestContextReaders),
            "the pull-request-context scan found only " + (guarded.Count + unguarded.Count)
                + " reader(s) across the push-triggered workflows, below the floor of "
                + MinimumPullRequestContextReaders
                + "; the scan is not matching and a clean result here would mean nothing");

        Assert.That(
            guarded,
            Is.Not.Empty,
            "no guarded reader was found, so the scan has never demonstrated that it can tell a guarded step "
                + "from an unguarded one");

        Assert.That(
            unguarded,
            Is.Empty,
            "these steps are reachable on the integration-branch push lane and read pull-request context, which is "
                + "EMPTY on a push - so they do not fail, they silently compare or diff against nothing: "
                + string.Join("; ", unguarded)
                + ". Condition the step (or its job) on \"" + EventGuard + "\".");
    }

    /// <summary>
    /// No workflow mapping may declare the same key twice. GitHub Actions
    /// rejects a duplicate key by failing the whole workflow at STARTUP, with no
    /// jobs and only the generic message "This run likely failed because of a
    /// workflow file issue".
    /// <para>
    /// <b>Why a gate rather than local validation.</b> This class of defect was
    /// introduced, and shipped, while a local PyYAML parse of the same file
    /// reported it valid: PyYAML resolves a duplicate key by keeping the LAST
    /// occurrence and raising nothing, so parsing is not a detector for it. The
    /// concrete case was a step that already carried an <c>if:</c> and acquired a
    /// second one when an event guard was added above it instead of merged into
    /// it. The first condition was silently discarded, so the file parsed, read
    /// correctly to a human, and was invalid to the only parser that matters.
    /// </para>
    /// <para>
    /// The failure direction is what makes this worth gating: a discarded key
    /// changes behaviour without changing how the file reads, and a startup
    /// failure reports no job and no step, so the run cannot say which line is at
    /// fault.
    /// </para>
    /// </summary>
    [Test]
    public void No_workflow_mapping_declares_the_same_key_twice()
    {
        // Positive control FIRST: the scan is asserted to observe a duplicate it
        // is handed, before its silence on the real files is read as a clean
        // result. Without this, a scan that matched nothing would pass.
        var control = string.Join(
            "\n",
            "steps:",
            "  - name: probe",
            "    if: a",
            "    if: b");

        Assert.That(
            DuplicateKeys(control).Select(duplicate => duplicate.Key),
            Does.Contain("if"),
            "the duplicate-key scan did not find the duplicate in its own control sample, so its silence on the "
                + "real workflow files would mean nothing");

        // ... and the control must not fire on a well-formed sibling, or the
        // scan would be reporting every file as broken and still "pass" above.
        var negativeControl = string.Join(
            "\n",
            "steps:",
            "  - name: probe",
            "    if: a",
            "  - name: probe",
            "    if: b");

        Assert.That(
            DuplicateKeys(negativeControl),
            Is.Empty,
            "the duplicate-key scan reported a duplicate across two SEPARATE list items, so it is not tracking "
                + "mapping scope and would red-flag well-formed workflows");

        var offenders = new List<string>();
        var scanned = 0;

        foreach (var workflow in Workflows())
        {
            var text = File.ReadAllText(workflow.Path);
            scanned++;

            foreach (var duplicate in DuplicateKeys(text))
            {
                offenders.Add(workflow.Name + " line " + duplicate.Line + ": duplicate key '" + duplicate.Key + "'");
            }
        }

        Assert.That(
            scanned,
            Is.GreaterThanOrEqualTo(MinimumWorkflowCount),
            "the scan read only " + scanned + " workflow file(s), below the floor of " + MinimumWorkflowCount
                + "; it is not enumerating the workflow directory and a clean result would mean nothing");

        Assert.That(
            offenders,
            Is.Empty,
            "these workflow mappings declare the same key twice. GitHub Actions fails the run at startup with no "
                + "jobs and a generic message, while a YAML parser silently keeps the last occurrence: "
                + string.Join("; ", offenders));
    }

    /// <summary>
    /// A job that runs on an integration-branch push and reports regardless of
    /// whether its dependencies ran must also exclude a member push.
    /// <para>
    /// The push lane cannot be kept off member branches by glob. The branch
    /// convention makes a member <c>&lt;bucket&gt;-&lt;item&gt;</c>, so a bucket and its
    /// members occupy the same path segment and <c>*/epic/**</c> matches both;
    /// the lane therefore starts on a member push, classifies the ref, and skips
    /// the test legs. A job conditioned on <c>always()</c> survives that skip by
    /// construction - that is what <c>always()</c> is for - so it runs with every
    /// leg skipped and publishes a green check on the member's own commit sha,
    /// asserting an outcome it did not measure. Check runs are keyed by sha, not
    /// by branch, so that green lands on the member's pull request beside the
    /// real one and is not distinguishable from it.
    /// </para>
    /// <para>
    /// The population is derived from the workflow text - every job whose
    /// condition contains <c>always()</c> in a workflow carrying the
    /// integration-branch push trigger - rather than from a list of job names.
    /// A list would cover exactly the jobs somebody remembered to add to it,
    /// which is the hand-maintained inventory the gate exists to replace. The
    /// derivation also means a second <c>always()</c> job added later is
    /// enrolled by existing, not by an edit here.
    /// </para>
    /// <para>
    /// Scope, stated against interest: this arm grades the reporting shape it
    /// can decide statically. A job that is NOT conditioned on <c>always()</c>
    /// but still concludes green on a member push - because it has no
    /// dependencies to skip, as <c>content-gates</c> does - is outside the
    /// population and is not graded. That is deliberate rather than overlooked:
    /// such a job reports on work it genuinely performed, so its green is
    /// accurate, and widening the predicate to redden it would push authors
    /// toward removing honest jobs from the push lane.
    /// </para>
    /// </summary>
    [Test]
    public void No_always_job_on_an_integration_push_reports_without_excluding_member_pushes()
    {
        var scanned = new List<string>();
        var offenders = new List<string>();

        foreach (var workflow in Workflows())
        {
            var text = File.ReadAllText(workflow.Path);

            if (BranchesOf(OnBlock(text), "push").All(b => b != IntegrationBranchPattern))
            {
                continue;
            }

            foreach (var (id, block) in Jobs(text))
            {
                var condition = Condition(block, 4);

                if (!condition.Contains("always()", StringComparison.Ordinal))
                {
                    continue;
                }

                scanned.Add(workflow.Name + ":" + id);

                if (!condition.Contains(MemberPushFlag, StringComparison.Ordinal))
                {
                    offenders.Add(workflow.Name + ":" + id + " -> if: " + condition.Trim());
                }
            }
        }

        Assert.That(
            scanned,
            Is.Not.Empty,
            "no job conditioned on always() was found in any workflow carrying the '" + IntegrationBranchPattern
                + "' push trigger; the job or condition scan has stopped matching and a clean result would mean "
                + "nothing");

        Assert.That(
            offenders,
            Is.Empty,
            "these jobs run on an integration-branch push and report regardless of upstream skips, without "
                + "excluding a member push. On a member push every test leg is skipped, so the job publishes a "
                + "green check on the member's commit sha that asserts an outcome nothing measured. The condition "
                + "must also require that " + MemberPushFlag + " is not true: " + string.Join("; ", offenders));
    }

    private sealed record DuplicateKey(string Key, int Line);
    /// <summary>
    /// Duplicate keys within a single YAML mapping, tracked by indentation.
    /// Block scalars (<c>|</c>, <c>&gt;-</c>, ...) are skipped entirely, because
    /// their content is literal text - a <c>run:</c> script full of shell is not
    /// a mapping and must never be scanned as one.
    /// </summary>
    private static IReadOnlyList<DuplicateKey> DuplicateKeys(string yaml)
    {
        var duplicates = new List<DuplicateKey>();
        var scopes = new SortedDictionary<int, HashSet<string>>();
        var lines = yaml.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var blockScalarIndent = -1;

        for (var index = 0; index < lines.Length; index++)
        {
            var line = lines[index];

            if (line.Trim().Length == 0)
            {
                continue;
            }

            var indent = line.Length - line.TrimStart(' ').Length;

            if (blockScalarIndent >= 0)
            {
                if (indent > blockScalarIndent)
                {
                    continue;
                }

                blockScalarIndent = -1;
            }

            var trimmed = line.TrimStart(' ');

            if (trimmed.StartsWith('#'))
            {
                continue;
            }

            // A list item opens a fresh mapping scope at the column of its first
            // key, so `- if: a` and a sibling `- if: b` are not duplicates.
            var itemMarker = Regex.Match(trimmed, @"^-[ \t]+");

            if (itemMarker.Success)
            {
                indent += itemMarker.Length;
                trimmed = trimmed[itemMarker.Length..];

                foreach (var scope in scopes.Keys.Where(key => key >= indent).ToList())
                {
                    scopes.Remove(scope);
                }
            }
            else if (trimmed.StartsWith("- ", StringComparison.Ordinal) || trimmed == "-")
            {
                continue;
            }

            var key = Regex.Match(trimmed, @"^(?<key>[A-Za-z_][A-Za-z0-9_.-]*):(?=[ \t]|$)");

            if (!key.Success)
            {
                continue;
            }

            foreach (var scope in scopes.Keys.Where(existing => existing > indent).ToList())
            {
                scopes.Remove(scope);
            }

            if (!scopes.TryGetValue(indent, out var keys))
            {
                keys = new HashSet<string>(StringComparer.Ordinal);
                scopes[indent] = keys;
            }

            var name = key.Groups["key"].Value;

            if (!keys.Add(name))
            {
                duplicates.Add(new DuplicateKey(name, index + 1));
            }

            var value = trimmed[key.Length..].Trim();

            if (Regex.IsMatch(value, @"^[|>][+-]?\d*([ \t]+#.*)?$"))
            {
                blockScalarIndent = indent;
            }
        }

        return duplicates;
    }

    private sealed record Workflow(string Name, string Path);

    private static IEnumerable<Workflow> Workflows()
    {
        var directory = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ".github",
            "workflows");

        return Directory.EnumerateFiles(directory, "*.yml", SearchOption.TopDirectoryOnly)
            .OrderBy(path => path, StringComparer.Ordinal)
            .Select(path => new Workflow(Path.GetFileName(path), path));
    }

    /// <summary>
    /// The workflow's <c>on:</c> block, from the key to the next column-zero
    /// key. Note GitHub Actions' <c>on</c> is the YAML 1.1 boolean <c>true</c>
    /// when parsed strictly, which is one reason this repository scans these
    /// files textually rather than through a parser.
    /// </summary>
    private static string OnBlock(string yaml)
    {
        var start = Regex.Match(yaml, @"^on:[ \t]*\r?$", RegexOptions.Multiline);
        if (!start.Success)
        {
            return string.Empty;
        }

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^[A-Za-z]", RegexOptions.Multiline);

        return next.Success ? rest[..next.Index] : rest;
    }

    /// <summary>
    /// The branch patterns listed for one trigger inside an <c>on:</c> block.
    /// Handles the inline flow sequence this repository uses throughout.
    /// </summary>
    private static IReadOnlyList<string> BranchesOf(string onBlock, string trigger)
    {
        var start = Regex.Match(
            onBlock,
            $@"^  {Regex.Escape(trigger)}:[ \t]*\r?$",
            RegexOptions.Multiline);

        if (!start.Success)
        {
            return [];
        }

        var rest = onBlock[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z]", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        var branches = Regex.Match(block, @"^    branches:[ \t]*(?<list>.+?)[ \t]*\r?$", RegexOptions.Multiline);
        if (!branches.Success)
        {
            return [];
        }

        return branches.Groups["list"].Value
            .Trim('[', ']')
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(entry => entry.Trim('\'', '"'))
            .ToArray();
    }

    /// <summary>
    /// Every top-level job, keyed by id. Scoped to the text after the
    /// column-zero <c>jobs:</c> key so that the two-space trigger keys inside
    /// <c>on:</c> are not mistaken for jobs.
    /// </summary>
    private static IEnumerable<(string Id, string Block)> Jobs(string yaml)
    {
        var jobs = Regex.Match(yaml, @"^jobs:[ \t]*\r?$", RegexOptions.Multiline);
        if (!jobs.Success)
        {
            yield break;
        }

        var body = yaml[(jobs.Index + jobs.Length)..];
        var starts = Regex.Matches(body, @"^  (?<id>[A-Za-z0-9_.-]+):[ \t]*\r?$", RegexOptions.Multiline);

        for (var i = 0; i < starts.Count; i++)
        {
            var from = starts[i].Index + starts[i].Length;
            var to = i + 1 < starts.Count ? starts[i + 1].Index : body.Length;

            yield return (starts[i].Groups["id"].Value, body[from..to]);
        }
    }

    /// <summary>
    /// Every step of one job, named by its <c>name:</c> or <c>uses:</c>.
    /// </summary>
    private static IEnumerable<(string Name, string Block)> Steps(string jobBlock)
    {
        var starts = Regex.Matches(jobBlock, @"^      - (?<head>.*)$", RegexOptions.Multiline);

        for (var i = 0; i < starts.Count; i++)
        {
            var from = starts[i].Index;
            var to = i + 1 < starts.Count ? starts[i + 1].Index : jobBlock.Length;

            yield return (starts[i].Groups["head"].Value.Trim(), jobBlock[from..to]);
        }
    }

    /// <summary>
    /// Whether a block reads pull-request-only context outside a comment.
    /// <para>
    /// Whole-line comments are stripped, in both the YAML and the embedded
    /// shell, because prose ABOUT this trap is not an instance of it - and this
    /// repository's workflows document the trap at length. Trailing comments are
    /// deliberately NOT stripped: doing so would require distinguishing a
    /// comment marker from a literal <c>#</c> inside a <c>run:</c> body, and the
    /// failure direction of leaving them in is a false RED, which is loud and
    /// cheap, rather than a false green.
    /// </para>
    /// </summary>
    private static bool ReadsPullRequestContext(string block) =>
        block.Replace("\r\n", "\n").Split('\n')
            .Where(line => !line.TrimStart().StartsWith('#'))
            .Any(line => PullRequestOnlyContext.Any(token => line.Contains(token, StringComparison.Ordinal)));

    /// <summary>
    /// The value of the <c>if:</c> key at a given indent, including the
    /// continuation lines of a folded block scalar.
    /// </summary>
    private static string Condition(string block, int indent)
    {
        var key = new string(' ', indent) + "if:";
        var condition = new StringBuilder();
        var capturing = false;

        foreach (var line in block.Replace("\r\n", "\n").Split('\n'))
        {
            if (!capturing)
            {
                if (line.StartsWith(key, StringComparison.Ordinal))
                {
                    capturing = true;
                    condition.Append(line[key.Length..].Trim()).Append(' ');
                }

                continue;
            }

            var lead = line.Length - line.TrimStart(' ').Length;
            if (line.Trim().Length == 0 || lead <= indent)
            {
                break;
            }

            condition.Append(line.Trim()).Append(' ');
        }

        return condition.ToString();
    }
}
