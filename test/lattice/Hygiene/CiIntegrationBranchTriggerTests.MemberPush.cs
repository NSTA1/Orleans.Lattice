using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The member-push arm of the integration-branch trigger gate (#4260).
/// <para>
/// The <c>'*/epic/**'</c> push trigger matches a bucket
/// <c>&lt;type&gt;/epic/&lt;bucket&gt;</c> AND each of its members
/// <c>&lt;type&gt;/epic/&lt;bucket&gt;-&lt;item&gt;</c>, and no glob can separate
/// the two because bucket slugs contain hyphens too. A push lane with no
/// classifier therefore re-runs its whole suite on every push to a member pull
/// request, on top of the pull_request run that already gates the same commit.
/// Measured on one member push before this gate, that was about 31 of 140
/// runner-minutes re-proving something another run in the same push proved.
/// </para>
/// <para>
/// <c>ci.yml</c>'s <c>pushref</c> step is the source of truth for the rule (a
/// remote head that is a proper prefix of the pushed branch, followed by
/// <c>-</c>). The advisory push lanes carry a copy of that script, because a
/// change to it in the required path should not be a side effect of a change to
/// an advisory one; this arm pins every copy to the original so the copies
/// cannot drift, and requires every job of a push lane to skip on a member push.
/// </para>
/// </summary>
public sealed partial class CiIntegrationBranchTriggerTests
{
    /// <summary>The step id of the member-push classifier, in every workflow.</summary>
    private const string ClassifierStepId = "pushref";

    /// <summary>The job output every classifier job publishes.</summary>
    private const string ClassifierOutput = "push_is_member";

    /// <summary>The workflow whose classifier is the source of truth.</summary>
    private const string SourceOfTruthWorkflow = "ci.yml";

    /// <summary>
    /// Status functions that make a job run even when a job it needs was
    /// skipped, which defeats skip propagation from a guarded upstream job.
    /// </summary>
    private static readonly string[] SkipBypassingFunctions = ["always()", "failure()", "cancelled()"];

    /// <summary>
    /// Every job of a workflow carrying the integration-branch push trigger
    /// either is the member-push classifier, conditions itself on the
    /// classifier's output, or needs a job that does (so it is skipped with
    /// it). <c>ci.yml</c> is graded through its plan gate, whose first arm reads
    /// the classifier, because its legs read plan outputs derived from that gate
    /// rather than the flag itself, and its content gates are deliberately cheap
    /// and report on work they genuinely performed.
    /// </summary>
    [Test]
    public void Every_job_on_an_integration_push_lane_skips_on_a_member_push()
    {
        var population = new List<string>();
        var graded = new List<string>();
        var offenders = new List<string>();

        foreach (var workflow in Workflows())
        {
            var text = File.ReadAllText(workflow.Path);

            if (BranchesOf(OnBlock(text), "push").All(b => b != IntegrationBranchPattern))
            {
                continue;
            }

            population.Add(workflow.Name);

            var jobs = Jobs(text).ToList();
            var classifiers = jobs.Where(job => ClassifierStep(job.Block) is not null).ToList();

            if (classifiers.Count != 1)
            {
                offenders.Add(
                    workflow.Name + ": expected exactly one job carrying an `id: " + ClassifierStepId
                        + "` member-push classifier step, found " + classifiers.Count);
                continue;
            }

            var (classifierId, classifierBlock) = classifiers[0];
            var step = ClassifierStep(classifierBlock)!;

            if (!Condition(step, indent: 8).Contains("github.event_name == 'push'", StringComparison.Ordinal))
            {
                offenders.Add(
                    workflow.Name + ":" + classifierId + ": the classifier step must be conditioned on "
                        + "github.event_name == 'push', so a pull_request run publishes an empty flag");
            }

            if (!Regex.IsMatch(
                    classifierBlock,
                    @"^      " + ClassifierOutput + @":[ \t]*\$\{\{[ \t]*steps\." + ClassifierStepId
                        + @"\.outputs\.member[ \t]*\}\}",
                    RegexOptions.Multiline))
            {
                offenders.Add(
                    workflow.Name + ":" + classifierId + ": the job must publish `" + ClassifierOutput
                        + ": ${{ steps." + ClassifierStepId + ".outputs.member }}`");
            }

            if (workflow.Name == SourceOfTruthWorkflow)
            {
                if (!classifierBlock.Contains(
                        "PUSH_IS_MEMBER: ${{ steps." + ClassifierStepId + ".outputs.member }}",
                        StringComparison.Ordinal))
                {
                    offenders.Add(
                        workflow.Name + ":" + classifierId + ": the plan gate no longer reads the member-push "
                            + "flag, so the test legs would run on a member push");
                }

                continue;
            }

            var guarded = new HashSet<string>(StringComparer.Ordinal) { classifierId };
            var memberExclusion = "needs." + classifierId + ".outputs." + ClassifierOutput + " != 'true'";
            bool progressed;

            do
            {
                progressed = false;

                foreach (var (id, block) in jobs)
                {
                    if (guarded.Contains(id))
                    {
                        continue;
                    }

                    var needs = Needs(block);
                    var condition = Condition(block, indent: 4);
                    var conditioned = needs.Contains(classifierId)
                        && condition.Contains(memberExclusion, StringComparison.Ordinal);
                    var propagated = needs.Any(need => need != classifierId && guarded.Contains(need))
                        && !SkipBypassingFunctions.Any(f => condition.Contains(f, StringComparison.Ordinal));

                    if (conditioned || propagated)
                    {
                        guarded.Add(id);
                        progressed = true;
                    }
                }
            }
            while (progressed);

            foreach (var (id, _) in jobs.Where(job => job.Id != classifierId))
            {
                graded.Add(workflow.Name + ":" + id);

                if (!guarded.Contains(id))
                {
                    offenders.Add(
                        workflow.Name + ":" + id + ": runs on a member push. Give it `needs: " + classifierId
                            + "` and `if: " + memberExclusion + "`, or make it need a job that does");
                }
            }
        }

        Assert.That(
            population,
            Does.Contain(SourceOfTruthWorkflow),
            "ci.yml carries the '" + IntegrationBranchPattern + "' push trigger and is not in the derived "
                + "population; the trigger scan is broken");

        Assert.That(
            offenders,
            Is.Empty,
            "these push lanes re-run on member branches, whose own pull_request run already gates the same "
                + "commit: " + string.Join("; ", offenders));

        Assert.That(
            graded,
            Is.Not.Empty,
            "no advisory push lane's jobs were graded, so this gate has never demonstrated it can tell a guarded "
                + "job from an unguarded one; the scan has stopped matching");
    }

    /// <summary>
    /// Every copy of the member-push classifier script runs exactly the rule
    /// <c>ci.yml</c>'s does, compared line by line once indentation, blank
    /// lines and whole-line comments are removed.
    /// </summary>
    [Test]
    public void Every_member_push_classifier_matches_the_ci_yml_source_of_truth()
    {
        var bodies = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);

        foreach (var workflow in Workflows())
        {
            foreach (var (_, block) in Jobs(File.ReadAllText(workflow.Path)))
            {
                var step = ClassifierStep(block);

                if (step is not null)
                {
                    bodies[workflow.Name] = RunBody(step);
                }
            }
        }

        Assert.That(
            bodies.ContainsKey(SourceOfTruthWorkflow),
            Is.True,
            "ci.yml has no `id: " + ClassifierStepId + "` step; the source of truth is missing or the step scan "
                + "is broken");

        var source = bodies[SourceOfTruthWorkflow];

        Assert.That(
            source.Any(line => line.Contains("git ls-remote --heads origin", StringComparison.Ordinal))
                && source.Any(line => line.Contains("\"$branch\"-*)", StringComparison.Ordinal)),
            Is.True,
            "ci.yml's classifier body no longer reads as the member-push rule; the run-body extraction is broken");

        Assert.That(
            bodies.Count,
            Is.GreaterThanOrEqualTo(2),
            "no copy of the classifier was found outside ci.yml, so this comparison checks nothing");

        var drifted = bodies
            .Where(pair => pair.Key != SourceOfTruthWorkflow && !pair.Value.SequenceEqual(source))
            .Select(pair => pair.Key)
            .ToList();

        Assert.That(
            drifted,
            Is.Empty,
            "these workflows' member-push classifier differs from ci.yml's `" + ClassifierStepId
                + "` step, which is the source of truth: " + string.Join(", ", drifted));
    }

    /// <summary>The step of a job carrying <c>id: pushref</c>, or null.</summary>
    private static string? ClassifierStep(string jobBlock) =>
        Steps(jobBlock)
            .Select(step => step.Block)
            .FirstOrDefault(block => Regex.IsMatch(
                block,
                @"^ +id:[ \t]*" + ClassifierStepId + @"[ \t]*\r?$",
                RegexOptions.Multiline));

    /// <summary>
    /// A job's <c>needs:</c>, in the scalar, flow-sequence and block-sequence
    /// spellings.
    /// </summary>
    private static IReadOnlyList<string> Needs(string jobBlock)
    {
        var lines = jobBlock.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');

        for (var i = 0; i < lines.Length; i++)
        {
            var match = Regex.Match(lines[i], @"^    needs:[ \t]*(?<value>.*?)[ \t]*$");

            if (!match.Success)
            {
                continue;
            }

            var value = match.Groups["value"].Value;

            if (value.Length > 0)
            {
                return value.Trim('[', ']')
                    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .ToArray();
            }

            return lines.Skip(i + 1)
                .TakeWhile(line => Regex.IsMatch(line, @"^      - "))
                .Select(line => line.Trim()[2..].Trim())
                .ToArray();
        }

        return [];
    }

    /// <summary>
    /// The lines of a step's <c>run: |</c> block scalar, trimmed, with blank
    /// lines and whole-line comments removed.
    /// </summary>
    private static IReadOnlyList<string> RunBody(string stepBlock)
    {
        var lines = stepBlock.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var body = new List<string>();
        var keyIndent = -1;

        foreach (var line in lines)
        {
            var indent = line.Length - line.TrimStart(' ').Length;

            if (keyIndent < 0)
            {
                if (Regex.IsMatch(line, @"^ +run:[ \t]*\|[ \t]*$"))
                {
                    keyIndent = indent;
                }

                continue;
            }

            if (line.Trim().Length == 0)
            {
                continue;
            }

            if (indent <= keyIndent)
            {
                break;
            }

            var trimmed = line.Trim();

            if (!trimmed.StartsWith('#'))
            {
                body.Add(trimmed);
            }
        }

        return body;
    }
}
