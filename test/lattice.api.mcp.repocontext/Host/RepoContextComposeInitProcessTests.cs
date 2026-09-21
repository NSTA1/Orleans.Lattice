using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Asserts that every compose file which runs the RepoContext MCP host declares
/// <c>init: true</c>, so PID 1 inside the container is an init process rather
/// than the host itself.
/// <para>
/// Issue #2576. During the epic #2368 gate runs a container reached a state in
/// which neither <c>docker kill</c> nor <c>docker rm -f</c> would reap PID 1, so
/// it had to be SIGKILLed. Orleans never flushed, the following run inherited
/// unbanked state to replay, and time-to-ready stopped being a comparable
/// measurement for the whole gate - in the direction that flatters the earlier
/// run, so the comparison had to be discarded rather than reported with a
/// caveat.
/// </para>
/// <para>
/// <b>Why this is a separate guard from <see cref="RepoContextComposeShutdownBudgetTests"/>.</b>
/// The two settings fix different halves of the same teardown, and either one
/// alone leaves a container that cannot be relied on to stop. The grace period
/// decides how long the drain is allowed to take; <c>init</c> decides whether
/// the SIGTERM that starts it is honoured at all, because the kernel applies no
/// default action to a signal delivered to PID 1 for which PID 1 has installed
/// no handler, and because nothing else in the container reaps orphaned
/// descendants. This file was in exactly that half-fixed state when #2576 was
/// raised: a carefully derived 120s grace period sitting above a PID 1 that was
/// not an init process.
/// </para>
/// <para>
/// <b>This guard is not the proof.</b> The point of #2576 is that the absence of
/// this configuration was invisible until a shutdown was needed, and a test that
/// greps YAML has the same blind spot in a different place. The behavioural
/// proof - a real stack, a plain <c>docker compose stop</c>, and a normal exit
/// code rather than 137 - lives in
/// <c>RepoContextComposeShutdownBehaviourTests</c>. This fixture exists because
/// that one is Docker-gated and cannot run in the ordinary lane, so something
/// has to hold the invariant continuously.
/// </para>
/// <para>
/// <b>What a green run here does NOT establish.</b> This fixture reads the
/// tracked compose files and compares them to each other and to the host's own
/// budget. It therefore says the repository is self-consistent, and nothing at
/// all about the deployment. Across both failed epic #2368 gate runs these
/// files were correct and in agreement the entire time: the container that
/// could not be reaped had been composed from a different checkout, whose
/// compose declared none of this. A fixture of this shape would have been green
/// throughout. Do not read it as evidence that a running container is
/// configured correctly - only an assertion against a live container can say
/// that, which is why the runtime checks live in the behavioural fixture, and
/// why the cold-start rig enforces the same invariant in
/// <c>Assert-RigComposeIsolation</c> against the document
/// <c>docker compose config</c> actually resolves rather than against these
/// files.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextComposeInitProcessTests
{
    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    /// <summary>
    /// Every tracked compose file that runs the MCP host, with the services in it
    /// that must run under an init process.
    /// <para>
    /// The cold-start rig is in this list deliberately, and it is not an
    /// afterthought to the sample. The rig is what produces the time-to-ready
    /// measurement, so a rig container that cannot be torn down cleanly poisons
    /// the NEXT run rather than its own: an unflushed WAL leaves unbanked state
    /// for the following start to replay. Fixing only the sample would repair
    /// the container that was not measuring and leave broken the one that was.
    /// </para>
    /// </summary>
    private static readonly (string Label, string[] Segments)[] ComposeFiles =
    [
        ("the live sample", ["samples", "RepoContextContainer", "docker-compose.yml"]),
        ("the cold-start rig", ["benchmark", "coldstart-rig", "docker-compose.rig.yml"]),
    ];

    /// <summary>The services that must run under an init process in every file above.</summary>
    private static readonly string[] RequiredServices = ["embedder", "repocontext"];

    private static string PathFor((string Label, string[] Segments) file) =>
        Path.Combine([RepoRoot, .. file.Segments]);

    /// <summary>
    /// Reads the <c>init</c> setting of every service in a compose file, keyed by
    /// service. Scoped by service rather than searched for as a substring for the
    /// same reason the sibling grace-period scan is: an <c>init: true</c> attached
    /// to the wrong service is indistinguishable from a correct one to a substring
    /// search, and would leave the defect fully in place.
    /// <para>
    /// A hand-rolled scan rather than a YAML dependency, matching
    /// <see cref="RepoContextComposeShutdownBudgetTests"/> - the two-space service
    /// indentation is stable, and the alternative is adding a parser to the test
    /// project for one assertion.
    /// </para>
    /// </summary>
    private static (Dictionary<string, string> Init, int ServicesSeen) ReadInitSettings(string composePath)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        var service = string.Empty;
        var servicesSeen = 0;

        foreach (var raw in File.ReadAllLines(composePath))
        {
            var trimmed = raw.TrimStart();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }

            var indent = raw.Length - trimmed.Length;

            if (indent == 2 && trimmed.EndsWith(':') && !trimmed.Contains(' ', StringComparison.Ordinal))
            {
                service = trimmed[..^1];
                servicesSeen++;
                continue;
            }

            if (indent == 4 && trimmed.StartsWith("init:", StringComparison.Ordinal))
            {
                result[service] = trimmed["init:".Length..].Trim();
            }
        }

        return (result, servicesSeen);
    }

    [Test]
    public void Every_compose_file_that_runs_the_host_sets_init_on_every_service()
    {
        // Anti-vacuity floor on the DENOMINATOR (issue #2275). A scan that silently
        // stopped finding services - a re-indented file, a renamed path, a rewritten
        // top-level shape - fails here rather than reporting a fully configured set
        // of files it never actually examined.
        Assert.That(ComposeFiles, Has.Length.GreaterThanOrEqualTo(2));

        Assert.Multiple(() =>
        {
            foreach (var file in ComposeFiles)
            {
                var path = PathFor(file);
                Assert.That(File.Exists(path), Is.True, $"expected {file.Label} compose file at {path}");

                var (init, servicesSeen) = ReadInitSettings(path);

                Assert.That(
                    servicesSeen,
                    Is.GreaterThanOrEqualTo(RequiredServices.Length),
                    $"the scan of {file.Label} found only {servicesSeen} services, so it is not reading the "
                    + "file it is asserting about and every result below would be vacuous");

                foreach (var required in RequiredServices)
                {
                    Assert.That(
                        init.ContainsKey(required),
                        Is.True,
                        $"the '{required}' service in {file.Label} must set init: true; without it the "
                        + "container's PID 1 reaps no orphaned children and takes no default action for a "
                        + "signal it has installed no handler for (issue #2576)");

                    Assert.That(
                        init[required],
                        Is.EqualTo("true"),
                        $"the '{required}' service in {file.Label} sets init to '{init[required]}'; only "
                        + "true runs the container under an init process");
                }
            }
        });
    }

    /// <summary>
    /// Reads the <c>stop_grace_period</c> of every service in a compose file.
    /// Duplicated from <see cref="RepoContextComposeShutdownBudgetTests"/> rather
    /// than shared, because that fixture asserts about the sample specifically and
    /// this one extends the same invariant to the rig; coupling them would make a
    /// change to either fixture's scope silently change the other's.
    /// </summary>
    private static Dictionary<string, TimeSpan> ReadStopGracePeriods(string composePath)
    {
        var result = new Dictionary<string, TimeSpan>(StringComparer.Ordinal);
        var service = string.Empty;

        foreach (var raw in File.ReadAllLines(composePath))
        {
            var trimmed = raw.TrimStart();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }

            var indent = raw.Length - trimmed.Length;

            if (indent == 2 && trimmed.EndsWith(':') && !trimmed.Contains(' ', StringComparison.Ordinal))
            {
                service = trimmed[..^1];
                continue;
            }

            if (!trimmed.StartsWith("stop_grace_period:", StringComparison.Ordinal))
            {
                continue;
            }

            var value = trimmed["stop_grace_period:".Length..].Trim();
            Assert.That(
                value.EndsWith('s'),
                Is.True,
                $"expected a seconds-suffixed duration for {service}, found '{value}'");

            result[service] = TimeSpan.FromSeconds(double.Parse(
                value[..^1],
                System.Globalization.CultureInfo.InvariantCulture));
        }

        return result;
    }

    /// <summary>
    /// Reads the declared container grant from each service's <c>environment</c>
    /// block, keyed by service.
    /// </summary>
    private static Dictionary<string, string> ReadDeclaredGrants(string composePath)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        var service = string.Empty;
        var prefix = RepoContextShutdownBudget.StopGracePeriodKey + ":";

        foreach (var raw in File.ReadAllLines(composePath))
        {
            var trimmed = raw.TrimStart();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }

            var indent = raw.Length - trimmed.Length;

            if (indent == 2 && trimmed.EndsWith(':') && !trimmed.Contains(' ', StringComparison.Ordinal))
            {
                service = trimmed[..^1];
                continue;
            }

            if (trimmed.StartsWith(prefix, StringComparison.Ordinal))
            {
                result[service] = trimmed[prefix.Length..].Trim().Trim('"');
            }
        }

        return result;
    }

    [Test]
    public void The_cold_start_rig_grants_the_host_the_shutdown_budget_it_asks_for()
    {
        // The rig was left out of the #2389 fix, which covered the sample only. That
        // is the worse of the two omissions: a rig teardown that is a crash teardown
        // leaves an unbanked WAL on the working volume, so the cold start the NEXT
        // run measures begins from a replay the change under test did not cause.
        var path = PathFor(ComposeFiles[1]);
        var periods = ReadStopGracePeriods(path);

        Assert.That(
            periods.ContainsKey("repocontext"),
            Is.True,
            "the rig's repocontext service must set stop_grace_period explicitly, or Docker's 10s default "
            + "applies and every rig teardown is a crash teardown (issue #2389)");

        Assert.That(
            periods["repocontext"],
            Is.GreaterThan(RepoContextHostBuilder.ShutdownBudget),
            $"the rig's stop_grace_period ({periods["repocontext"].TotalSeconds}s) must exceed the host "
            + $"shutdown budget ({RepoContextHostBuilder.ShutdownBudget.TotalSeconds}s)");
    }

    [Test]
    public void The_cold_start_rig_declares_the_grant_it_actually_gives()
    {
        var path = PathFor(ComposeFiles[1]);
        var periods = ReadStopGracePeriods(path);
        var grants = ReadDeclaredGrants(path);

        Assert.That(
            grants.ContainsKey("repocontext"),
            Is.True,
            $"the rig's repocontext service must declare {RepoContextShutdownBudget.StopGracePeriodKey}; the "
            + "host cannot read its own stop_grace_period and derives its whole budget from this declaration");

        // The host derives its budget from the declaration and cannot observe the
        // real grant, so a declaration LARGER than the grant silences the
        // drain-abandoned line and reintroduces the silent kill of #2389 - the one
        // signal that would tell an operator a rig measurement spanned a crash
        // teardown. Asserting both are merely present would pass in that state.
        Assert.That(
            RepoContextShutdownBudget.ParseStopGracePeriod(grants["repocontext"]),
            Is.EqualTo(periods["repocontext"]),
            $"{RepoContextShutdownBudget.StopGracePeriodKey} ('{grants["repocontext"]}') must equal the rig's "
            + $"stop_grace_period ({periods["repocontext"].TotalSeconds}s)");
    }
}
