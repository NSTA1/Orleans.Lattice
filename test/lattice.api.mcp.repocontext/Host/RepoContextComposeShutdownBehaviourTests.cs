using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Docker-gated proof that the sample stack STOPS cleanly: a plain
/// <c>docker compose stop</c> returns without a kill, and the container reports
/// a normal exit code rather than 137.
/// <para>
/// Issue #2576. This fixture exists because asserting the configuration is not
/// the same as asserting the behaviour, and the whole reason the defect survived
/// is that its absence was invisible until a shutdown was actually needed. A
/// test that reads <c>init: true</c> out of the YAML has exactly that blind spot
/// relocated: it would pass against a stack that still could not be torn down,
/// because it never tears one down. <c>RepoContextComposeInitProcessTests</c>
/// holds the configuration invariant in the fast lane; this fixture is the one
/// that demonstrates the property the configuration is supposed to buy.
/// </para>
/// <para>
/// <b>Three outcomes are distinguishable, which is what makes the assertion
/// worth making.</b> Exit 0 is a drain that completed inside the host's budget;
/// exit <see cref="Api.Mcp.RepoContext.Host.RepoContextExitCode.DrainAbandoned"/>
/// is a drain that ran out of budget and reported it (issue #2401); 137 is
/// SIGKILL, meaning Docker gave up waiting and the WAL was never flushed. Only
/// the first is a clean stop, and the third is the state that destroyed the
/// epic #2368 gate measurement.
/// </para>
/// <para>
/// <b>Read the corpus caveat before treating a green run as proof, because the
/// two arms differ in how much a green is worth.</b> The
/// <c>HostConfig.Init</c> assertion is a daemon-reported configuration fact and
/// is meaningful at any corpus size: PID 1 either is an init process or it is
/// not, and an empty repository cannot make it look like one. <b>The exit-code
/// assertion is not.</b> A container holding no durable state drains in
/// milliseconds and exits 0 <i>even when the grace period is wrong</i>, so on
/// the empty corpus this fixture mounts by default, exit 0 confirms the
/// <c>init</c> half and says nothing whatsoever about the grace period. That
/// vacuity is not hypothetical - it is the trap recorded against issue #2389,
/// where a green from a too-small rig read as a proof and was not.
/// </para>
/// <para>
/// To make the exit-code arm load-bearing, run the positive control first, in
/// this order: with the fix ABSENT, load the stack until it holds real state
/// and reproduce exit 137; only then apply the fix and repeat with an identical
/// command. A synthetic corpus of roughly 400 files (about 3 MB, ~19k symbols)
/// was sufficient to reproduce 137 on issue #2389. Use a synthetic corpus and
/// never the live volume - a copy of SQLite/WAL taken while a silo is writing
/// can be torn and produces phantom faults.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Container</c> and <c>Explicit</c> for the same reasons as
/// <see cref="RepoContextContainerSmokeTests"/>: it needs a Docker daemon and
/// builds multi-hundred-megabyte images. Run it deliberately with
/// <c>dotnet test --filter "TestCategory=Container" -- NUnit.Explicit=false</c>.
/// <para>
/// <b>It never touches another stack.</b> The compose project name and the
/// published port are both unique per run, and teardown is scoped to that
/// project name, so a stack running elsewhere on the same daemon - a live
/// deployment on the default <c>repocontextcontainer</c> project, say - is
/// neither stopped, adopted, nor removed. The workspace is bound to an empty
/// temporary directory so the box registers no repository and stays idle:
/// nothing here is a throughput measurement, and an idle host keeps the run
/// cheap on a loaded machine.
/// </para>
/// </remarks>
[TestFixture]
[Category("Container")]
[Explicit("Requires a Docker daemon; builds and runs the container images.")]
public sealed class RepoContextComposeShutdownBehaviourTests
{
    /// <summary>The exit code Docker reports for a container killed by SIGKILL (128 + 9).</summary>
    private const int SigkillExitCode = 137;

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string SampleDirectory => Path.Combine(RepoRoot, "samples", "RepoContextContainer");

    [Test]
    public async Task A_plain_compose_stop_drains_the_host_and_exits_without_a_kill()
    {
        // Unique per run so a stack running elsewhere on this daemon is untouched,
        // and so two concurrent runs cannot adopt each other's containers.
        var project = "rc-init-" + Guid.NewGuid().ToString("N")[..8];
        var hostPort = FreeTcpPort();
        var workspace = Path.Combine(Path.GetTempPath(), project + "-workspace");
        Directory.CreateDirectory(workspace);

        var environment = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["REPOCONTEXT_PORT"] = hostPort.ToString(CultureInfo.InvariantCulture),
            ["REPO_PATH"] = workspace,
        };

        try
        {
            await RunAsync(
                $"compose -p {project} up -d --build",
                environment,
                TimeSpan.FromMinutes(25));

            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var ready = await PollAsync(
                () => client.GetAsync($"http://localhost:{hostPort}/health/ready"),
                TimeSpan.FromMinutes(5));

            Assert.That(ready, Is.True, "the container did not report readiness within the timeout");

            var container = (await RunAsync(
                $"compose -p {project} ps -q repocontext",
                environment,
                TimeSpan.FromMinutes(1))).Output.Trim().Split('\n')[0].Trim();

            Assert.That(container, Is.Not.Empty, "could not resolve the running repocontext container id");

            // The runtime check, not the YAML one. `init: true` in the file is a
            // request; this is the daemon reporting that the request took effect and
            // PID 1 in the running container really is an init process.
            var init = (await RunAsync(
                $"inspect {container} --format {{{{.HostConfig.Init}}}}",
                environment,
                TimeSpan.FromMinutes(1))).Output.Trim();

            Assert.That(
                init,
                Is.EqualTo("true").IgnoreCase,
                "the running container reports HostConfig.Init=" + init + ", so PID 1 is the host process "
                + "itself rather than an init process (issue #2576)");

            // A PLAIN stop. No `-t` override, so the compose file's own
            // stop_grace_period governs; no `-f`, no `docker kill`, no `rm -f`. If
            // this needs a kill to return, the defect is still present.
            var stopped = Stopwatch.StartNew();
            await RunAsync($"compose -p {project} stop", environment, TimeSpan.FromMinutes(5));
            stopped.Stop();

            var exitCode = (await RunAsync(
                $"inspect {container} --format {{{{.State.ExitCode}}}}",
                environment,
                TimeSpan.FromMinutes(1))).Output.Trim();

            Assert.That(
                exitCode,
                Is.Not.EqualTo(SigkillExitCode.ToString(CultureInfo.InvariantCulture)),
                $"the container exited {SigkillExitCode} (SIGKILL) after a plain compose stop, so Docker gave "
                + "up waiting and the WAL was never flushed - the exact state that destroyed the epic #2368 "
                + $"gate measurement. The stop took {stopped.Elapsed.TotalSeconds:F1}s.");

            Assert.That(
                exitCode,
                Is.EqualTo("0"),
                $"expected a clean drain (exit 0) but the container exited {exitCode}; "
                + $"{Api.Mcp.RepoContext.Host.RepoContextExitCode.DrainAbandoned} means the drain ran out of "
                + "the host shutdown budget rather than being killed by Docker");

            // The drain either completed or it did not, and the host says which. Its
            // absence means the process was torn down mid-drain, which a bare exit
            // code cannot always distinguish.
            var logs = (await RunAsync(
                $"compose -p {project} logs --no-color repocontext",
                environment,
                TimeSpan.FromMinutes(2))).Output;

            Assert.That(
                logs,
                Does.Contain("RepoContext drain complete"),
                "the host never logged a completed drain, so it did not finish deactivating the silo and "
                + "flushing the WAL commit-log before the process exited");

            // THE DEPLOYMENT-REACHING ASSERTION, and the reason this fixture is not
            // redundant with the tracked-file guard.
            //
            // RepoContextComposeInitProcessTests compares two values inside the
            // repository and would have been GREEN throughout both epic #2368 gate
            // runs, because both files were correct and the deployment was launched
            // from a different checkout entirely - one whose compose declared no
            // grant at all. File-to-file agreement is simply not the property that
            // failed. What failed is that the agreed value never reached a
            // container, and the only place that is observable is a running one.
            //
            // The host already distinguishes the two cases and says which it
            // believes, taking the "is unset" branch when no grant was declared.
            // That line was emitted on both gate-run containers, correctly and in
            // plain language, and was not acted on - so this asserts on it rather
            // than adding a further signal nobody reads.
            Assert.That(
                logs,
                Does.Not.Contain($"{Api.Mcp.RepoContext.Host.RepoContextShutdownBudget.StopGracePeriodKey} is unset"),
                $"the container started without {Api.Mcp.RepoContext.Host.RepoContextShutdownBudget.StopGracePeriodKey} in its "
                + "environment, so the host derived its budget from an assumed grant while Docker applied "
                + "its own 10s default. The compose file declaring it is not sufficient - this is the state "
                + "the epic #2368 gate-run containers were in, launched from a checkout whose compose "
                + "declared no grant.");

            // CONFOUND. An embedder OOM-killed during teardown fails the drain for a
            // wholly unrelated reason and looks identical at the exit code, so an arm
            // that does not exclude it can report a false negative with confidence.
            var health = (await RunAsync(
                $"inspect {container} --format {{{{.State.OOMKilled}}}}/{{{{.RestartCount}}}}",
                environment,
                TimeSpan.FromMinutes(1))).Output.Trim();

            Assert.That(
                health,
                Is.EqualTo("false/0").IgnoreCase,
                $"the container reports OOMKilled/RestartCount = {health}; a container that was memory-killed "
                + "or restarted mid-run did not exercise the shutdown path this test claims to measure, so "
                + "this arm is void rather than passing or failing");

            // The teardown must also be clean, and for the same reason: `down`
            // without `-f` or a manual kill is what an operator actually runs.
            await RunAsync($"compose -p {project} down -v", environment, TimeSpan.FromMinutes(5));
        }
        finally
        {
            // Scoped to this run's project name only, so nothing else on the daemon
            // is affected even when the assertions above have already failed.
            await TryRunAsync($"compose -p {project} down -v --remove-orphans", environment);

            try
            {
                Directory.Delete(workspace, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    /// <summary>Leases an ephemeral loopback port so concurrent runs cannot collide.</summary>
    private static int FreeTcpPort()
    {
        using var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task<bool> PollAsync(Func<Task<HttpResponseMessage>> probe, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var response = await probe();
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return true;
                }
            }
            catch (HttpRequestException)
            {
                // Container not yet listening.
            }

            await Task.Delay(2000);
        }

        return false;
    }

    private static async Task<(int ExitCode, string Output)> RunAsync(
        string args,
        IReadOnlyDictionary<string, string> environment,
        TimeSpan timeout)
    {
        var result = await ExecAsync(args, environment, timeout);
        Assert.That(
            result.ExitCode,
            Is.EqualTo(0),
            $"'docker {args}' failed with exit code {result.ExitCode}:{Environment.NewLine}{result.Output}");
        return result;
    }

    private static async Task TryRunAsync(string args, IReadOnlyDictionary<string, string> environment)
    {
        try
        {
            await ExecAsync(args, environment, TimeSpan.FromMinutes(5));
        }
        catch
        {
            // Cleanup is best-effort.
        }
    }

    private static async Task<(int ExitCode, string Output)> ExecAsync(
        string args,
        IReadOnlyDictionary<string, string> environment,
        TimeSpan timeout)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = "docker",
            Arguments = args,
            WorkingDirectory = SampleDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        foreach (var (key, value) in environment)
        {
            startInfo.Environment[key] = value;
        }

        using var process = new Process { StartInfo = startInfo };

        process.Start();
        var stdout = await process.StandardOutput.ReadToEndAsync();
        var stderr = await process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(timeout))
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"'docker {args}' did not complete within {timeout}.");
        }

        return (process.ExitCode, stdout + Environment.NewLine + stderr);
    }
}
