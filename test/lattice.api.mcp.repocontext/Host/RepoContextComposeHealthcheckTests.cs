using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Asserts that the RepoContext MCP container declares a grain-liveness
/// <c>healthcheck</c> on its <c>repocontext</c> service, targeting the shell-less
/// <c>--healthcheck</c> self-probe with a start period sized for real silo startup.
/// <para>
/// Issue #2666. The service PUBLISHED no healthcheck while CONSUMING
/// <c>condition: service_healthy</c> from its dependencies, so the one service that
/// actually fails - the silo host - reported nothing, and an outage in which the
/// silo vanished stayed green on every automated surface until a human read
/// <c>docker ps</c> by eye.
/// </para>
/// <para>
/// <b>This guard is not the proof.</b> It reads the tracked compose file and says
/// the repository is self-consistent - that the healthcheck is present, targets the
/// self-probe rather than a probe binary the chiseled image does not contain, and
/// has a generous start period. It says nothing about a running container. The
/// behavioural proof is a live throwaway stack cycled through all four states
/// (stopped / grain-layer-failing / starting / healthy); the deterministic proof of
/// the endpoint's three-way logic is <see cref="RepoContextSiloHealthCheckTests"/>.
/// A green here across the epic #2368 gate would have coexisted with the outage,
/// because the broken container was composed from a different checkout.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextComposeHealthcheckTests
{
    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ComposePath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "docker-compose.yml");

    /// <summary>The floor below which a silo still joining would crash-loop under restart: unless-stopped.</summary>
    private static readonly TimeSpan StartPeriodFloor = TimeSpan.FromSeconds(120);

    /// <summary>
    /// Reads the <c>healthcheck</c> sub-keys of a named service, keyed by sub-key,
    /// with a hand-rolled indentation scan matching the sibling compose fixtures
    /// rather than adding a YAML parser for a handful of assertions. The two-space
    /// service / four-space property / six-space healthcheck-property indentation is
    /// stable across this file.
    /// </summary>
    private static (Dictionary<string, string> Keys, int ServicesSeen) ReadHealthcheck(
        string composePath, string targetService)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        var service = string.Empty;
        var servicesSeen = 0;
        var inHealthcheck = false;

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
                inHealthcheck = false;
                continue;
            }

            if (indent == 4 && !string.Equals(service, targetService, StringComparison.Ordinal))
            {
                inHealthcheck = false;
                continue;
            }

            if (indent == 4 && string.Equals(service, targetService, StringComparison.Ordinal))
            {
                inHealthcheck = trimmed.StartsWith("healthcheck:", StringComparison.Ordinal);
                continue;
            }

            if (inHealthcheck && indent >= 6)
            {
                var colon = trimmed.IndexOf(':', StringComparison.Ordinal);
                if (colon > 0)
                {
                    result[trimmed[..colon]] = trimmed[(colon + 1)..].Trim();
                }
            }
        }

        return (result, servicesSeen);
    }

    [Test]
    public void The_repocontext_service_declares_a_healthcheck_targeting_the_self_probe()
    {
        Assert.That(File.Exists(ComposePath), Is.True, $"expected the compose file at {ComposePath}");

        var (keys, servicesSeen) = ReadHealthcheck(ComposePath, "repocontext");

        // Anti-vacuity floor: a re-indented or renamed file that stopped yielding
        // services must fail here, not silently report an absent healthcheck as an
        // examined-and-correct one.
        Assert.That(
            servicesSeen,
            Is.GreaterThanOrEqualTo(2),
            $"the scan found only {servicesSeen} services, so it is not reading the compose file and every "
            + "assertion below would be vacuous");

        Assert.That(
            keys.ContainsKey("test"),
            Is.True,
            "the repocontext service must declare a healthcheck.test; without it the service publishes no "
            + "health while consuming service_healthy from its dependencies (issue #2666)");

        var test = keys["test"];
        Assert.Multiple(() =>
        {
            Assert.That(
                test,
                Does.Contain("--healthcheck"),
                "the healthcheck must invoke the host's own --healthcheck self-probe");
            Assert.That(
                test,
                Does.Contain("Orleans.Lattice.Api.Mcp.RepoContext.Host.dll"),
                "the self-probe is the host binary re-invoked, exec-form, because the chiseled image is shell-less");

            // The image ships no curl/wget/nc, so a healthcheck referencing one would
            // be permanently unhealthy. Pin that the fix did NOT reach for one.
            foreach (var probeTool in new[] { "curl", "wget", "\"nc\"", " nc ", "CMD-SHELL" })
            {
                Assert.That(
                    test,
                    Does.Not.Contain(probeTool),
                    $"the chiseled runtime image contains no shell or '{probeTool.Trim().Trim('\"')}'; a "
                    + "healthcheck referencing one would be permanently unhealthy");
            }
        });
    }

    [Test]
    public void The_healthcheck_start_period_covers_real_silo_startup()
    {
        var (keys, _) = ReadHealthcheck(ComposePath, "repocontext");

        Assert.That(
            keys.ContainsKey("start_period"),
            Is.True,
            "the healthcheck must set start_period; Docker's health model is two-valued, so 'starting' is "
            + "realised only by a start_period that holds early failing probes out of the retry tally. Without "
            + "one, a silo still joining is reported unhealthy and crash-loops under restart: unless-stopped");

        var raw = keys["start_period"];
        Assert.That(raw.EndsWith('s'), Is.True, $"expected a seconds-suffixed duration, found '{raw}'");
        var startPeriod = TimeSpan.FromSeconds(double.Parse(raw[..^1], CultureInfo.InvariantCulture));

        Assert.That(
            startPeriod,
            Is.GreaterThanOrEqualTo(StartPeriodFloor),
            $"start_period ({startPeriod.TotalSeconds}s) must cover real silo startup - cluster join plus WAL "
            + $"replay warmup - which is far longer than a stateless service's; below {StartPeriodFloor.TotalSeconds}s "
            + "a normal boot risks being reported unhealthy and restarted mid-join");
    }
}
