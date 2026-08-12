using System.Diagnostics;
using System.Net;
using System.Net.Http;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Docker-gated smoke test that builds the RepoContext MCP image from
/// <c>apps/repocontext/Dockerfile</c> and runs it in the default local durability
/// profile, asserting the container reaches readiness over its HTTP probe. It
/// validates the distroless, shell-less, non-root runtime image and the on-mount
/// data path end to end.
/// </summary>
/// <remarks>
/// Marked <c>Container</c> and <c>Explicit</c> so it is excluded from both the unit
/// and the ordinary integration tiers - it requires a Docker daemon and pulls /
/// builds multi-hundred-megabyte images. Run it deliberately with
/// <c>dotnet test --filter "TestCategory=Container" -- NUnit.Explicit=false</c> or
/// by name from an environment that has Docker.
/// </remarks>
[TestFixture]
[Category("Container")]
[Explicit("Requires a Docker daemon; builds and runs the container image.")]
public sealed class RepoContextContainerSmokeTests
{
    private const string ImageTag = "repocontext-mcp:test";
    private const string ContainerName = "repocontext-mcp-smoke";

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    [Test]
    public async Task Container_builds_and_reaches_readiness()
    {
        var hostPort = 18080;
        var dataDir = Path.Combine(Path.GetTempPath(), "repocontext-container-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);

        try
        {
            await RunAsync("docker", $"build -f apps/repocontext/Dockerfile -t {ImageTag} .", RepoRoot, TimeSpan.FromMinutes(20));

            // Run detached in the local profile. No embedder is required for the
            // silo to reach readiness (the warmup seed proves the durable stores).
            await RunAsync(
                "docker",
                $"run -d --name {ContainerName} -p {hostPort}:8080 " +
                $"-e LATTICE_DURABILITY=local -v \"{dataDir}:/data\" {ImageTag}",
                RepoRoot,
                TimeSpan.FromMinutes(1));

            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var ready = await PollAsync(
                () => client.GetAsync($"http://localhost:{hostPort}/health/ready"),
                TimeSpan.FromMinutes(3));

            Assert.That(ready, Is.True, "The container did not report readiness within the timeout.");
        }
        finally
        {
            await TryRunAsync("docker", $"rm -f {ContainerName}");
            if (Directory.Exists(dataDir))
            {
                try
                {
                    Directory.Delete(dataDir, recursive: true);
                }
                catch (IOException)
                {
                    // Best-effort cleanup.
                }
            }
        }
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

    private static async Task RunAsync(string file, string args, string workingDirectory, TimeSpan timeout)
    {
        var (exitCode, output) = await ExecAsync(file, args, workingDirectory, timeout);
        Assert.That(exitCode, Is.EqualTo(0), $"'{file} {args}' failed with exit code {exitCode}:{Environment.NewLine}{output}");
    }

    private static async Task TryRunAsync(string file, string args)
    {
        try
        {
            await ExecAsync(file, args, RepoRoot, TimeSpan.FromMinutes(1));
        }
        catch
        {
            // Cleanup is best-effort.
        }
    }

    private static async Task<(int ExitCode, string Output)> ExecAsync(
        string file,
        string args,
        string workingDirectory,
        TimeSpan timeout)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = file,
                Arguments = args,
                WorkingDirectory = workingDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            },
        };

        process.Start();
        var stdout = await process.StandardOutput.ReadToEndAsync();
        var stderr = await process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(timeout))
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"'{file} {args}' did not complete within {timeout}.");
        }

        return (process.ExitCode, stdout + Environment.NewLine + stderr);
    }
}
