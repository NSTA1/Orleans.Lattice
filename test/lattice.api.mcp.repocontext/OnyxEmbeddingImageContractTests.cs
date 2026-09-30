using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// A CI-gated contract test that the produced <c>apps/embedding</c> image really
/// serves an embedding for a known input at the expected dimension. It builds the
/// image, runs it, waits for the health endpoint, embeds a passage through the
/// same wire contract <see cref="OnyxEmbeddingProvider"/> uses, and asserts the
/// returned vector is 768-dimensional (the baked <c>nomic-embed-text-v1</c> space).
/// </summary>
/// <remarks>
/// Tagged <c>Container</c> and <see cref="ExplicitAttribute"/> so it is excluded
/// from the unit run (<c>TestCategory!=Container</c>) and only executes on a host
/// with a Docker daemon. Building the base image is a multi-GB, minutes-long
/// operation, so it never runs in the inner loop.
/// </remarks>
[TestFixture]
[Category("Container")]
[Explicit("Builds and runs the multi-GB Onyx model-server image; CI/container lane only.")]
public sealed class OnyxEmbeddingImageContractTests
{
    private const int ExpectedDimension = 768;
    private const string ImageTag = "orleans-lattice-embedding:contract-test";
    private const string ContainerName = "orleans-lattice-embedding-contract";

    [Test]
    public async Task Built_image_embeds_a_passage_at_the_expected_dimension()
    {
        var contextPath = ResolveAppContextPath();
        Docker("build", "-t", ImageTag, contextPath);

        var hostPort = 19000;
        Docker("run", "-d", "--rm", "--name", ContainerName, "-p", $"{hostPort}:9000", ImageTag);
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri($"http://localhost:{hostPort}/") };
            await WaitForHealthAsync(client);

            using var response = await client.PostAsJsonAsync(
                "encoder/bi-encoder-embed",
                new
                {
                    texts = new[] { "the quick brown fox" },
                    model_name = OnyxEmbeddingOptions.DefaultModelName,
                    max_context_length = OnyxEmbeddingOptions.DefaultMaxContextLength,
                    normalize_embeddings = true,
                    text_type = "passage",
                    provider_type = (string?)null,
                });

            response.EnsureSuccessStatusCode();
            var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
            var vector = payload.GetProperty("embeddings").EnumerateArray().Single();

            Assert.That(vector.GetArrayLength(), Is.EqualTo(ExpectedDimension));
        }
        finally
        {
            TryDocker("rm", "-f", ContainerName);
        }
    }

    private static async Task WaitForHealthAsync(HttpClient client)
    {
        var deadline = DateTime.UtcNow.AddMinutes(3);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var health = await client.GetAsync("api/health");
                if (health.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch (HttpRequestException)
            {
                // Server not up yet; keep polling until the deadline.
            }

            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        Assert.Fail("The embedding container did not become healthy within the timeout.");
    }

    private static string ResolveAppContextPath()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            var candidate = Path.Combine(directory.FullName, "apps", "embedding");
            if (Directory.Exists(candidate))
            {
                return candidate;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the apps/embedding build context.");
    }

    private static void Docker(params string[] arguments)
    {
        var exitCode = RunDocker(arguments, out var output);
        if (exitCode != 0)
        {
            Assert.Fail($"docker {string.Join(' ', arguments)} failed ({exitCode}): {output}");
        }
    }

    private static void TryDocker(params string[] arguments) => RunDocker(arguments, out _);

    private static int RunDocker(string[] arguments, out string output)
    {
        var startInfo = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        using var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the docker process.");

        // Both pipes must be drained concurrently. Reading one to completion before the other
        // starts deadlocks as soon as the child fills the unread pipe's buffer, and the hang
        // happens inside the read, so WaitForExit is never reached and bounds nothing.
        var standardOutputTask = process.StandardOutput.ReadToEndAsync();
        var standardErrorTask = process.StandardError.ReadToEndAsync();

        // The wait is bounded too. With both pipes draining, what is left to hang is
        // docker itself - a daemon that is starting, unreachable, or pulling a layer
        // over a stalled connection. Unbounded, that is a CI abort naming no fixture;
        // bounded, it is a failure that says docker overran.
        if (!process.WaitForExit((int)DockerTimeout.TotalMilliseconds))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException) { /* already exited */ }
            catch (NotSupportedException) { /* unsupported here */ }
            catch (System.ComponentModel.Win32Exception) { /* the OS refused; the diagnosis below is the useful part */ }

            output = $"docker {string.Join(' ', arguments)} did not exit within "
                + $"{DockerTimeout.TotalSeconds:0}s and was terminated. Both output pipes were being "
                + "drained, so the docker CLI or daemon is stuck rather than blocked on output.";
            return TimedOutExitCode;
        }

        output = standardOutputTask.GetAwaiter().GetResult() + standardErrorTask.GetAwaiter().GetResult();
        return process.ExitCode;
    }

    /// <summary>
    /// How long any single <c>docker</c> invocation may take. The image build is
    /// the slow case and takes over a quarter of an hour on a warm machine, so
    /// the budget is deliberately several times that: the bound exists to catch a
    /// wedged daemon, and a bound a healthy cold run could trip would be worse
    /// than no bound at all.
    /// </summary>
    private static readonly TimeSpan DockerTimeout = TimeSpan.FromMinutes(45);

    /// <summary>
    /// The exit code reported for a terminated invocation. Non-zero so that
    /// <see cref="Docker"/> fails rather than reading the timeout as success, and
    /// distinct from any code docker returns itself.
    /// </summary>
    private const int TimedOutExitCode = -1;
}
