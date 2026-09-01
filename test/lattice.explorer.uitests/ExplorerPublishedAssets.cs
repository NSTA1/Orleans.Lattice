using System.Diagnostics;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Materialises the Explorer web head's static web assets into a portable, OS-neutral
/// content root by publishing this test project once, and returns the directory to host
/// from.
/// <para>
/// A plain <c>dotnet build</c> (what CI runs) does not copy the framework and RCL static
/// web assets into the output. It emits a runtime manifest whose <c>_framework</c>
/// content root is an <b>absolute, machine-local</b> NuGet cache path, so
/// <c>_framework/blazor.web.js</c> and the <c>_content/**</c> stylesheets resolve only on
/// the machine that built them. On a Linux CI runner that absolute path does not exist,
/// the framework asset 404s, the interactive Blazor Server circuit never connects, and
/// the shell never renders - producing bare locator timeouts with no server-side signal.
/// </para>
/// <para>
/// The SDK's publish pipeline, by contrast, writes every computed asset as a real file
/// under <c>wwwroot/</c> with a relative path and drops the absolute-path runtime
/// manifest, leaving only the endpoint manifest the head's <c>MapStaticAssets</c>
/// resolves against the physical <c>wwwroot</c>. Publishing once at run start therefore
/// yields a content root that serves identically on every operating system, and exercises
/// the exact asset layout a shipped consumer serves. The published output goes to a fresh
/// temp directory that the host deletes on disposal.
/// </para>
/// </summary>
internal static class ExplorerPublishedAssets
{
    /// <summary>
    /// How long the publish is allowed to take before the fixture gives up.
    /// <para>
    /// The publish is fast (single-digit seconds; see <see cref="RunPublishAsync"/>), so
    /// this bound is pure headroom for a loaded agent. It exists because an
    /// <b>unbounded</b> wait here is the worst possible failure mode: this runs in
    /// <c>[OneTimeSetUp]</c>, before any test reports, so a stall produces no console
    /// output at all and <c>dotnet test --blame-hang</c> eventually aborts the whole
    /// test run with "Test host process crashed" and no test to attribute it to. Failing
    /// fast with the publish output in the message turns that into a readable error.
    /// </para>
    /// </summary>
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Publishes this test project's static web assets into a fresh temp directory and
    /// returns that directory. Uses <c>--no-build</c> so it reuses the assemblies the
    /// build step already produced (CI builds before it tests), keeping the step to a
    /// few seconds.
    /// </summary>
    /// <returns>The absolute path of the published content root to host from.</returns>
    public static async Task<string> EnsureAsync()
    {
        var projectPath = ResolveProjectPath();
        var outputDir = Path.Combine(
            Path.GetTempPath(),
            "lattice-explorer-uitests-" + Guid.NewGuid().ToString("N"));

        await RunPublishAsync(projectPath, outputDir);

        var frameworkScript = Path.Combine(outputDir, "wwwroot", "_framework", "blazor.web.js");
        if (!File.Exists(frameworkScript))
        {
            throw new InvalidOperationException(
                "Publishing the Explorer UI-test host did not materialise "
                + $"'{frameworkScript}'. Without the Blazor Web framework bootstrap script the "
                + "interactive server circuit cannot connect and the shell will not render.");
        }

        return outputDir;
    }

    private static string ResolveProjectPath()
    {
        // Walk up from the test binary's directory to the project file. The build output
        // lives at <project>/bin/<config>/<tfm>/, so the csproj is three levels up; fall
        // back to a search to stay robust to layout changes.
        var candidate = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "Orleans.Lattice.Explorer.UiTests.csproj"));
        if (File.Exists(candidate))
        {
            return candidate;
        }

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var found = dir.GetFiles("Orleans.Lattice.Explorer.UiTests.csproj");
            if (found.Length > 0)
            {
                return found[0].FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException(
            "Could not locate Orleans.Lattice.Explorer.UiTests.csproj from "
            + $"'{AppContext.BaseDirectory}'. The UI-test host publishes this project to build a "
            + "portable static-asset content root and needs its path.");
    }

    private static async Task RunPublishAsync(string projectPath, string outputDir)
    {
        var configuration = ResolveConfiguration();
        var startInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add("publish");
        startInfo.ArgumentList.Add(projectPath);
        startInfo.ArgumentList.Add("-c");
        startInfo.ArgumentList.Add(configuration);
        startInfo.ArgumentList.Add("--no-build");
        startInfo.ArgumentList.Add("-o");
        startInfo.ArgumentList.Add(outputDir);

        // Run MSBuild single-proc, with node reuse off, so this publish neither spawns
        // worker nodes nor depends on finding warm ones.
        //
        // This is load-bearing, and it is what made the coverage lane fail. MSBuild
        // keeps its worker nodes alive for reuse for fifteen minutes after a build. The
        // coverage workflow builds the whole solution and then runs every test project
        // in one job, so this suite starts either side of that fifteen-minute mark
        // depending only on how long the earlier projects took. Inside the window the
        // publish reused the existing nodes and finished in seconds. Outside it, the
        // publish had to spawn a fresh worker node per core - measured at thirty
        // processes, sixty-seven seconds, on an unloaded developer machine - from inside
        // the vstest host, alongside coverlet instrumentation and a Chromium launch on a
        // four-core agent. That tipped over and never returned, and because it happens in
        // [OneTimeSetUp] the run produced no output for ten minutes until --blame-hang
        // aborted it. Twenty-five consecutive coverage runs split on that boundary with
        // no overlap: every run under fifteen minutes passed, every run over it hung.
        //
        // Single-proc costs nothing here - there is one project and --no-build means
        // there is nothing to compile in parallel - and it is strictly faster in the cold
        // case (measured at ten seconds, zero extra processes). Do not remove it to "let
        // the publish parallelise"; parallelism is what broke it.
        startInfo.ArgumentList.Add("-maxcpucount:1");
        startInfo.Environment["MSBUILDDISABLENODEREUSE"] = "1";

        using var process = new Process { StartInfo = startInfo };
        process.Start();

        using var timeout = new CancellationTokenSource(PublishTimeout);

        string stdout;
        string stderr;
        try
        {
            var stdoutTask = process.StandardOutput.ReadToEndAsync(timeout.Token);
            var stderrTask = process.StandardError.ReadToEndAsync(timeout.Token);
            await process.WaitForExitAsync(timeout.Token);
            stdout = await stdoutTask;
            stderr = await stderrTask;
        }
        catch (OperationCanceledException)
        {
            TryKill(process);
            throw new InvalidOperationException(
                $"'dotnet publish' of the Explorer UI-test host did not complete within "
                + $"{PublishTimeout.TotalMinutes:0} minutes and was terminated. The UI suite "
                + "publishes it to obtain a portable static-asset content root (see "
                + "ExplorerPublishedAssets). This runs before any test, so an unbounded wait "
                + "would abort the whole test run with no attributable failure.");
        }

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"'dotnet publish' of the Explorer UI-test host failed with exit code "
                + $"{process.ExitCode}. The UI suite publishes it to obtain a portable "
                + "static-asset content root (see ExplorerPublishedAssets). Output:"
                + Environment.NewLine + stdout + Environment.NewLine + stderr);
        }
    }

    private static void TryKill(Process process)
    {
        try
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
        catch (InvalidOperationException)
        {
            // The process exited between the check and the kill; nothing to do.
        }
        catch (NotSupportedException)
        {
        }
    }

    private static string ResolveConfiguration()
    {
        // Infer the build configuration from the output path so the --no-build publish
        // reuses the assemblies the current run was built with.
        var normalized = AppContext.BaseDirectory.Replace('\\', '/');
        return normalized.Contains("/Debug/", StringComparison.OrdinalIgnoreCase)
            ? "Debug"
            : "Release";
    }
}
