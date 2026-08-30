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

        using var process = new Process { StartInfo = startInfo };
        process.Start();

        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        var stdout = await stdoutTask;
        var stderr = await stderrTask;

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"'dotnet publish' of the Explorer UI-test host failed with exit code "
                + $"{process.ExitCode}. The UI suite publishes it to obtain a portable "
                + "static-asset content root (see ExplorerPublishedAssets). Output:"
                + Environment.NewLine + stdout + Environment.NewLine + stderr);
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
