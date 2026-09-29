using System.Diagnostics;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.State.Grpc.Tests.Security;

/// <summary>
/// Verifies that the credential-generation helper script
/// <c>tools/New-LatticeStateCredential.ps1</c> and the server-side
/// <see cref="LatticePasswordHash"/> agree on the encoded hash for the same salt,
/// password, and iteration count, and that the script is the repository's only
/// credential helper. The script leg is skipped (not failed) when the host lacks
/// pwsh.
/// </summary>
[TestFixture]
public class CredentialScriptParityTests
{
    private const string ScriptName = "New-LatticeStateCredential.ps1";
    private const string DeterministicSaltB64 = "AQIDBAUGBwgJCgsMDQ4PEA==";
    private const string Password = "Password1";
    private const string ExpectedHash =
        "pbkdf2-sha256$210000$AQIDBAUGBwgJCgsMDQ4PEA==$Qc/KlSS3jQS+Upam+rUnCYWhq5v8/JBbmCDEdGfOX8k=";

    [Test]
    public void Bcl_encode_matches_documentedVector()
    {
        // This leg always runs: it pins the server hash to the same vector the
        // script targets, so the contract is enforced even on a bare CI host.
        byte[] salt = Convert.FromBase64String(DeterministicSaltB64);
        Assert.That(LatticePasswordHash.Encode(Password, salt, 210_000), Is.EqualTo(ExpectedHash));
    }

    [Test]
    public void PowerShell_script_matches_documentedVector()
    {
        var script = Path.Combine(FindToolsDirectory(), ScriptName);
        Assert.That(File.Exists(script), Is.True, $"tools/{ScriptName} is missing.");

        // The script requires PowerShell 7.2+, so Windows PowerShell 5.1 is not a fallback.
        var pwsh = FindExecutable("pwsh");
        if (pwsh is null)
        {
            Assert.Ignore("pwsh is not available on this host.");
        }

        var output = RunScript(
            pwsh!,
            $"-NoProfile -File \"{script}\" -Username alice -PasswordEnv LATTICE_TEST_PW -Iterations 210000 -Format value");

        Assert.That(output, Is.EqualTo(ExpectedHash));
    }

    [Test]
    public void PowerShell_script_is_the_only_credential_helper()
    {
        var helpers = Directory
            .EnumerateFiles(FindToolsDirectory())
            .Select(Path.GetFileName)
            .Where(name => name!.Contains("credential", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.That(helpers, Is.EqualTo(new[] { ScriptName }));
    }

    private static string RunScript(string fileName, string arguments)
    {
        var psi = new ProcessStartInfo(fileName, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        psi.Environment["LATTICE_CRED_SALT_B64"] = DeterministicSaltB64;
        psi.Environment["LATTICE_TEST_PW"] = Password;

        using var process = Process.Start(psi);
        Assert.That(process, Is.Not.Null, $"Failed to start '{fileName}'.");

        // Both pipes must be drained concurrently. Reading one to EOF while the
        // other is unread deadlocks the moment the child fills the unread pipe's
        // buffer - and it deadlocks INSIDE the read, before WaitForExit is ever
        // reached, so the timeout below cannot bound it and the run hangs until
        // CI's blame-hang timer fires. A script that fails verbosely (a PowerShell
        // error record is written to stderr) is exactly the case that reaches it.
        var stdoutTask = process!.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();

        if (!process.WaitForExit(30_000))
        {
            process.Kill(entireProcessTree: true);
            Assert.Fail("Credential script timed out after 30s.");
        }

        var stdout = stdoutTask.GetAwaiter().GetResult();
        var stderr = stderrTask.GetAwaiter().GetResult();

        // stderr was previously read and discarded, so a non-zero exit named no
        // cause. The script's own error record is the whole diagnosis.
        Assert.That(process.ExitCode, Is.EqualTo(0),
            $"Credential script exited {process.ExitCode}."
            + Environment.NewLine + "stderr: " + stderr
            + Environment.NewLine + "stdout: " + stdout);

        return stdout.Trim();
    }

    private static string FindToolsDirectory()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "tools");
            if (File.Exists(Path.Combine(candidate, "Invoke-RepositoryWideGates.ps1")))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        Assert.Ignore("Could not locate the repository tools/ directory from the test base directory.");
        return string.Empty; // unreachable
    }

    private static string? FindExecutable(string name)
    {
        var pathVar = Environment.GetEnvironmentVariable("PATH");
        if (pathVar is null)
        {
            return null;
        }

        var exts = OperatingSystem.IsWindows() ? new[] { ".exe", ".cmd", ".bat", string.Empty } : new[] { string.Empty };
        foreach (var dir in pathVar.Split(Path.PathSeparator))
        {
            if (string.IsNullOrWhiteSpace(dir))
            {
                continue;
            }

            foreach (var ext in exts)
            {
                var candidate = Path.Combine(dir, name + ext);
                if (File.Exists(candidate))
                {
                    return candidate;
                }
            }
        }

        return null;
    }
}
