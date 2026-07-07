using System.Diagnostics;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.State.Grpc.Tests.Security;

/// <summary>
/// Verifies that the two credential-generation helper scripts under <c>tools/</c>
/// and the server-side <see cref="LatticePasswordHash"/> all agree on the encoded
/// hash for the same salt, password, and iteration count. The cross-shell legs are
/// skipped (not failed) when the host lacks pwsh, bash, or openssl.
/// </summary>
[TestFixture]
public class CredentialScriptParityTests
{
    private const string DeterministicSaltB64 = "AQIDBAUGBwgJCgsMDQ4PEA==";
    private const string Password = "Password1";
    private const string ExpectedHash =
        "pbkdf2-sha256$210000$AQIDBAUGBwgJCgsMDQ4PEA==$Qc/KlSS3jQS+Upam+rUnCYWhq5v8/JBbmCDEdGfOX8k=";

    [Test]
    public void Bcl_encode_matches_documentedVector()
    {
        // This leg always runs: it pins the server hash to the same vector the
        // scripts target, so the contract is enforced even on a bare CI host.
        byte[] salt = Convert.FromBase64String(DeterministicSaltB64);
        Assert.That(LatticePasswordHash.Encode(Password, salt, 210_000), Is.EqualTo(ExpectedHash));
    }

    [Test]
    public void PowerShell_script_matches_documentedVector()
    {
        var script = FindToolsScript("New-LatticeStateCredential.ps1");
        var pwsh = FindExecutable("pwsh") ?? FindExecutable("powershell");
        if (pwsh is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        var output = RunScript(
            pwsh!,
            $"-NoProfile -File \"{script}\" -Username alice -PasswordEnv LATTICE_TEST_PW -Iterations 210000 -Format value");

        Assert.That(output, Is.EqualTo(ExpectedHash));
    }

    [Test]
    public void Bash_script_matches_documentedVector()
    {
        var script = FindToolsScript("new-lattice-state-credential.sh");
        var bash = FindBash();
        if (bash is null)
        {
            Assert.Ignore("bash is not available on this host.");
        }

        // Pass a POSIX-style path to bash so Git Bash resolves it.
        var posixScript = script.Replace('\\', '/');
        var output = RunScript(
            bash!,
            $"-lc \"bash '{posixScript}' --username alice --password-env LATTICE_TEST_PW --iterations 210000 --format value\"");

        Assert.That(output, Is.EqualTo(ExpectedHash));
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
        Assert.That(process, Is.Not.Null);
        var stdout = process!.StandardOutput.ReadToEnd();
        process.StandardError.ReadToEnd();
        if (!process.WaitForExit(30_000))
        {
            process.Kill(entireProcessTree: true);
            Assert.Fail("Credential script timed out.");
        }

        Assert.That(process.ExitCode, Is.EqualTo(0), "Credential script exited non-zero.");
        return stdout.Trim();
    }

    private static string FindToolsScript(string name)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "tools", name);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        Assert.Ignore($"Could not locate tools/{name} from the test base directory.");
        return string.Empty; // unreachable
    }

    private static string? FindBash()
    {
        var candidates = new[]
        {
            @"C:\Program Files\Git\bin\bash.exe",
            @"C:\Program Files\Git\usr\bin\bash.exe",
            "/bin/bash",
            "/usr/bin/bash",
        };
        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return FindExecutable("bash");
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
