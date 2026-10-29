using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The composite action and its pin file: <c>tools/tlc/toolchain.json</c> is
/// well formed and is the only place a pin lives, the action reads every pin
/// through <c>tools/tlc/toolchain.py</c>, builds the checkpoint overlay only on
/// a cache miss, and verifies the overlay's receipt unconditionally.
/// </summary>
public sealed partial class CiTlaToolchainProvisioningTests
{
    private const string ToolchainFile = "tools/tlc/toolchain.json";

    private const string ActionFile = ".github/actions/tla-toolchain/action.yml";

    private const string BuildCommand =
        "run: python3 tools/tlc/build.py --java-home \"$JAVA_HOME\" --jar tools/tla2tools.jar --output tools/tlc/build";

    private const string VerifyCommand =
        "run: python3 tools/tlc/toolchain.py verify --jar tools/tla2tools.jar --build tools/tlc/build";

    private const string CacheMiss = "if: steps.restore.outputs.cache-hit != 'true'";

    private static readonly TimeSpan ScriptTimeout = TimeSpan.FromMinutes(2);

    private static readonly Regex StepHeader = new(
        @"^    - name: (?<name>.+)$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private sealed record ToolchainPins(string JarUrl, string JarSha256, string JavaDistribution, string JavaVersion);

    [Test]
    public void Toolchain_json_is_the_single_well_formed_source_of_every_pin()
    {
        var path = Path.Combine(HygieneRepository.FindRepoRoot(), ToolchainFile);
        using var document = JsonDocument.Parse(File.ReadAllBytes(path));
        var names = document.RootElement.EnumerateObject().Select(property => property.Name).Order(StringComparer.Ordinal);
        var pins = LoadToolchainPins();

        Assert.Multiple(() =>
        {
            Assert.That(names, Is.EqualTo(new[] { "jarSha256", "jarUrl", "javaDistribution", "javaVersion" }),
                "contract C3 of #5148 is exactly these four keys");
            Assert.That(pins.JarUrl,
                Does.Match(@"^https://github\.com/tlaplus/tlaplus/releases/download/v[0-9][0-9A-Za-z.\-]*/tla2tools\.jar$"));
            Assert.That(pins.JarSha256, Does.Match("^[0-9a-f]{64}$"));
            Assert.That(pins.JavaDistribution, Does.Match(@"^[A-Za-z0-9][A-Za-z0-9._\-]*$"));
            Assert.That(pins.JavaVersion, Does.Match(@"^[A-Za-z0-9][A-Za-z0-9._\-]*$"));
        });
    }

    [Test]
    public void The_action_reads_every_pin_from_toolchain_json()
    {
        var action = ActionText();
        var pins = LoadToolchainPins();
        var setupJava = Step(action, "Setup Java");
        var fetch = Step(action, "Fetch TLA+ tools");

        Assert.Multiple(() =>
        {
            Assert.That(Step(action, "Read TLA+ toolchain pins"), Does.Contain("id: pins")
                .And.Contain("python3 tools/tlc/toolchain.py --self-test")
                .And.Contain("python3 tools/tlc/toolchain.py pins >> \"$GITHUB_OUTPUT\""));
            Assert.That(setupJava, Does.Contain("uses: actions/setup-java@")
                .And.Contain("distribution: ${{ steps.pins.outputs.java-distribution }}")
                .And.Contain("java-version: ${{ steps.pins.outputs.java-version }}"));
            Assert.That(fetch, Does.Contain("JAR_URL: ${{ steps.pins.outputs.jar-url }}")
                .And.Contain("JAR_SHA256: ${{ steps.pins.outputs.jar-sha256 }}")
                .And.Contain("\"$JAR_URL\"")
                .And.Contain("echo \"$JAR_SHA256  tools/tla2tools.jar\" | sha256sum --check --strict"));
            Assert.That(JarReleaseUrl.IsMatch(action), Is.False, "the action must not carry the jar URL itself");
            Assert.That(Digest.IsMatch(action), Is.False, "the action must not carry a digest itself");
            Assert.That(action, Does.Not.Contain(pins.JavaDistribution + "\n")
                .And.Not.Contain("java-version: '"), "the action must not carry the JDK pin itself");
        });
    }

    [Test]
    public void The_action_builds_the_overlay_only_on_a_cache_miss_and_always_verifies_its_receipt()
    {
        var action = ActionText();
        var order = StepHeader.Matches(action).Select(match => match.Groups["name"].Value.Trim()).ToList();
        string[] expected =
        [
            "Read TLA+ toolchain pins", "Setup Java", "Fetch TLA+ tools", "Compute checkpoint overlay cache key",
            "Restore checkpoint overlay", "Build and regression-check TLC checkpoint overlay",
            "Verify checkpoint overlay receipt", "Save checkpoint overlay",
        ];
        var key = Step(action, "Compute checkpoint overlay cache key");
        var restore = Step(action, "Restore checkpoint overlay");
        var build = Step(action, "Build and regression-check TLC checkpoint overlay");
        var verify = Step(action, "Verify checkpoint overlay receipt");
        var save = Step(action, "Save checkpoint overlay");

        Assert.Multiple(() =>
        {
            Assert.That(order.Where(expected.Contains), Is.EqualTo(expected), "step order");
            Assert.That(HasOverlayBuild(action), Is.True);
            Assert.That(key, Does.Contain("id: key").And.Contain("python3 tools/tlc/toolchain.py cache-key --java-home \"$JAVA_HOME\""));
            Assert.That(restore, Does.Contain("id: restore").And.Contain("uses: actions/cache/restore@")
                .And.Contain("path: tools/tlc/build").And.Contain("key: ${{ steps.key.outputs.key }}"));
            Assert.That(restore, Does.Not.Contain("restore-keys"), "a partial key match would restore another build");
            Assert.That(build, Does.Contain(CacheMiss).And.Contain(BuildCommand));
            Assert.That(verify, Does.Contain(VerifyCommand).And.Not.Contain("if:"),
                "the receipt is verified on every leg, restored or built, and a mismatch fails the leg");
            Assert.That(verify, Does.Not.Contain("continue-on-error"));
            Assert.That(save, Does.Contain(CacheMiss).And.Contain("uses: actions/cache/save@")
                .And.Contain("path: tools/tlc/build").And.Contain("key: ${{ steps.key.outputs.key }}"));
        });
    }

    [TestCase("--java-home \"$JAVA_HOME\"")]
    [TestCase("--jar tools/tla2tools.jar")]
    [TestCase("--output tools/tlc/build")]
    [TestCase("python3 tools/tlc/build.py")]
    [TestCase("tools/tlc/toolchain.py verify")]
    [TestCase("sha256sum --check --strict")]
    public void Overlay_provisioning_guard_rejects_missing_or_misdirected_build(string removed)
    {
        var action = ActionText();
        Assert.That(HasOverlayBuild(action), Is.True, "the positive control must be wired");
        Assert.That(HasOverlayBuild(action.Replace(removed, "", StringComparison.Ordinal)), Is.False);
    }

    [Test]
    public void The_toolchain_script_self_test_passes()
    {
        var (exitCode, stdout, stderr) = RunPython(["tools/tlc/toolchain.py", "--self-test"]);
        Assert.That(exitCode, Is.EqualTo(0), "tools/tlc/toolchain.py --self-test failed:\n" + stdout + stderr);
    }

    /// <summary>The jar is verified, then the overlay is built once, then its receipt is verified.</summary>
    private static bool HasOverlayBuild(string text)
    {
        var checkedJar = text.IndexOf("sha256sum --check --strict", StringComparison.Ordinal);
        var build = text.IndexOf(BuildCommand, StringComparison.Ordinal);
        var verify = text.IndexOf(VerifyCommand, StringComparison.Ordinal);
        return checkedJar >= 0 && build > checkedJar && verify > build
            && text.IndexOf(BuildCommand, build + BuildCommand.Length, StringComparison.Ordinal) < 0;
    }

    private static ToolchainPins LoadToolchainPins()
    {
        using var document = JsonDocument.Parse(
            File.ReadAllBytes(Path.Combine(HygieneRepository.FindRepoRoot(), ToolchainFile)));
        var root = document.RootElement;
        return new ToolchainPins(
            root.GetProperty("jarUrl").GetString()!,
            root.GetProperty("jarSha256").GetString()!,
            root.GetProperty("javaDistribution").GetString()!,
            root.GetProperty("javaVersion").GetString()!);
    }

    private static string ActionText() =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), ActionFile)).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>One step of the action, from its <c>- name:</c> line to the next step.</summary>
    private static string Step(string action, string name)
    {
        var headers = StepHeader.Matches(action);
        var index = headers.ToList().FindIndex(match => match.Groups["name"].Value.Trim() == name);
        Assert.That(index, Is.GreaterThanOrEqualTo(0), ActionFile + " has no step named '" + name + "'");
        var start = headers[index].Index;
        var end = index + 1 < headers.Count ? headers[index + 1].Index : action.Length;
        return action[start..end];
    }

    /// <summary>
    /// Runs a script under the first working Python interpreter. Missing Python
    /// is a broken pipeline under CI and a visible skip locally - never an
    /// inconclusive, which NUnit would not count.
    /// </summary>
    private static (int ExitCode, string StandardOutput, string StandardError) RunPython(IEnumerable<string> arguments)
    {
        string[] candidates = OperatingSystem.IsWindows() ? ["python", "py", "python3"] : ["python3", "python"];
        var interpreter = candidates.FirstOrDefault(candidate =>
        {
            try
            {
                var probe = new ProcessStartInfo(candidate) { ArgumentList = { "--version" } };
                var (exitCode, stdout, stderr) = Run(probe);
                return exitCode == 0 && (stdout + stderr).Contains("Python 3", StringComparison.Ordinal);
            }
            catch (System.ComponentModel.Win32Exception)
            {
                return false;
            }
        });

        if (interpreter is null)
        {
            if (string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Fail("no Python interpreter was found on the CI runner, so the toolchain script cannot be checked");
            }

            Assert.Ignore("no Python interpreter is available on this host");
        }

        var start = new ProcessStartInfo(interpreter!) { WorkingDirectory = HygieneRepository.FindRepoRoot() };
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        return Run(start);
    }

    /// <summary>Drains both pipes before waiting, and bounds the wait.</summary>
    private static (int ExitCode, string StandardOutput, string StandardError) Run(ProcessStartInfo start)
    {
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        start.UseShellExecute = false;
        using var process = Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit((int)ScriptTimeout.TotalMilliseconds))
        {
            process.Kill(entireProcessTree: true);
            Assert.Fail("'" + start.FileName + " " + string.Join(' ', start.ArgumentList) + "' did not exit within " + ScriptTimeout);
        }

        return (process.ExitCode, stdout.GetAwaiter().GetResult(), stderr.GetAwaiter().GetResult());
    }
}
