using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class LatticeReplicationConfigurationSafetyValidatorTests
{
    private string? _savedAllow;

    [SetUp]
    public void SetUp()
    {
        _savedAllow = Environment.GetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets);
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets, null);
    }

    [TearDown]
    public void TearDown()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets, _savedAllow);
    }

    private static IOptionsMonitor<LatticeReplicationSecurityOptions> OptionsFor(LatticeReplicationSecurityOptions o)
    {
        var m = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        m.CurrentValue.Returns(o);
        return m;
    }

    private static IServiceProvider ProviderWith(IConfiguration? cfg)
    {
        var sp = Substitute.For<IServiceProvider>();
        sp.GetService(typeof(IConfiguration)).Returns(cfg);
        return sp;
    }

    [Test]
    public async Task StartAsync_is_noop_when_no_configuration_in_di()
    {
        var v = new LatticeReplicationConfigurationSafetyValidator(
            ProviderWith(null),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

        var start = v.StartAsync(CancellationToken.None);

        // The claim is about cost, not outcome: with no IConfiguration in DI the
        // validator short-circuits and never reaches the provider walk, so it
        // completes synchronously rather than merely "not throwing".
        Assert.That(start.IsCompletedSuccessfully, Is.True,
            "a validator with no configuration to scan must short-circuit synchronously");
        await start;
    }

    [Test]
    public async Task StartAsync_is_noop_when_scan_disabled_via_options()
    {
        // Use the same file-backed configuration that
        // StartAsync_still_throws_when_escape_hatch_env_var_is_not_truthy proves
        // does trip the scan. An in-memory provider has no file path and so can
        // never trip it, which would leave this test green even if the option
        // were ignored entirely.
        var cfg = WriteScannedAppSettingsAndBuild(out var settingsDir);
        try
        {
            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions { ScanConfigurationForSecrets = false }),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            var start = v.StartAsync(CancellationToken.None);

            Assert.That(start.IsCompletedSuccessfully, Is.True,
                "ScanConfigurationForSecrets=false must suppress the scan that this configuration otherwise trips");
            await start;
        }
        finally
        {
            DeleteDirectory(settingsDir);
        }
    }

    [Test]
    public async Task StartAsync_is_noop_when_escape_hatch_env_var_set()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets, "1");
        var cfg = WriteScannedAppSettingsAndBuild(out var settingsDir);
        try
        {
            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            var start = v.StartAsync(CancellationToken.None);

            // Previously this test had no assertion at all: it could only fail by
            // throwing. The escape hatch short-circuits before the provider walk,
            // so synchronous completion is the falsifiable form of "no-op".
            Assert.That(start.IsCompletedSuccessfully, Is.True,
                "the escape hatch must suppress the scan that this configuration otherwise trips");
            await start;
        }
        finally
        {
            DeleteDirectory(settingsDir);
        }
    }

    [TestCase("true")]
    [TestCase("TRUE")]
    [TestCase("True")]
    [TestCase("yes")]
    [TestCase("YES")]
    [TestCase("  1  ")]
    public async Task StartAsync_is_noop_when_escape_hatch_env_var_uses_truthy_casing(string value)
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets, value);
        var cfg = WriteScannedAppSettingsAndBuild(out var settingsDir);
        try
        {
            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            var start = v.StartAsync(CancellationToken.None);

            Assert.That(start.IsCompletedSuccessfully, Is.True,
                $"'{value}' must be read as truthy and suppress the scan this configuration otherwise trips");
            await start;
        }
        finally
        {
            DeleteDirectory(settingsDir);
        }
    }

    [TestCase("0")]
    [TestCase("false")]
    [TestCase("no")]
    [TestCase("")]
    public void StartAsync_still_throws_when_escape_hatch_env_var_is_not_truthy(string value)
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets, value);
        var cfg = WriteScannedAppSettingsAndBuild(out var settingsDir);
        try
        {
            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            Assert.That(
                async () => await v.StartAsync(CancellationToken.None),
                Throws.InvalidOperationException);
        }
        finally
        {
            DeleteDirectory(settingsDir);
        }
    }

    [Test]
    public async Task StartAsync_passes_when_nested_secret_sourced_from_in_memory_provider()
    {
        var cfg = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["LatticeReplication:Secrets:Secret"] = "alpha",
                ["LatticeReplication:Secrets:AcceptedSecrets:0"] = "beta",
                ["LatticeReplication:Secrets:PeerSecrets:site-b"] = "gamma",
            })
            .Build();

        var v = new LatticeReplicationConfigurationSafetyValidator(
            ProviderWith(cfg),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

        var start = v.StartAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Guard that the scan genuinely had secret-shaped keys to find, so the
            // pass is caused by the provider having no file path rather than by
            // there being nothing to detect.
            Assert.That(cfg["LatticeReplication:Secrets:Secret"], Is.EqualTo("alpha"));
            Assert.That(cfg["LatticeReplication:Secrets:PeerSecrets:site-b"], Is.EqualTo("gamma"));
            Assert.That(start.IsCompletedSuccessfully, Is.True,
                "a non-file provider must clear the scan rather than trip it");
        });
        await start;
    }

    [Test]
    public async Task StartAsync_passes_when_secret_sourced_from_in_memory_provider()
    {
        // In-memory providers have no file path; the scan does not trip.
        var cfg = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["LatticeReplication:Secret"] = "alpha",
            })
            .Build();

        var v = new LatticeReplicationConfigurationSafetyValidator(
            ProviderWith(cfg),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

        var start = v.StartAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(cfg["LatticeReplication:Secret"], Is.EqualTo("alpha"),
                "the scan must have a secret-shaped key to find for this test to mean anything");
            Assert.That(start.IsCompletedSuccessfully, Is.True,
                "a non-file provider must clear the scan rather than trip it");
        });
        await start;
    }

    [Test]
    public void StartAsync_throws_when_secret_sourced_from_file_under_app_directory()
    {
        // The validator's app-directory anchor is AppContext.BaseDirectory; place
        // the appsettings file alongside the test assembly so the scan trips.
        var path = Path.Combine(AppContext.BaseDirectory, $"appsettings.test-leaked-{Guid.NewGuid():N}.json");
        File.WriteAllText(path, """{"LatticeReplication":{"Secret":"leaked"}}""");

        try
        {
            var cfg = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile(Path.GetFileName(path), optional: false, reloadOnChange: false)
                .Build();

            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            Assert.That(
                async () => await v.StartAsync(CancellationToken.None),
                Throws.InvalidOperationException);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void StartAsync_throws_when_nested_secret_sourced_from_file_under_app_directory()
    {
        // The ConfigurationBindingSecretSource shape uses LatticeReplication:Secrets:Secret.
        var path = Path.Combine(AppContext.BaseDirectory, $"appsettings.test-leaked-nested-{Guid.NewGuid():N}.json");
        File.WriteAllText(path, """{"LatticeReplication":{"Secrets":{"Secret":"leaked"}}}""");

        try
        {
            var cfg = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile(Path.GetFileName(path), optional: false, reloadOnChange: false)
                .Build();

            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            Assert.That(
                async () => await v.StartAsync(CancellationToken.None),
                Throws.InvalidOperationException);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void StartAsync_throws_when_nested_accepted_secrets_sourced_from_file_under_app_directory()
    {
        var path = Path.Combine(AppContext.BaseDirectory, $"appsettings.test-leaked-accepted-{Guid.NewGuid():N}.json");
        File.WriteAllText(path, """{"LatticeReplication":{"Secrets":{"AcceptedSecrets":["alpha","beta"]}}}""");

        try
        {
            var cfg = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile(Path.GetFileName(path), optional: false, reloadOnChange: false)
                .Build();

            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            Assert.That(
                async () => await v.StartAsync(CancellationToken.None),
                Throws.InvalidOperationException);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void StartAsync_throws_when_per_peer_secret_sourced_from_file_under_app_directory()
    {
        var path = Path.Combine(AppContext.BaseDirectory, $"appsettings.test-leaked-peer-{Guid.NewGuid():N}.json");
        File.WriteAllText(path, """{"LatticeReplication":{"Secrets":{"PeerSecrets":{"site-b":"leaked"}}}}""");

        try
        {
            var cfg = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile(Path.GetFileName(path), optional: false, reloadOnChange: false)
                .Build();

            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            Assert.That(
                async () => await v.StartAsync(CancellationToken.None),
                Throws.InvalidOperationException);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public async Task StopAsync_is_noop()
    {
        var v = new LatticeReplicationConfigurationSafetyValidator(
            ProviderWith(null),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

        var stop = v.StopAsync(CancellationToken.None);

        Assert.That(stop.IsCompletedSuccessfully, Is.True,
            "the validator is startup-only and holds nothing to unwind on stop");
        await stop;
    }

    /// <summary>
    /// Writes an <c>appsettings.json</c> carrying a leaked secret and binds it
    /// through a json file provider whose recorded source path is absolute and
    /// under <see cref="AppContext.BaseDirectory"/>, which is what the scan
    /// actually keys on.
    /// </summary>
    /// <remarks>
    /// The file deliberately lives under the application directory rather than
    /// the system temp directory. A relative <c>AddJsonFile</c> name is recorded
    /// verbatim as the provider's source path and is later resolved against the
    /// process working directory, so a temp-directory file only tripped the scan
    /// while the test runner happened to set the working directory to the test
    /// output folder. Anchoring the file under the application directory and
    /// passing an absolute path makes the trip deterministic and independent of
    /// the runner's working directory.
    /// </remarks>
    private static IConfiguration WriteScannedAppSettingsAndBuild(out string settingsDir)
    {
        settingsDir = Path.Combine(AppContext.BaseDirectory, $"cfgscan-{Guid.NewGuid():N}");
        Directory.CreateDirectory(settingsDir);
        var path = Path.Combine(settingsDir, "appsettings.json");
        File.WriteAllText(path, """{"LatticeReplication":{"Secret":"leaked"}}""");

        return new ConfigurationBuilder()
            .AddJsonFile(path, optional: false, reloadOnChange: false)
            .Build();
    }

    private static void DeleteDirectory(string directory)
    {
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
