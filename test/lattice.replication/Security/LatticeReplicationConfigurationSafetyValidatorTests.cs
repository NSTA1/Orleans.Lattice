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

        await v.StartAsync(CancellationToken.None);
        Assert.Pass();
    }

    [Test]
    public async Task StartAsync_is_noop_when_scan_disabled_via_options()
    {
        var cfg = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["LatticeReplication:Secret"] = "leaked",
            })
            .Build();

        var v = new LatticeReplicationConfigurationSafetyValidator(
            ProviderWith(cfg),
            OptionsFor(new LatticeReplicationSecurityOptions { ScanConfigurationForSecrets = false }),
            NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

        await v.StartAsync(CancellationToken.None);
        Assert.Pass();
    }

    [Test]
    public async Task StartAsync_is_noop_when_escape_hatch_env_var_set()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets, "1");
        var cfg = WriteTempAppSettingsAndBuild(out var tempDir, out var path);
        try
        {
            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            await v.StartAsync(CancellationToken.None);
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
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
        var cfg = WriteTempAppSettingsAndBuild(out var tempDir, out var path);
        try
        {
            var v = new LatticeReplicationConfigurationSafetyValidator(
                ProviderWith(cfg),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationConfigurationSafetyValidator>.Instance);

            await v.StartAsync(CancellationToken.None);
            Assert.Pass();
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }

    [TestCase("0")]
    [TestCase("false")]
    [TestCase("no")]
    [TestCase("")]
    public void StartAsync_still_throws_when_escape_hatch_env_var_is_not_truthy(string value)
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets, value);
        var cfg = WriteTempAppSettingsAndBuild(out var tempDir, out var path);
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
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
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

        await v.StartAsync(CancellationToken.None);
        Assert.Pass();
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

        await v.StartAsync(CancellationToken.None);
        Assert.Pass();
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

        await v.StopAsync(CancellationToken.None);
        Assert.Pass();
    }

    private static IConfiguration WriteTempAppSettingsAndBuild(out string tempDir, out string path)
    {
        tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        path = Path.Combine(tempDir, "appsettings.json");
        File.WriteAllText(path, """{"LatticeReplication":{"Secret":"leaked"}}""");

        return new ConfigurationBuilder()
            .SetBasePath(tempDir)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
            .Build();
    }
}
