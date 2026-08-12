using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextHostBuilder"/>'s non-silo concerns: the
/// probe-path constants and <see cref="RepoContextHostBuilder.PrepareDataPaths"/>,
/// which proves the data paths are on a writable mount and applies the local
/// SQLite schema before the silo starts.
/// </summary>
[TestFixture]
public sealed class RepoContextHostBuilderTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
        => _root = Path.Combine(Path.GetTempPath(), "repocontext-host-" + Guid.NewGuid().ToString("N"));

    [TearDown]
    public void TearDown()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private RepoContextHostConfiguration LocalConfig()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [RepoContextHostConfiguration.DataRootKey] = _root,
            })
            .Build();
        return RepoContextHostConfiguration.FromConfiguration(configuration);
    }

    [Test]
    public void Probe_paths_are_distinct_and_health_rooted()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextHostBuilder.LivenessPath, Is.EqualTo("/health/live"));
            Assert.That(RepoContextHostBuilder.ReadinessPath, Is.EqualTo("/health/ready"));
            Assert.That(RepoContextHostBuilder.LivenessPath, Is.Not.EqualTo(RepoContextHostBuilder.ReadinessPath));
            Assert.That(RepoContextHostBuilder.LivenessTag, Is.Not.EqualTo(RepoContextHostBuilder.ReadinessTag));
        });

    [Test]
    public void PrepareDataPaths_creates_the_wal_directory_and_applies_the_sqlite_schema()
    {
        var config = LocalConfig();

        RepoContextHostBuilder.PrepareDataPaths(config);

        Assert.Multiple(() =>
        {
            Assert.That(Directory.Exists(config.WalDirectory), Is.True);
            Assert.That(File.Exists(config.SqlitePath), Is.True);
        });
    }

    [Test]
    public void PrepareDataPaths_rejects_a_null_config()
        => Assert.That(() => RepoContextHostBuilder.PrepareDataPaths(null!), Throws.ArgumentNullException);

    [Test]
    public void Build_rejects_a_null_args()
        => Assert.That(() => RepoContextHostBuilder.Build(null!), Throws.ArgumentNullException);
}
