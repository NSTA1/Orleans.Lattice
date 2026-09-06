using Microsoft.AspNetCore.Builder;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Coverage for the ambient-configuration entry point,
/// <see cref="RepoContextHostBuilder.Build(string[])"/> - the exact overload
/// <c>Program.cs</c> calls, and the only one that reads the container's real
/// environment rather than an injected configuration object.
///
/// Everything else in the suite drives the <c>(builder, config)</c> overload with
/// an explicit configuration, so a regression in the environment-reading path
/// (a mis-named key, or validation not running before wiring) would slip through
/// unnoticed and only surface as a container that will not start.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c> and <c>NonParallelizable</c>: it mutates process
/// environment variables and constructs a fully wired host, so it must not run
/// beside anything else reading the environment. The host is built but never
/// run, so no port is bound.
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class RepoContextHostBuilderAmbientConfigurationTests
{
    private static readonly string[] EnvironmentKeys =
    [
        RepoContextHostConfiguration.DataRootKey,
        RepoContextHostConfiguration.ClusterIdKey,
        RepoContextHostConfiguration.ServiceIdKey,
        RepoContextHostConfiguration.WorkspaceRootKey,
    ];

    private string _dataRoot = null!;
    private readonly Dictionary<string, string?> _saved = [];

    [SetUp]
    public void SetUp()
    {
        foreach (var key in EnvironmentKeys)
        {
            _saved[key] = Environment.GetEnvironmentVariable(key);
        }

        _dataRoot = Path.Combine(Path.GetTempPath(), "repocontext-ambient-" + Guid.NewGuid().ToString("N"));
        Environment.SetEnvironmentVariable(RepoContextHostConfiguration.DataRootKey, _dataRoot);
        Environment.SetEnvironmentVariable(RepoContextHostConfiguration.ClusterIdKey, "repocontext-ambient");
        Environment.SetEnvironmentVariable(RepoContextHostConfiguration.ServiceIdKey, "repocontext-ambient");
        Environment.SetEnvironmentVariable(RepoContextHostConfiguration.WorkspaceRootKey, _dataRoot);
    }

    [TearDown]
    public void TearDown()
    {
        foreach (var (key, value) in _saved)
        {
            Environment.SetEnvironmentVariable(key, value);
        }

        _saved.Clear();
        SqliteConnection.ClearAllPools();
        if (!Directory.Exists(_dataRoot))
        {
            return;
        }

        try
        {
            Directory.Delete(_dataRoot, recursive: true);
        }
        catch (IOException)
        {
            // A background flush may briefly hold a file; cleanup is best-effort.
        }
    }

    [Test]
    public async Task Build_from_ambient_configuration_wires_a_runnable_host()
    {
        var app = RepoContextHostBuilder.Build([]);

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(app, Is.Not.Null);
                Assert.That(
                    app.Services.GetService<RepoContextReadinessState>(),
                    Is.Not.Null,
                    "The readiness gate must be wired, otherwise the container's probes have nothing to read.");
            });

            // The environment-driven path must apply the same data-root contract
            // as the explicit-configuration path: durable state lands on the mount.
            Assert.That(Directory.Exists(_dataRoot), Is.True);
        }
        finally
        {
            await app.DisposeAsync();
        }
    }

    /// <summary>
    /// Asserts the claim-lease ceiling is actually wired into the built host, by
    /// reading it back off the resolved <see cref="LatticeOptions"/> rather than
    /// off the extension method that sets it.
    /// </summary>
    /// <remarks>
    /// This exists because every other test of the ceiling calls
    /// <c>ConfigureRepoContextClaimLeases</c> directly, so all of them would keep
    /// passing if the single call site in <see cref="RepoContextHostBuilder"/>
    /// were deleted - the option would simply revert to the library's five-minute
    /// maximum and backlog claims would silently start lapsing mid-build again.
    /// Resolving the option from the host is the only assertion that fails when
    /// the wiring is removed rather than when the resolver is.
    /// <para>
    /// It reads the <b>unnamed</b> options instance deliberately:
    /// <c>LatticeLockGrain.ResolveLeaseTicks</c> uses
    /// <c>IOptionsMonitor&lt;LatticeOptions&gt;.CurrentValue</c>, not
    /// <c>Get(treeName)</c>, so a per-tree configuration would not reach the lock
    /// grain and this test would pass while the deployment stayed broken.
    /// </para>
    /// </remarks>
    [Test]
    public async Task Build_from_ambient_configuration_raises_the_claim_lease_ceiling()
    {
        var app = RepoContextHostBuilder.Build([]);

        try
        {
            var options = app.Services.GetRequiredService<IOptionsMonitor<LatticeOptions>>().CurrentValue;

            Assert.Multiple(() =>
            {
                Assert.That(
                    options.MaxLockLeaseDuration,
                    Is.EqualTo(TimeSpan.FromSeconds(RepoContextClaimLeases.DefaultMaxLockLeaseSeconds)),
                    "The host must raise the lease ceiling; without it a backlog claim cannot outlive a build.");
                Assert.That(
                    options.MaxLockLeaseDuration,
                    Is.GreaterThan(LatticeOptions.MaxLockLeaseDurationValue),
                    "The wiring is only meaningful if it exceeds the library ceiling it exists to raise.");
            });
        }
        finally
        {
            await app.DisposeAsync();
        }
    }

    [Test]
    public void Build_rejects_null_arguments()
        => Assert.Throws<ArgumentNullException>(() => RepoContextHostBuilder.Build(null!));
}
