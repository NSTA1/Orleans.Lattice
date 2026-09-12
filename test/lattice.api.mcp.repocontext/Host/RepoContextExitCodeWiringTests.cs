using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Asserts that the real host wires the process-exit-code reporter into its drain
/// signal (issue #2401), which is the one link the unit tests cannot cover.
/// </summary>
/// <remarks>
/// <para>
/// The reporter defaults to absent, deliberately, so that no fixture driving an
/// overrun can poison the NUnit host's own exit code. That default is only safe if
/// the composition root supplies the real reporter, and forgetting to would restore
/// the defect in full while every unit test kept passing - they inject their own
/// reporter and would never notice.
/// </para>
/// <para>
/// The assertion is on whether a reporter was supplied, not on which one, because
/// triggering a real overrun against a real host would assign the test process's own
/// exit code. The reporter's behaviour is pinned separately, directly, in
/// <see cref="RepoContextExitCodeTests"/>.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class RepoContextExitCodeWiringTests
{
    private string _dataRoot = null!;

    [SetUp]
    public void SetUp()
        => _dataRoot = Path.Combine(Path.GetTempPath(), "repocontext-exitcode-" + Guid.NewGuid().ToString("N"));

    [TearDown]
    public void TearDown()
    {
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
    public void The_host_supplies_the_process_exit_code_reporter_to_the_drain_signal()
    {
        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _dataRoot,
                    [RepoContextHostConfiguration.ClusterIdKey] = "repocontext-exitcode",
                    [RepoContextHostConfiguration.ServiceIdKey] = "repocontext-exitcode",
                })
                .Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();

        var app = RepoContextHostBuilder.Build(builder, config);

        try
        {
            var signal = app.Services.GetRequiredService<RepoContextDrainSignal>();

            Assert.That(
                signal.ReportsProcessExitCode,
                Is.True,
                "the host must supply RepoContextExitCode.SetProcessExitCode, otherwise an abandoned drain is "
                + "detected, latched and logged and still exits reporting success");
        }
        finally
        {
            app.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
