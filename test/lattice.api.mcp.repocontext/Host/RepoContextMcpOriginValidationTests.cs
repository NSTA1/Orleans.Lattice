using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Regression guard for the container's DNS-rebinding exposure. The MCP listener
/// binds every interface with authentication off and the write-enabled tools on,
/// so a browser page that rebinds its hostname to the container could otherwise
/// drive tree deletion or purge. These tests assert the host opts into the
/// binding's Origin validation, keeps loopback origins allowed for a local
/// inspector, and lists no foreign origin.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Measured 2.8 s for the whole fixture run locally: builds the host's service provider in-process and never starts the silo, so there is no storage and no cluster.")]
public sealed class RepoContextMcpOriginValidationTests
{
    private string _root = null!;
    private WebApplication _app = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-origin-" + Guid.NewGuid().ToString("N"));
        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _root,
                })
                .Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        _app = RepoContextHostBuilder.Build(builder, config);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _app?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (Directory.Exists(_root))
        {
            try
            {
                Directory.Delete(_root, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup; a background handle may briefly hold a file.
            }
        }
    }

    [Test]
    public void The_host_refuses_foreign_browser_origins_and_allows_only_loopback()
    {
        var options = _app.Services.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.RequireAllowedOrigin, Is.True,
                "An unauthenticated, all-interfaces, write-enabled MCP listener must validate Origin.");
            Assert.That(options.AllowLoopbackOrigins, Is.True,
                "A local inspector must keep working.");
            Assert.That(options.AllowedOrigins, Is.Empty,
                "No non-loopback origin is trusted on this single-user box.");
        });
    }
}
