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
/// Pins the one assumption <see cref="RepoContextActivationCensusTests"/> cannot
/// possibly test: that Orleans still publishes an instrument named
/// <see cref="RepoContextActivationCensus.InstrumentName"/>.
/// </summary>
/// <remarks>
/// <para>
/// The census is selected by instrument name, and <b>that name belongs to Orleans</b>.
/// A fixture that supplies its own probe instrument proves the listening behaviour is
/// correct and proves nothing at all about whether the real name still exists: an
/// Orleans upgrade that renamed or withdrew the instrument would leave every unit test
/// green and the container silently unable to read its own resident set. Because the
/// consumers of that reading all treat "unavailable" as an acceptable answer - and
/// they must, or a lost diagnostic would become a failed shutdown - the loss would be
/// entirely silent in production too. This fixture is the only thing standing between
/// that upgrade and a forecast that quietly reports nothing forever.
/// </para>
/// <para>
/// It is marked <c>Integration</c> because it stands up a real silo, and it must be:
/// the instrument is published by the Orleans catalog, which does not exist until a
/// silo does.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class RepoContextActivationCensusInstrumentNameTests
{
    private string _dataRoot = null!;

    [SetUp]
    public void SetUp()
        => _dataRoot = Path.Combine(Path.GetTempPath(), "repocontext-census-" + Guid.NewGuid().ToString("N"));

    [TearDown]
    public void TearDown()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(_dataRoot))
        {
            try
            {
                Directory.Delete(_dataRoot, recursive: true);
            }
            catch (IOException)
            {
                // A background flush may briefly hold the WAL file; best-effort cleanup.
            }
        }
    }

    [Test]
    public async Task Orleans_still_publishes_the_activation_working_set_instrument_the_census_reads()
    {
        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _dataRoot,
                    [RepoContextHostConfiguration.ClusterIdKey] = "repocontext-census",
                    [RepoContextHostConfiguration.ServiceIdKey] = "repocontext-census",
                })
                .Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        await using var app = RepoContextHostBuilder.Build(builder, config);

        await app.StartAsync(TestContext.CurrentContext.CancellationToken);
        try
        {
            // The census the host itself wired, not a fresh one: this asserts the
            // composed object reads the composed silo, which is the claim that
            // matters. A locally constructed census would test the same listener
            // against the same silo and would still miss a wiring regression.
            var census = app.Services.GetRequiredService<RepoContextActivationCensus>();

            Assert.That(
                census.TrySample(),
                Is.Not.Null,
                $"Orleans no longer publishes an instrument named "
                + $"'{RepoContextActivationCensus.InstrumentName}'. The drain forecast reads the resident "
                + "activation set through that name and treats an unreadable count as merely unavailable, so "
                + "this failure is the ONLY place the loss is visible. Find the instrument's new name in the "
                + "Orleans catalog and update RepoContextActivationCensus.InstrumentName.");
        }
        finally
        {
            await app.StopAsync(TestContext.CurrentContext.CancellationToken);
        }
    }
}
