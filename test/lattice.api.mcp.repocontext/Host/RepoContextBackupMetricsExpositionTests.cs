using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Asserts that the backup state series reaches the container's real
/// <c>/metrics</c> exposition, built by the real host builder, carrying a real
/// value, before any capture cycle has run.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this is the test that proves the design.</b> Every other test in this
/// change constructs <see cref="RepoContextBackupMeter"/> directly, so all of them
/// would still pass if the host builder stopped constructing it. An observable
/// instrument nobody resolves is never published, so that regression would remove
/// the series from <c>/metrics</c> entirely while leaving the unit tests green -
/// which is the exact silent-absence failure the meter exists to report on,
/// reproduced in its own test suite.
/// </para>
/// <para>
/// <b>It scrapes the real renderer, and deliberately does not start the host.</b>
/// <c>collector.Render()</c> is precisely what the <c>/metrics</c> endpoint returns,
/// so this covers the whole chain: the builder constructing the meter eagerly, the
/// collector subscribing to it by meter-name prefix, and the exposition rendering a
/// value. Hosted services are not started on purpose. Starting them would run the
/// backup service, whose first cycle would move the state off
/// <see cref="RepoContextBackupState.NeverCaptured"/> at a moment this test does not
/// control, making the assertion racy - and "no capture has run yet" is exactly the
/// condition under test.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextBackupMetricsExpositionTests
{
    private string _dataRoot = null!;

    [SetUp]
    public void SetUp()
        => _dataRoot = Path.Combine(Path.GetTempPath(), "repocontext-backupmetrics-" + Guid.NewGuid().ToString("N"));

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
    public void A_host_with_backup_configured_exposes_state_2_before_any_capture()
    {
        var exposition = RenderExposition(backupEnabled: true);

        var value = ReadSingleSample(exposition, RepoContextBackupMeter.StateGaugeName);

        Assert.That(
            value,
            Is.EqualTo((double)RepoContextBackupState.NeverCaptured),
            $"'{RepoContextBackupMeter.StateGaugeName}' must read "
            + $"{(int)RepoContextBackupState.NeverCaptured} (configured, nothing captured yet) on a host "
            + "that has just been built with a backup sink and has completed no capture. A different "
            + "value means the state derivation no longer distinguishes 'configured but nothing captured' "
            + "from the other states, which is the distinction the existing "
            + "orleans_lattice_backup_scope_last_run_status gauge cannot express.");
    }

    [Test]
    public void A_host_without_backup_configured_exposes_state_0_rather_than_nothing()
    {
        var exposition = RenderExposition(backupEnabled: false);

        var value = ReadSingleSample(exposition, RepoContextBackupMeter.StateGaugeName);

        Assert.That(
            value,
            Is.EqualTo((double)RepoContextBackupState.Disabled),
            $"'{RepoContextBackupMeter.StateGaugeName}' must read {(int)RepoContextBackupState.Disabled} "
            + "on a host with no backup sink configured. The series has to be PRESENT and say 'disabled' "
            + "rather than be absent, because an absent series is indistinguishable from a healthy one to "
            + "a scraper, which is the defect this meter exists to remove.");
    }

    [Test]
    public void Every_backup_instrument_reaches_the_exposition()
    {
        var exposition = RenderExposition(backupEnabled: true);

        Assert.Multiple(() =>
        {
            foreach (var name in new[]
            {
                RepoContextBackupMeter.StateGaugeName,
                RepoContextBackupMeter.CapturesCounterName,
                RepoContextBackupMeter.LastFullEntriesGaugeName,
                RepoContextBackupMeter.SinkBackupsGaugeName,
                RepoContextBackupMeter.IncrementalFallbacksCounterName,
            })
            {
                Assert.That(
                    TryReadSingleSample(exposition, name, out _),
                    Is.True,
                    $"'{name}' is absent from the rendered exposition. Either the host builder no longer "
                    + "constructs RepoContextBackupMeter, or its meter name has moved out from under "
                    + $"'{RepoContextMetricsCollector.MeterNamePrefix}' and the collector no longer "
                    + "subscribes to it. Both remove the series silently, leaving /metrics looking "
                    + "complete.");
            }
        });
    }

    private string RenderExposition(bool backupEnabled)
    {
        var settings = new Dictionary<string, string?>
        {
            [RepoContextHostConfiguration.DataRootKey] = _dataRoot,
            [RepoContextHostConfiguration.ClusterIdKey] = "repocontext-backupmetrics",
            [RepoContextHostConfiguration.ServiceIdKey] = "repocontext-backupmetrics",
        };

        if (backupEnabled)
        {
            // Never contacted: no hosted service is started, so this only has to
            // resolve to Enabled == true.
            settings[RepoContextBackup.BlobConnectionStringKey] = "UseDevelopmentStorage=true";
        }

        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder().AddInMemoryCollection(settings).Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        foreach (var pair in settings)
        {
            builder.Configuration[pair.Key] = pair.Value;
        }

        var app = RepoContextHostBuilder.Build(builder, config);
        try
        {
            // Exactly what the /metrics endpoint returns.
            return app.Services.GetRequiredService<RepoContextMetricsCollector>().Render();
        }
        finally
        {
            ((IDisposable)app).Dispose();
        }
    }

    private static double ReadSingleSample(string exposition, string family)
    {
        Assert.That(
            TryReadSingleSample(exposition, family, out var value),
            Is.True,
            $"'{family}' is absent from the rendered exposition entirely.");
        return value;
    }

    private static bool TryReadSingleSample(string exposition, string family, out double value)
    {
        var match = Regex.Match(
            exposition,
            "^" + Regex.Escape(family) + @"(?:\{[^}]*\})?[ \t]+(?<v>[-+0-9.eE]+)[ \t]*$",
            RegexOptions.Multiline);

        if (!match.Success)
        {
            value = default;
            return false;
        }

        value = double.Parse(match.Groups["v"].Value, System.Globalization.CultureInfo.InvariantCulture);
        return true;
    }
}
