using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Publishes the backup protection of the durable agent-memory tree onto the
/// container's existing scrape endpoint, so a deployment whose captures are all
/// failing says so in <c>/metrics</c> instead of only in a log line nobody is
/// alerting on.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this fixes.</b> Issue #2640 records that <c>/metrics</c> carried no
/// backup series of any kind while every capture threw, so there was nothing for an
/// alert rule to be written against. <see cref="RepoContextBackupStatus"/> already
/// held every fact needed; nothing had ever exported it. This type is only the
/// export, and it derives nothing of its own: the verdict comes from
/// <see cref="RepoContextBackupStatus.State"/>, which the health check reads too, so
/// the two surfaces cannot disagree.
/// </para>
/// <para>
/// <b>Every instrument is observable, and that is load-bearing.</b> An instrument
/// that is created on the first failure does not exist until the failure, so the
/// series an alert needs is missing during exactly the window the alert is meant to
/// cover, and a scraper cannot distinguish "no failures" from "no instrument". Worse,
/// a series first created late can be refused outright: the collector caps its series
/// (issue #2480), and a cap refuses <em>new</em> series, so an instrument that waits
/// for an event can find the door shut when the event arrives. Observable instruments
/// are sampled at scrape time from process start, so every series here exists, with a
/// real value, from the first scrape onwards.
/// </para>
/// <para>
/// <b>It is constructed eagerly by the host builder.</b> An observable instrument
/// that nobody resolves is never published, so registering this as a lazy singleton
/// would reproduce the absence it exists to remove. See the construction site in
/// <c>RepoContextHostBuilder</c>, immediately after the metrics collector so the
/// listener is already running when these instruments publish.
/// </para>
/// </remarks>
public sealed class RepoContextBackupMeter : IDisposable
{
    /// <summary>
    /// The gauge reporting protection as a <see cref="RepoContextBackupState"/>
    /// ordinal. Only <see cref="RepoContextBackupState.Protected"/> means the tree is
    /// captured as configured, so an alert is written against inequality with that
    /// value rather than against an ordering of the others.
    /// </summary>
    public const string StateGaugeName = "lattice_repocontext_backup_state";

    /// <summary>The counter reporting captures this container has completed.</summary>
    public const string CapturesCounterName = "lattice_repocontext_backup_captures_total";

    /// <summary>
    /// The gauge reporting how many key descriptors the most recent full capture
    /// carried.
    /// </summary>
    public const string LastFullEntriesGaugeName = "lattice_repocontext_backup_last_full_entries";

    /// <summary>
    /// The gauge reporting how many backups of the configured tree the external sink
    /// was found to hold, or <c>-1</c> when the sink has not been enumerated.
    /// </summary>
    public const string SinkBackupsGaugeName = "lattice_repocontext_backup_sink_backups";

    /// <summary>
    /// The counter reporting incremental captures silently promoted to full ones.
    /// </summary>
    public const string IncrementalFallbacksCounterName =
        "lattice_repocontext_backup_incremental_fallbacks_total";

    // Declared above every instrument it constructs, and every instrument is built
    // from this field, so a reordering throws at initialisation rather than
    // publishing against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;

    private readonly RepoContextBackupStatus _status;

    /// <summary>Creates the meter and publishes every instrument.</summary>
    /// <param name="status">The live backup status this meter reports. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="status"/> is null.</exception>
    public RepoContextBackupMeter(RepoContextBackupStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        _status = status;

        // Published on the host meter, whose name sits under the collector's
        // subscribed prefix, so these series reach the existing /metrics endpoint
        // with no exposition change.
        _meter = new Meter(RepoContextDrainForecastService.MeterName);

        _meter.CreateObservableGauge(
            StateGaugeName,
            () => (int)_status.State,
            unit: "{state}",
            description:
                "Protection of the durable agent-memory tree as a RepoContextBackupState ordinal: "
                + "0 disabled, 1 failing with nothing ever captured, 2 configured but nothing captured "
                + "yet, 3 capturing but the last full capture described zero entries, 4 failing after an "
                + "earlier success, 5 protected. Only 5 means the tree is captured as configured, and the "
                + "other values are identifiers rather than a severity ranking, so alert on != 5 rather "
                + "than on a threshold. The series is published from process start and sampled on every "
                + "scrape, so its absence means the host did not construct this meter or the collector "
                + "refused the series at a ceiling, and never that backup is healthy.");

        _meter.CreateObservableCounter(
            CapturesCounterName,
            () => (long)_status.CaptureCount,
            unit: "{capture}",
            description:
                "Captures of the agent-memory tree this container has completed since it started. It "
                + "denominates "
                + StateGaugeName
                + ": a container reporting state 2 with this at zero has attempted and achieved nothing, "
                + "which is the condition that previously reported healthy on every surface.");

        _meter.CreateObservableGauge(
            LastFullEntriesGaugeName,
            () => _status.LastFullEntryCount,
            unit: "{entry}",
            description:
                "Key descriptors carried by the most recent full capture's manifest. Zero after a "
                + "successful capture means an empty or wrongly-scoped selection was captured, which "
                + "succeeds and reports success everywhere else while protecting nothing.");

        _meter.CreateObservableGauge(
            SinkBackupsGaugeName,
            () => _status.SinkBackupCount,
            unit: "{backup}",
            description:
                "Backups of the configured tree found in the external sink when it was enumerated, or -1 "
                + "when it has not been enumerated. Zero and -1 are different facts: zero means the sink "
                + "was readable and empty, -1 means it was never successfully read. This counts what is "
                + "recoverable after the store is destroyed, which "
                + CapturesCounterName
                + " cannot, because that counts only what this process captured.");

        _meter.CreateObservableCounter(
            IncrementalFallbacksCounterName,
            () => (long)_status.IncrementalFallbackCount,
            unit: "{fallback}",
            description:
                "Incremental captures the capture service silently promoted to full captures. The data is "
                + "safe and the capture succeeded, so this is invisible in every success signal; a count "
                + "tracking the number of incrementals attempted means the configured cadence is not "
                + "being honoured and every hour is paying for a full capture.");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}
