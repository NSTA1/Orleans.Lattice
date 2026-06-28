namespace Orleans.Lattice.Api.State;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.State</c> add-on, the
/// read-only cluster state API. Bound by
/// <see cref="LatticeApiStateServiceCollectionExtensions.AddLatticeStateApi"/>
/// and resolvable via <c>IOptions&lt;LatticeApiStateOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The type carries the read-bounding knobs the read facade honours: the
/// entry-scan page-size cap and the scan / single-entry value-preview byte
/// budgets. Later issues in the cluster-state-API epic add further knobs
/// (sampling cadences, the authorization posture, and so on) without changing
/// the registration front door.
/// </remarks>
public sealed class LatticeApiStateOptions
{
    /// <summary>
    /// Page size used for an entry scan when the request leaves
    /// <c>PageSize</c> unset (<c>0</c> or negative). Defaults to <c>100</c>.
    /// </summary>
    public int DefaultScanPageSize { get; set; } = 100;

    /// <summary>
    /// Largest entry-scan page size honoured; larger requested page sizes are
    /// clamped down. Defaults to <c>1000</c>.
    /// </summary>
    public int MaxScanPageSize { get; set; } = 1000;

    /// <summary>
    /// Value-preview byte budget used for an entry scan when the request
    /// leaves the budget unset (<c>0</c> or negative). Keeps whole values off
    /// the wire during a list scan. Defaults to <c>256</c> bytes.
    /// </summary>
    public int DefaultScanValuePreviewBytes { get; set; } = 256;

    /// <summary>
    /// Largest value-preview byte budget honoured for an entry scan; larger
    /// requested budgets are clamped down. Defaults to <c>65536</c> bytes.
    /// </summary>
    public int MaxScanValuePreviewBytes { get; set; } = 64 * 1024;

    /// <summary>
    /// Value-preview byte budget for a single-key
    /// <c>GetEntryAsync</c> detail read. Larger than the scan budget because a
    /// detail pane shows one entry at a time. Defaults to <c>1048576</c> bytes.
    /// </summary>
    public int SingleEntryValuePreviewBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Page size used for a per-key history read
    /// (<c>GetEntryHistoryAsync</c>) when the request leaves <c>Limit</c> unset
    /// (<c>0</c> or negative). Defaults to <c>100</c>.
    /// </summary>
    public int DefaultHistoryPageSize { get; set; } = 100;

    /// <summary>
    /// Largest per-key history page size honoured; larger requested limits are
    /// clamped down. Defaults to <c>1000</c>.
    /// </summary>
    public int MaxHistoryPageSize { get; set; } = 1000;

    /// <summary>
    /// Per-revision value / delta preview byte budget for a per-key history read
    /// when the request leaves the budget unset (<c>0</c> or negative). The
    /// durable history substrate already clips stored previews to a fixed
    /// per-revision ceiling, so a larger budget cannot recover more bytes than
    /// were retained. Defaults to <c>256</c> bytes.
    /// </summary>
    public int DefaultHistoryValuePreviewBytes { get; set; } = 256;

    /// <summary>
    /// Largest per-revision value / delta preview byte budget honoured for a
    /// per-key history read; larger requested budgets are clamped down. Defaults
    /// to <c>256</c> bytes, the per-revision ceiling the history substrate
    /// stores.
    /// </summary>
    public int MaxHistoryValuePreviewBytes { get; set; } = 256;

    /// <summary>
    /// How long a change-observation subscription waits before re-polling the
    /// WAL tail once it has drained all currently-available changes. Lower
    /// values reduce notification latency at the cost of more idle WAL reads.
    /// Defaults to 250&#160;ms.
    /// </summary>
    public TimeSpan ChangeObservationPollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Maximum number of WAL entries read per partition per drain cycle by a
    /// change-observation subscription. Bounds the work and memory of a single
    /// catch-up read. Defaults to <c>256</c>.
    /// </summary>
    public int ChangeObservationPageSize { get; set; } = 256;

    /// <summary>
    /// Default cadence at which the metadata / metrics observation feed samples
    /// per-tree aggregates when a request does not override it. Because the
    /// feed samples already-maintained aggregates on a timer (rather than per
    /// mutation), this trades dashboard-gauge freshness against sampling cost.
    /// Defaults to one second.
    /// </summary>
    public TimeSpan MetricsSampleInterval { get; set; } = TimeSpan.FromSeconds(1);
}
