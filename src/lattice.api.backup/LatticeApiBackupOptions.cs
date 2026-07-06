namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.Backup</c> add-on, the
/// transport-agnostic backup / restore control facade. Bound by
/// <see cref="LatticeApiBackupServiceCollectionExtensions.AddLatticeBackupApi"/>
/// and resolvable via <c>IOptions&lt;LatticeApiBackupOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The type carries the read-bounding knobs the control facade honours: the
/// default and maximum page sizes for the paged, cursor-resumable backup
/// catalog listing. Later issues in the backup control-API epic add further
/// knobs without changing the registration front door.
/// </remarks>
public sealed class LatticeApiBackupOptions
{
    /// <summary>
    /// Page size used for a backup-catalog listing
    /// (<see cref="ILatticeBackupControl.ListBackupsAsync"/>) when the request
    /// leaves its page size unset (<c>0</c> or negative). Defaults to
    /// <c>100</c>.
    /// </summary>
    public int DefaultListPageSize { get; set; } = 100;

    /// <summary>
    /// Largest backup-catalog listing page size honoured; larger requested page
    /// sizes are clamped down. Defaults to <c>1000</c>.
    /// </summary>
    public int MaxListPageSize { get; set; } = 1000;
}
