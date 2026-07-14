using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// The read-only result of a capability probe: which backup / restore operations
/// the current caller may perform over a single <see cref="BackupScopeSelector"/>,
/// evaluated through the same fail-closed backup access gate the real operations
/// use but with <b>no side effects</b> - no data is read, captured, restored, or
/// deleted. Every flag is default-deny: a flag is <see langword="true"/> only when
/// the gate would authorize the corresponding operation for the probed scope, and
/// <see langword="false"/> for any denial.
/// </summary>
/// <remarks>
/// <para>
/// The probe is a UX affordance for a management surface (for example the state
/// explorer's backup area) so it can disable controls the caller cannot use; it
/// is <b>not</b> a security boundary. The control facade still authorizes every
/// real operation fail-closed on attempt, so an over-optimistic client that acts
/// on a stale or wrong flag is still refused by the server.
/// </para>
/// <para>
/// The underlying gate distinguishes two capabilities for a scope: the capture /
/// read authority (<see cref="LatticeOperation.Backup"/>) and the author /
/// bulk-load authority (<see cref="LatticeOperation.Restore"/>). Listing,
/// describing, capturing, capturing incrementally, and deleting all require the
/// capture / read authority, so <see cref="CanList"/>, <see cref="CanCapture"/>,
/// <see cref="CanCaptureIncremental"/> and <see cref="CanDelete"/> reflect the
/// same grant over the scope; <see cref="CanRestore"/> reflects the distinct
/// restore authority.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiBackupTypeAliases.BackupScopeCapabilities)]
[Immutable]
public sealed record BackupScopeCapabilities
{
    /// <summary>The scope these capabilities were evaluated over.</summary>
    [Id(0)] public required BackupScopeSelector Scope { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller may list / read (and so describe)
    /// backups whose scope is <see cref="Scope"/>.
    /// </summary>
    [Id(1)] public bool CanList { get; init; }

    /// <summary><see langword="true"/> when the caller may capture a full backup of <see cref="Scope"/>.</summary>
    [Id(2)] public bool CanCapture { get; init; }

    /// <summary><see langword="true"/> when the caller may capture an incremental backup of <see cref="Scope"/>.</summary>
    [Id(3)] public bool CanCaptureIncremental { get; init; }

    /// <summary><see langword="true"/> when the caller may restore into <see cref="Scope"/>.</summary>
    [Id(4)] public bool CanRestore { get; init; }

    /// <summary><see langword="true"/> when the caller may delete a backup whose scope is <see cref="Scope"/>.</summary>
    [Id(5)] public bool CanDelete { get; init; }
}
