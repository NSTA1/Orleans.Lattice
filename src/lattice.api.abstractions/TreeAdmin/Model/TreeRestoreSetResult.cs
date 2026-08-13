namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The per-member outcome of a backup-<b>set</b> restore
/// (<see cref="ILatticeTreeAdmin.RestoreTreeSetAsync"/>): one
/// <see cref="TreeRestoreResult"/> for every member tree this cluster restored, in
/// the order the backup engine applied them. A dedicated wrapper record is used
/// rather than a bare list so the result marshals over the single-message unary
/// gRPC binding.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeRestoreSetResult)]
[Immutable]
public sealed record TreeRestoreSetResult
{
    /// <summary>The per-member restore results this cluster applied, one per hosted member tree. Never <see langword="null"/>.</summary>
    [Id(0)] public required IReadOnlyList<TreeRestoreResult> Results { get; init; }
}
