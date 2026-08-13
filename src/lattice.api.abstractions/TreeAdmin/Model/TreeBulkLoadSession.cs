namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The result of opening a resumable bulk-load (tree creation) session with
/// <see cref="ILatticeTreeAdmin.BeginBulkLoadAsync"/>. It confirms the target
/// tree was empty and the caller is authorized, and echoes the caller-supplied
/// <see cref="OperationId"/> that keys the whole append / commit sequence for
/// idempotent resume.
/// </summary>
/// <remarks>
/// The session carries <b>no server-side state</b>: it is a confirmation that the
/// preconditions (whole-tree <see cref="LatticeOperation.BulkLoad"/> authority and
/// an empty target tree) held at begin time. Progress is tracked entirely by the
/// caller through the monotonic chunk index it supplies to
/// <see cref="ILatticeTreeAdmin.AppendBulkLoadAsync"/>; a dropped connection is
/// recovered by re-driving from the last un-acknowledged chunk under the same
/// <see cref="OperationId"/>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeBulkLoadSession)]
[Immutable]
public sealed record TreeBulkLoadSession
{
    /// <summary>The tree the bulk-load session targets.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The caller-supplied operation id that keys the session. Every
    /// <see cref="ILatticeTreeAdmin.AppendBulkLoadAsync"/> and
    /// <see cref="ILatticeTreeAdmin.CommitBulkLoadAsync"/> call for this load must
    /// carry the same value so a re-driven chunk is an idempotent no-op.
    /// </summary>
    [Id(1)] public required string OperationId { get; init; }
}
