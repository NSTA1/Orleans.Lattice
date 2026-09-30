using Orleans.Lattice.Api.Mcp;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Access-set builders for fixtures whose subject is group membership, tool
/// naming, or transport wiring rather than the per-tool operation minimum.
/// </summary>
/// <remarks>
/// <para>
/// The discovery core applies a per-tool operation minimum to every tool in an
/// admitted group, so a set that names a group while carrying no granted
/// operation is refused every tool in it. That is the correct fail-closed
/// reading of absent evidence, and it is a state the in-box resolver never
/// produces: it unions a rule's operations into the set in the same pass that
/// admits the group.
/// </para>
/// <para>
/// A fixture that wrote <c>None.With(group)</c> was therefore asserting against
/// an incoherent caller, and only passed while the minimum was skipped whenever
/// the evidence was missing. These builders keep such a fixture on the subject it
/// actually tests by pairing the group with operations, so it models a caller the
/// resolver could really hand the discovery core. Use
/// <see cref="LatticeApiMcpAccessSet.None"/> directly when the absence of a grant
/// is the point.
/// </para>
/// </remarks>
internal static class TestAccessSets
{
    /// <summary>
    /// Every operation a rule can carry, so a group's tools all clear their
    /// per-tool minimum and the fixture sees the whole group.
    /// </summary>
    internal const LatticeOperation AllOperations =
        LatticeOperation.Read
        | LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeRead
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad
        | LatticeOperation.Admin
        | LatticeOperation.Backup
        | LatticeOperation.Restore
        | LatticeOperation.SchemaAdmin
        | LatticeOperation.Telemetry
        | LatticeOperation.Replication
        | LatticeOperation.TreeLifecycle
        | LatticeOperation.AppInstall;

    /// <summary>
    /// An access set admitting <paramref name="groups"/> and carrying every
    /// operation, modelling a fully privileged caller.
    /// </summary>
    internal static LatticeApiMcpAccessSet Granting(params LatticeApiMcpGroup[] groups)
    {
        var access = LatticeApiMcpAccessSet.None.WithOperations(AllOperations);
        foreach (var group in groups)
        {
            access = access.With(group);
        }

        return access;
    }
}
