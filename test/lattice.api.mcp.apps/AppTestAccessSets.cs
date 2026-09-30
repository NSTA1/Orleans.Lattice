using Orleans.Lattice;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Access-set builders for fixtures whose subject is the app tool surface rather
/// than the per-tool operation minimum the discovery core applies.
/// </summary>
/// <remarks>
/// The discovery core applies that minimum to every tool in an admitted group,
/// so a set naming a group while carrying no granted operation is refused every
/// tool in it. That is the correct fail-closed reading of absent evidence, and
/// it is a state the in-box resolver never produces - it unions a rule's
/// operations into the set in the same pass that admits the group. Pairing the
/// group with operations here keeps a fixture on the subject it actually tests.
/// Use <see cref="LatticeApiMcpAccessSet.None"/> directly when the absence of a
/// grant is the point, as <c>AppMcpTestHost</c> does.
/// </remarks>
internal static class AppTestAccessSets
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
