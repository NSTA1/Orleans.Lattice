namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// The per-session <see cref="IExplorerShellEntryGate"/>. Registered scoped, so
/// one instance serves a circuit and the claim it records is the session's.
/// </summary>
public sealed class ExplorerShellEntryGate : IExplorerShellEntryGate
{
    private bool _claimed;

    /// <inheritdoc />
    public bool TryClaimEntry()
    {
        if (_claimed)
        {
            return false;
        }

        _claimed = true;
        return true;
    }
}
