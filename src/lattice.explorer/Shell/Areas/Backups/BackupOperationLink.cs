using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>A link an operation's status page offers once the operation has produced something, such as the captured backup.</summary>
/// <param name="Text">The link text.</param>
/// <param name="Target">Where it goes.</param>
internal sealed record BackupOperationLink(string Text, ExplorerAddress Target);
