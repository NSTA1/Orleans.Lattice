namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>One estate process whose captured log a checkpoint reads.</summary>
/// <param name="Name">The process's name in the estate, such as <c>west-explorer-0</c>.</param>
/// <param name="LogFile">Where its output is captured.</param>
/// <param name="FailsCheckpoint">
/// Whether an error it logs fails the checkpoint (an Explorer head, a gateway) or is only
/// reported with it (a silo, whose errors a fault journey provokes on purpose).
/// </param>
public sealed record HostLogSource(string Name, string LogFile, bool FailsCheckpoint);
