namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>One estate process whose memory the bounded-memory proof watches.</summary>
/// <param name="Name">Its name in the estate, such as <c>west-silo-0</c>.</param>
/// <param name="ProcessId">Its operating-system process id.</param>
/// <param name="ProbeRoot">The root address serving its memory probe: a silo's internal HTTP port, or the Explorer head.</param>
internal sealed record MeasuredProcess(string Name, int ProcessId, Uri ProbeRoot);
