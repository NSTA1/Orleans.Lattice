namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>One estate process whose memory the bounded-memory proof watches.</summary>
/// <param name="Name">Its name in the estate, such as <c>west-silo-0</c>.</param>
/// <param name="ProcessId">Its operating-system process id.</param>
/// <param name="MetricsPort">The port serving its <c>/metrics</c>, or <see langword="null"/> when it serves none.</param>
internal sealed record MeasuredProcess(string Name, int ProcessId, int? MetricsPort);
