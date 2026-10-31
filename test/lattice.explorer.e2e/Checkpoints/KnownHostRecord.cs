using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>A host log record known not to be a fault, and the issue that tracks it.</summary>
/// <param name="Issue">The tracking issue, such as <c>#5442</c>.</param>
/// <param name="Pattern">Matches the whole record: its level, category and message shape.</param>
internal sealed record KnownHostRecord(string Issue, Regex Pattern);
