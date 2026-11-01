namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>One pending wrapper: a step wrapped in <see cref="KnownDefect"/> or <see cref="KnownGap"/>.</summary>
/// <param name="Kind"><c>KnownDefect</c> or <c>KnownGap</c>.</param>
/// <param name="Issue">The tracking issue, as <c>#N</c>.</param>
/// <param name="Summary">What the defect or gap is.</param>
/// <param name="File">The source file it is written in, repository-relative, when found by the scan.</param>
/// <param name="Line">Its line, when found by the scan.</param>
public sealed record PendingWrapper(string Kind, string Issue, string Summary, string? File, int Line);
