namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of inspecting a memory <c>body</c> for MCP tool-call framing that
/// leaked into it, as produced by <see cref="RepoContextBodyFraming.Inspect"/>.
/// </summary>
/// <param name="IsContaminated">
/// <see langword="true"/> when the body ends in a run of tool-call framing rather
/// than in prose, which means the call that produced it was malformed.
/// </param>
/// <param name="DisplacedArguments">
/// The distinct argument names named by the trailing framing, in the order they
/// appear. These are the arguments the caller almost certainly intended to supply
/// separately and which were instead absorbed into the body, so nothing recorded
/// that they were ever supplied. Empty when the trailing framing names none - a
/// body that merely acquired a structural close tag has nothing displaced.
/// </param>
internal readonly record struct RepoContextBodyFramingInspection(
    bool IsContaminated,
    IReadOnlyList<string> DisplacedArguments);
