namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Why a tool call was rejected as a caller mistake rather than failed as a
/// server fault (issue #3761). Each value is one arm of the <c>reason</c> tag on
/// <see cref="LatticeApiMcpMetrics.ToolClientErrors"/>.
/// </summary>
internal enum McpToolClientErrorReason
{
    /// <summary>
    /// A required argument was missing or empty, an argument carried a value the
    /// tool does not recognise, or the arguments could not be bound to the tool's
    /// parameters at all.
    /// </summary>
    InvalidArgument = 0,

    /// <summary>The call supplied an argument name the tool does not declare.</summary>
    UnknownArgument = 1,

    /// <summary>
    /// An argument arrived intact but its content was refused, for example a
    /// free-text value carrying leaked tool-call framing.
    /// </summary>
    RejectedContent = 2,

    /// <summary>The call named a record or resource that does not exist.</summary>
    NotFound = 3,
}
