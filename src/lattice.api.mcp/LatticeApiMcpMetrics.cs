using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/>
/// instruments for <c>Orleans.Lattice.Api.Mcp</c>. Every instrument is published
/// on a single <see cref="Meter"/> named <see cref="MeterName"/>, so an
/// OpenTelemetry pipeline subscribes once and receives every MCP-host metric.
/// </summary>
public static class LatticeApiMcpMetrics
{
    /// <summary>
    /// The root meter name for all <c>Orleans.Lattice.Api.Mcp</c> telemetry.
    /// Subscribers reference this constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice.api.mcp";

    /// <summary>Tag key naming the invoked MCP tool, on <see cref="ToolClientErrors"/>.</summary>
    public const string TagTool = "tool";

    /// <summary>Tag key naming why a call was rejected, on <see cref="ToolClientErrors"/>.</summary>
    public const string TagReason = "reason";

    /// <summary>
    /// <see cref="TagReason"/> value for a missing, empty, unrecognised, or unbindable argument.
    /// </summary>
    public const string ReasonInvalidArgument = "invalid_argument";

    /// <summary><see cref="TagReason"/> value for an argument name the tool does not declare.</summary>
    public const string ReasonUnknownArgument = "unknown_argument";

    /// <summary>
    /// <see cref="TagReason"/> value for an argument whose content was refused, for
    /// example a body carrying leaked tool-call framing.
    /// </summary>
    public const string ReasonRejectedContent = "rejected_content";

    /// <summary><see cref="TagReason"/> value for a record or resource the call named that does not exist.</summary>
    public const string ReasonNotFound = "not_found";

    /// <summary>Canonical name of the <see cref="ToolClientErrors"/> counter.</summary>
    public const string ToolClientErrorsName = "orleans.lattice.api.mcp.tool.client_errors";

    /// <summary>
    /// The meter that owns every MCP-host instrument. Exposed publicly so tests and
    /// custom exporters can subscribe by reference rather than by name.
    /// </summary>
    /// <remarks>
    /// Must stay above every instrument declared below it, and every instrument
    /// must be constructed from it: static field initialisers run in declaration
    /// order, so an instrument declared above this field would be built while it
    /// is still <see langword="null"/>.
    /// </remarks>
    public static readonly Meter Meter = new(MeterName);

    /// <summary>
    /// Count of tool calls rejected as the caller's mistake, tagged by
    /// <see cref="TagTool"/>, <see cref="TagReason"/>, and the platform tenant
    /// (issue #3761).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A client error is answered with an MCP error result and logged at Debug
    /// without a stack. It used to be thrown, and the ModelContextProtocol SDK logs
    /// every thrown tool exception at Error with its stack, so a malformed call was
    /// indistinguishable in the log from a server fault. This counter is how the
    /// rate stays visible now that it no longer reaches the error log. Server
    /// faults and authorization denials are still thrown, so they still log at
    /// Error.
    /// </para>
    /// <para>
    /// Not primed: the tool name is only known once a session's tool collection is
    /// assembled, so an absent series means no client error of that tool and reason
    /// has happened in this process, not a measured zero.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> ToolClientErrors =
        Meter.CreateCounter<long>(ToolClientErrorsName, unit: "{error}",
            description: "Count of MCP tool calls rejected as a caller mistake (a missing or malformed argument, refused content, or an unknown record), answered with an error result instead of logged as a server fault.");

    /// <summary>Records one client error of <paramref name="reason"/> on <paramref name="toolName"/>.</summary>
    /// <param name="toolName">The invoked tool.</param>
    /// <param name="reason">Why the call was rejected.</param>
    internal static void RecordToolClientError(string toolName, McpToolClientErrorReason reason)
        => ToolClientErrors.Add(
            1,
            new KeyValuePair<string, object?>(TagTool, toolName),
            new KeyValuePair<string, object?>(TagReason, McpToolClientErrors.ReasonTag(reason)),
            LatticeTenantLabel.Platform);
}
