namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Optional seam that lets a topology defer (omit) individual tools from a
/// session's advertised tool set by name. A deferred tool is <b>never listed</b>
/// - not listed-then-erroring - so discovery semantics stay honest: every tool a
/// caller can see, the caller can invoke. Registered only by the remote-host
/// topology to defer tools whose backing gRPC method is not yet bound; absent
/// under the in-silo topology, where every tool is reachable in-process.
/// </summary>
internal interface ILatticeApiMcpUnsupportedToolSource
{
    /// <summary>
    /// Returns <see langword="true"/> when the tool named
    /// <paramref name="toolName"/> is not supported by the current topology and
    /// must be omitted from the advertised tool set.
    /// </summary>
    /// <param name="toolName">The protocol tool name to test.</param>
    bool IsUnsupported(string toolName);
}
