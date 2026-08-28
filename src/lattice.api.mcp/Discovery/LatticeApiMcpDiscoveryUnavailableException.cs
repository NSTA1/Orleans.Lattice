namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Thrown when MCP session discovery could not resolve the caller's permissions
/// because the backend was transiently unavailable - a cancelled, deadline-exceeded,
/// unavailable, or internal transport fault, an Orleans response timeout, or silo
/// churn - rather than because the caller was genuinely denied.
/// </summary>
/// <remarks>
/// <para>
/// Discovery is <b>fail-closed</b> for a genuine denial: a caller with no grants is
/// advertised no group tools. That guarantee is unchanged. This exception exists to
/// stop a transient infrastructure fault from being reported to the client as a
/// <em>successful</em> but falsely narrow permission set, which is indistinguishable
/// from "your grants were revoked" and led clients to cache a one-tool session for
/// the lifetime of the connection.
/// </para>
/// <para>
/// The fault escapes the session configurator so the transport answers the
/// initialisation with an error the client can retry, instead of a well-formed
/// advertisement built from an answer the server never actually received. Retrying
/// is deliberately left to the client: the observed failure mode was a saturated
/// silo, and an in-process retry loop would add load to the very component that is
/// already too slow to answer.
/// </para>
/// </remarks>
internal sealed class LatticeApiMcpDiscoveryUnavailableException : Exception
{
    /// <summary>Initialises the exception with a message and the underlying transport fault.</summary>
    /// <param name="message">A description of the discovery step that could not complete.</param>
    /// <param name="innerException">The classified transient fault.</param>
    public LatticeApiMcpDiscoveryUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
