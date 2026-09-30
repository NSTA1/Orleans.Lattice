namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// Whether the session chrome lets the browser edit and test the cluster endpoint.
/// </summary>
/// <remarks>
/// <para>
/// The connection dialog's Test connection dials, from the head, whatever address
/// the visitor typed, and the outcome tells the visitor whether something answered
/// there. On a head that anyone can reach that is a probe into the head's own
/// network, so the whole edit surface - the Connection settings entry, the editable
/// form, the test and the save - is offered only when the head opted in to
/// browser-driven endpoint configuration. Otherwise the chrome shows the configured
/// endpoint read-only.
/// </para>
/// <para>
/// Fail closed: the Shell's default registration (added with <c>TryAdd</c>) refuses
/// interactive configuration, and a head that accepts it registers its own instance
/// first. The instance is immutable and carries no per-circuit state, which is what
/// makes a singleton registration safe.
/// </para>
/// </remarks>
internal sealed class SessionEndpointConfigurationOptions
{
    /// <summary>
    /// Whether the browser may edit, test and save the connection endpoint.
    /// <see langword="false"/> (the default) shows the configured endpoint read-only.
    /// </summary>
    public bool AllowInteractiveEndpointConfiguration { get; init; }
}
