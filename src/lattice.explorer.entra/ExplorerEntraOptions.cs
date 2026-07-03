namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// Configuration for the Entra ID interactive login provider. The values may be
/// supplied statically here or discovered at connect time from the State API's
/// auth-scheme advertisement (the advertised parameters take precedence). All of
/// these are public OIDC parameters; no secret is ever configured on the client.
/// </summary>
public sealed class ExplorerEntraOptions
{
    /// <summary>
    /// The OIDC authority (for example
    /// <c>https://login.microsoftonline.com/&lt;tenant&gt;</c>). When set it
    /// takes precedence over <see cref="TenantId"/>.
    /// </summary>
    public string? Authority { get; set; }

    /// <summary>The directory tenant id (used to compose the authority when <see cref="Authority"/> is unset).</summary>
    public string? TenantId { get; set; }

    /// <summary>The public client (application) id registered in Entra.</summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// The scopes requested for the access token, identifying the State API
    /// audience (for example <c>api://&lt;app-id&gt;/.default</c>). At least one
    /// scope is required to acquire a token.
    /// </summary>
    public IList<string> Scopes { get; } = new List<string>();

    /// <summary>
    /// When <see langword="true"/>, sign-in uses the device-code flow (for
    /// headless/CLI hosts) instead of an interactive browser redirect. Defaults
    /// to <see langword="false"/>.
    /// </summary>
    public bool UseDeviceCode { get; set; }

    /// <summary>
    /// Invoked with the device-code prompt text when <see cref="UseDeviceCode"/>
    /// is enabled, so a host can surface it however it likes. Defaults to writing
    /// to the console.
    /// </summary>
    public Func<string, CancellationToken, Task>? DeviceCodeCallback { get; set; }
}
