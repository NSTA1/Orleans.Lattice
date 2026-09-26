# Orleans.Lattice.Explorer configuration

The Explorer libraries expose seven public options types, all documented here. `ExplorerConfigStoreOptions` is bound by the `AddExplorerConfiguration` registration extension. `LatticeExplorerWebOptions` is bound by the `AddLatticeExplorerWeb` extension (and read back by `MapLatticeExplorer`). `ExplorerNavigationOptions` and `ExplorerAuthUiOptions` are head-level options registered as DI singletons by a head; the web head derives both from `LatticeExplorerWebOptions` - `ExplorerNavigationOptions.AllowEndpointConfiguration` from `AllowInteractiveEndpointConfiguration`, and `ExplorerAuthUiOptions` as the server-form-post login flow with its paths under `BasePath` - and when no instance is registered each type falls back to its own defaults. The authentication layer's `ExplorerReauthOptions`, `ExplorerSignOutOptions`, and `ExplorerContentSecurityPolicyOptions` are seams a sign-in provider package configures (the [hosted-web Entra provider](../lattice.explorer.entra.web/configuration.md) sets all three; see also [Adding a custom auth method](adding-a-custom-auth-method.md)) rather than options a host usually binds directly. The launcher environment variables, the persisted configuration document, and the `LatticeConnectionSettings` record the connection is configured with are described at the end of this page. The optional Entra sign-in provider packages document their own options: `ExplorerEntraOptions` in [Orleans.Lattice.Explorer.Entra](../lattice.explorer.entra/README.md#configuration) and `ExplorerEntraWebOptions` in [Orleans.Lattice.Explorer.Entra.Web](../lattice.explorer.entra.web/configuration.md).

## `ExplorerConfigStoreOptions`

Options for the local JSON config store. Bind it through `AddExplorerConfiguration(configure)`. Each head supplies a per-user app-data location (the MAUI app-data directory on the desktop, the local application-data folder on the web server).

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultFileName` | `string` | `"config.json"` | The default config file name. |
| `DefaultFolderName` | `string` | `"Orleans.Lattice.Explorer"` | The default per-user sub-folder the config lives under. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `FilePath` | `string` | `DefaultFilePath()` | The full path to the JSON config document. The default is built under the per-user local application-data folder, for example `%LOCALAPPDATA%\Orleans.Lattice.Explorer\config.json` on Windows. |

## `ExplorerNavigationOptions`

Head-level options controlling how the navigation panel presents itself. Which areas the console surfaces is not an option: under the plugin model a head surfaces an area by registering its plugin and withholds it by not registering it, so the per-area `EnableSchemaArea` switch this type once carried is retired.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `AllowEndpointConfiguration` | `bool` | `true` | When `true`, the navigation panel renders the connection-settings affordance that opens the endpoint configuration dialog. When `false`, the affordance is withheld and the dialog cannot be opened. The desktop head, which is inherently single-user, leaves this at the default; the web head derives it from `LatticeExplorerWebOptions.AllowInteractiveEndpointConfiguration`, which is `false` by default. This is a UI affordance only - the authoritative refusal lives in the configuration store, so hiding the button is not the security boundary. |

## `ExplorerAuthUiOptions`

Per-head options controlling how the shared login / logout UI submits credentials. The desktop head signs in fully in-process; the web head instead posts to a server endpoint so the password never crosses the SignalR circuit and is stored in an encrypted, `HttpOnly` server cookie. A head registers an instance in DI; when none is registered the UI defaults to the in-process desktop flow.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `UseServerFormPost` | `bool` | `false` | When `true`, the login dialog renders a native HTML form that POSTs to `LoginPath` (the web head). When `false`, the dialog signs in in-process (the desktop head). |
| `LoginPath` | `string` | `"/auth/login"` | The server path the login form posts to when `UseServerFormPost` is set. |
| `LogoutPath` | `string` | `"/auth/logout"` | The server path the logout form posts to when `UseServerFormPost` is set. |

The web head registers `UseServerFormPost = true` with `LoginPath` and `LogoutPath` set to `auth/login` and `auth/logout` under its base path (for example `/explorer/auth/login` when `BasePath` is `/explorer`), matching the endpoints `MapLatticeExplorer` maps. The desktop head registers `UseServerFormPost = false`.

## `LatticeExplorerWebOptions`

Options controlling how the embeddable Explorer web head is registered and mapped. An instance is registered in DI by `AddLatticeExplorerWeb` and read back by `MapLatticeExplorer` and the host document component, so the two calls agree on the mount point.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `BasePath` | `string` | `"/"` | The base path the explorer is mounted under, for example `/explorer`. Defaults to `/` (mounted at the application root). A value is normalized on assignment to a single leading slash with no trailing slash (the root stays `/`). |
| `ConfigFilePath` | `string?` | `null` | An explicit path for the explorer's JSON configuration backing store. When `null`, the store falls back to the `LATTICE_EXPLORER_CONFIG` environment variable, then to the per-user app-data default. |
| `UseEnvironmentBootstrap` | `bool` | `true` | When `true`, the launcher-friendly environment bootstrap is registered, seeding the first-run endpoint from process environment variables when nothing is persisted yet. The credential half of that bootstrap is withheld on the web head unless `AllowEnvironmentCredentialSeed` is also set. |
| `AllowEnvironmentCredentialSeed` | `bool` | `false` | When `false` (the default), the environment bootstrap seeds only the secret-free endpoint and the sign-in credential read from the environment is never made available to a browser circuit. When `true`, an unauthenticated visitor's circuit is signed in with that credential and inherits the operator's grant, so enable it only on a host reachable by exactly one trusted operator. Ignored when `UseEnvironmentBootstrap` is `false`. |
| `AllowInteractiveEndpointConfiguration` | `bool` | `false` | When `false` (the default), the shared configuration store refuses writes and the connection-settings affordance is withheld, so a visitor cannot repoint the deployment's cluster endpoint. Configure the endpoint with `ConfigFilePath` or the environment bootstrap instead. Set to `true` only for a web head that is genuinely configured through its own UI by a trusted operator. |
| `DataProtectionKeyRingBlobUri` | `Uri?` | `null` | When set, the ASP.NET Data Protection key ring is persisted to this Azure Blob Storage blob (for example `https://account.blob.core.windows.net/keys/explorer-keyring.xml`) instead of the default per-instance ephemeral ring, so every replica shares one key ring and can decrypt the OpenID Connect session cookie any other replica issued. Required for a multi-replica / failover deployment; leave `null` for single-instance behaviour. `DataProtectionKeyRingCredential` must be supplied when this is set. See [Multi-replica and failover hosting](multi-replica-hosting.md). |
| `DataProtectionKeyRingCredential` | `TokenCredential?` | `null` | The `Azure.Core.TokenCredential` used to authenticate to the key-ring blob named by `DataProtectionKeyRingBlobUri` (for example a `DefaultAzureCredential` or a managed-identity credential). Required when `DataProtectionKeyRingBlobUri` is set; ignored otherwise. |
| `DataProtectionApplicationName` | `string?` | `null` | Sets the Data Protection application-discriminator name. Every replica that must decrypt one another's cookies has to share the same value, so set a stable, deployment-wide name (for example `lattice-explorer`) when persisting the key ring to shared storage. When `null`, the framework default (content-root-derived) discriminator is used. |
| `ConfigureDataProtection` | `Action<IDataProtectionBuilder>?` | `null` | Optional escape hatch invoked with the Data Protection builder after the built-in persistence and application-name configuration is applied, so a host can attach additional configuration (a different key store, key encryption at rest, a custom key lifetime). Runs whether or not the blob-persistence options above are set. |

Setting `DataProtectionKeyRingBlobUri` without `DataProtectionKeyRingCredential` throws `InvalidOperationException` at registration time (fail-closed): a half-configured shared key ring wedges every operator at the first failover, so the misconfiguration is surfaced loudly rather than falling back silently to the ephemeral ring.

## `ExplorerReauthOptions`

Configures where the re-authentication interstitial sends the browser when a sign-in latches into its revoked state (the token can no longer be renewed silently). `AddExplorerAuth` registers a default instance with `TryAdd`; a sign-in provider that maps a forced-interactive challenge endpoint registers its own instance afterwards, and that later registration is the one resolved. The hosted-web Entra provider does this from its `ReauthChallengePath`.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultReturnUrlParameter` | `string` | `"returnUrl"` | The default name of the query-string parameter that carries the local return URL. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `ChallengePath` | `string?` | `null` | The head-relative path of the forced-interactive challenge endpoint the interstitial navigates to with a full-page load. When `null` or empty, the interstitial reloads the current page instead - correct for a method that recovers on a plain reload, such as Basic, but unable to force a fresh authorization-code redemption. |
| `AppendReturnUrl` | `bool` | `true` | When `true`, the current page's local path and query is appended to `ChallengePath` as a URL-encoded return-URL parameter (after `&` when the path already carries a query string, otherwise after `?`), so the operator lands back where they were. The challenge endpoint is responsible for accepting it only as a local path. |
| `ReturnUrlParameter` | `string` | `DefaultReturnUrlParameter` (`"returnUrl"`) | The query-string parameter name for the return URL. It must match the parameter the challenge endpoint reads; an empty value falls back to `DefaultReturnUrlParameter`. |

## `ExplorerSignOutOptions`

Configures a federated sign-out: one that ends the browser's hosted-web session (the OpenID Connect cookie and the identity provider's session) as well as dropping the local State API credential. `AddExplorerAuth` registers a default instance with `TryAdd`; the hosted-web Entra provider registers its own from its `SignOutPath`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `FederatedSignOutPath` | `string?` | `null` | The head-relative path the "Sign out" button posts to, as a full-page, antiforgery-guarded form post. When set, it takes precedence over `ExplorerAuthUiOptions`. When `null` or empty, the button falls back to the local-only sign-out: a form post to `ExplorerAuthUiOptions.LogoutPath` when `UseServerFormPost` is set (the web head), or an in-circuit sign-out otherwise (the desktop head). On a hosted-web OpenID Connect head the local-only sign-out leaves the session cookie valid, so the fallback authorization policy would silently re-authenticate the circuit. |

## `ExplorerContentSecurityPolicyOptions`

Carries extra Content-Security-Policy source expressions that the web head's security-header middleware folds into its `form-action` directive, so a federated sign-out form whose POST redirects to an identity provider's end-session URL is not blocked by the baseline `form-action 'self'` (browsers enforce `form-action` across the whole redirect chain). A provider contributes with `services.Configure<ExplorerContentSecurityPolicyOptions>(...)`, and the middleware composes the accumulated set once, when it is constructed. The hosted-web Entra provider contributes the origin of its configured authority instance.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `AdditionalFormActionSources` | `IList<string>` (get-only) | empty | Extra sources appended to the `form-action` directive, which already contains `'self'`. Each entry is one CSP source expression, typically an origin such as `https://login.microsoftonline.com`. An entry that is blank or contains whitespace, `;` or `,` is dropped when the header is composed, so a malformed contribution cannot inject another directive or policy. With no valid entries the emitted policy is the unchanged baseline. |

## Environment variables

The launcher-friendly environment bootstrap (`AddExplorerEnvironmentBootstrap`, which the web head registers while `LatticeExplorerWebOptions.UseEnvironmentBootstrap` is `true`) reads these process environment variables. The endpoint seed is used only when no configuration is persisted yet, and is held in memory rather than written back to the store; the credential seed is kept separate from it, so a seeded endpoint is never persisted with a secret.

| Variable | Meaning |
|---|---|
| `LATTICE_EXPLORER_ENDPOINT` | The state-API endpoint URL to seed. When unset, nothing is seeded and the console shows its normal first-run (unconfigured) flow. |
| `LATTICE_EXPLORER_INSECURE_DEV` | When truthy (`1`, `true`, `yes` or `on`, case-insensitive), the seeded endpoint uses the insecure loopback-dev transport mode with unencrypted HTTP/2 allowed, for a local h2c development cluster. Otherwise the seed is secure by default. |
| `LATTICE_EXPLORER_TRANSPORT_HEADERS` | Optional non-secret transport headers attached to every call, as a semicolon-separated list of `Name=Value` pairs (for example an origin-lock routing header). A value may be empty or contain `=`; an entry with no name is skipped. |
| `LATTICE_EXPLORER_USERNAME`, `LATTICE_EXPLORER_PASSWORD` | An optional sign-in credential, used only when both are set and applied in memory for the current process. The web head withholds it unless `LatticeExplorerWebOptions.AllowEnvironmentCredentialSeed` is `true`. |

`LATTICE_EXPLORER_CONFIG` is read by the heads themselves rather than by the bootstrap: it overrides the path of the JSON configuration document (on the web head only when `ConfigFilePath` is unset), ahead of the per-user app-data default.

## The configuration document

The configuration store persists one `ExplorerConfiguration` record as JSON at `ExplorerConfigStoreOptions.FilePath`, using camelCase property names (read case-insensitively). A missing, corrupt, or unreadable document is treated as no configuration, and a loaded document whose endpoint fails the transport rules below is not applied, so the console stays unconfigured. A save writes a temporary file and moves it into place. On the web head the store refuses writes unless `AllowInteractiveEndpointConfiguration` is `true`, so a pre-provisioned document (or the environment bootstrap) is how a deployment ships its endpoint.

| Property | JSON name | Type | Default | Meaning |
|---|---|---|---|---|
| `SchemaVersion` | `schemaVersion` | `int` | `CurrentSchemaVersion` (`2`) | The document's schema version, for forward compatibility. |
| `Endpoint` | `endpoint` | `string` | `""` | The state-API endpoint, for example `https://host:443`, or `http://localhost:5199` for local development. |
| `TransportMode` | `transportMode` | `ExplorerTransportMode` (written as a number) | `Secure` (`0`) | `Secure` requires an `https` address for any non-loopback endpoint; `InsecureLoopbackDev` (`1`) is the explicit opt-in to the plaintext development path, accepted only for a loopback endpoint. |
| `AllowUnencryptedHttp2` | `allowUnencryptedHttp2` | `bool` | `false` | Allows unencrypted HTTP/2 (h2c) so a plain `http://` endpoint works for local development. Ignored for an `https://` endpoint. |
| `Headers` | `headers` | `IReadOnlyDictionary<string, string>?` | `null` | Optional non-secret metadata headers attached to every call through the authentication seam, so an interactive sign-in replaces them. The live credential is never stored here. |
| `TransportHeaders` | `transportHeaders` | `IReadOnlyDictionary<string, string>?` | `null` | Optional non-secret transport headers attached to every call regardless of the sign-in state, for example `X-Azure-FDID` for an Azure Front Door origin lock. See [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md#reaching-an-endpoint-behind-an-origin-locked-proxy). |

## `LatticeConnectionSettings`

The immutable record the state-API connection is configured with, passed to `ILatticeStateConnection.ConfigureAsync`. Every `ConfigureAsync` call rebuilds the channel and the background health monitor from the settings it is given. The configuration document above is mapped onto it by `ExplorerConfiguration.ToConnectionSettings()`: `Endpoint` becomes `Address`, `AllowUnencryptedHttp2` is copied, a non-empty `Headers` becomes `Authentication`, and a non-empty `TransportHeaders` is copied. `TransportMode` has no counterpart here; it is checked against the endpoint before the configuration is applied.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Address` | `string` | none (`required`) | The state-API endpoint, for example `https://host:443`, or `http://localhost:5199` for local development. An address the channel cannot be built for leaves the connection `Faulted` with an "Invalid endpoint" status rather than throwing. |
| `AllowUnencryptedHttp2` | `bool` | `false` | Enables unencrypted HTTP/2 (h2c) so a plain `http://` endpoint works; it has no effect on an `https://` endpoint. It is also the explicit opt-in required before a static sign-in credential, such as a Basic `authorization` header, is sent to a non-`https` endpoint. |
| `Authentication` | `LatticeCallAuthentication?` | `null` | The authentication seam attached to every call. `null` connects anonymously, which is only appropriate against a development endpoint with authorization disabled. The sign-in session sets it to the signed-in credential. |
| `TransportHeaders` | `IReadOnlyDictionary<string, string>?` | `null` | Non-secret headers attached to every call regardless of the authentication mode, for example `X-Azure-FDID` for an Azure Front Door origin lock. They survive a sign-in that replaces `Authentication`, and are never a credential. |
| `DegradeAfter` | `TimeSpan` | 5 seconds | How long the connection may stay `Reconnecting`, measured from the first transient failure since it last succeeded or was reconfigured, before it degrades to `Faulted`, the visual disconnected state. |
| `HealthCheckInterval` | `TimeSpan` | 1 second | How often the background health monitor probes the endpoint while the connection is `Connecting`, `Reconnecting`, or `Faulted`, to recover it. A `Connected` connection is not probed. |
| `TransientRetryBackoff` | `TimeSpan` | 250 milliseconds | The delay between inline retries of a single call after a transient failure, and before a dropped live stream is resubscribed. |
| `MaxTransientRetries` | `int` | `2` | How many times a single call is retried inline after a transient failure before the failure is surfaced to the caller; the background monitor then continues recovery. A load-shed refusal (gRPC `ResourceExhausted`) is never retried inline. |

The four timing properties take effect for any caller that passes them to `ConfigureAsync` directly, but none of them can be set through the configuration document, an options type, or an environment variable. The Explorer's own configuration session (on startup and whenever a configuration is applied) and its sign-in session (on every sign-in, sign-out, and re-applied credential, setting only `Authentication` on top) always rebuild the settings with `ToConnectionSettings()`, so a head that uses those sessions - both shipped heads do - always runs with the defaults above.
