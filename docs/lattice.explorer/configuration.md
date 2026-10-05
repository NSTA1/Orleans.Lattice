# Orleans.Lattice.Explorer configuration

The Explorer rewrite exposes these public options types in the core web packages:
`ExplorerConfigStoreOptions`, `LatticeExplorerWebOptions`,
`ExplorerReauthOptions`, `ExplorerSignOutOptions`, and
`ExplorerContentSecurityPolicyOptions`. The UI sign-in chrome uses an internal
server-form-post option object; hosts configure it indirectly through the web
head and through the public re-authentication and sign-out options below.

This page also documents the launcher environment variables, the persisted JSON
configuration document, and `LatticeConnectionSettings`. Optional Entra packages
have their own option tables: `ExplorerEntraOptions` in
[`Orleans.Lattice.Explorer.Entra`](../lattice.explorer.entra/README.md#configuration)
and `ExplorerEntraWebOptions` in
[`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/configuration.md).

## `ExplorerConfigStoreOptions`

Options for the local JSON configuration store. Bind it through
`AddExplorerConfiguration(configure)`, or let `AddLatticeExplorerWeb` configure it
from `LatticeExplorerWebOptions.ConfigFilePath` and `LATTICE_EXPLORER_CONFIG`.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultFileName` | `string` | `"config.json"` | The default configuration file name. |
| `DefaultFolderName` | `string` | `"Orleans.Lattice.Explorer"` | The default per-user subfolder. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `FilePath` | `string` | `DefaultFilePath()` | The full path to the JSON configuration document. The default is under the per-user local application-data folder, for example `%LOCALAPPDATA%\Orleans.Lattice.Explorer\config.json` on Windows. |

## `LatticeExplorerWebOptions`

Options controlling the embeddable web head. `AddLatticeExplorerWeb` registers
one instance in DI and `MapLatticeExplorer` reads it back so registration and
endpoint mapping agree on the mount point.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `BasePath` | `string` | `"/"` | The base path the Explorer is mounted under, for example `/explorer`. Assignment normalises the value to a single leading slash with no trailing slash; the root remains `/`. |
| `ConfigFilePath` | `string?` | `null` | Explicit path for the JSON configuration backing store. When `null`, the web head uses `LATTICE_EXPLORER_CONFIG`, then the per-user app-data default. |
| `UseEnvironmentBootstrap` | `bool` | `true` | Registers the launcher-friendly environment bootstrap. When no configuration is persisted, it can seed the endpoint and optional sign-in credential from environment variables. |
| `AllowEnvironmentCredentialSeed` | `bool` | `false` | Allows the environment bootstrap to seed `LATTICE_EXPLORER_USERNAME` and `LATTICE_EXPLORER_PASSWORD` into an empty browser credential store. Enable only for a single-operator deployment; otherwise every anonymous browser would inherit the seeded operator credential. Ignored when `UseEnvironmentBootstrap` is `false`. |
| `AllowInteractiveEndpointConfiguration` | `bool` | `false` | Allows browser users to edit, test and save the process-wide endpoint configuration: it gates the header's **Connection settings** entry, the editable connection dialog and its **Test connection**. The default wraps the store as read-only and shows the endpoint in a read-only **Cluster connection** dialog, so deploy the endpoint through `ConfigFilePath`, `LATTICE_EXPLORER_CONFIG`, or `LATTICE_EXPLORER_ENDPOINT`. See [The connection dialog](running-the-explorer.md#the-connection-dialog). |
| `DataProtectionKeyRingBlobUri` | `Uri?` | `null` | Azure Blob Storage URI for a shared ASP.NET Data Protection key ring. Use this for multi-replica hosted-web sign-in so replicas can decrypt each other's cookies. `DataProtectionKeyRingCredential` is required when this is set. |
| `DataProtectionKeyRingCredential` | `TokenCredential?` | `null` | Azure credential used to read and write the key-ring blob named by `DataProtectionKeyRingBlobUri`. Required when the blob URI is set; ignored otherwise. |
| `DataProtectionApplicationName` | `string?` | `null` | Optional Data Protection application discriminator. Set the same stable value on every replica that must share cookies. |
| `ConfigureDataProtection` | `Action<IDataProtectionBuilder>?` | `null` | Escape hatch invoked after the built-in Data Protection configuration, so a host can add key encryption, a custom key lifetime, or a different store. |

Setting `DataProtectionKeyRingBlobUri` without
`DataProtectionKeyRingCredential` throws `InvalidOperationException` during
registration. A half-configured shared key ring fails closed instead of silently
falling back to a per-instance ephemeral key ring.

## `ExplorerReauthOptions`

Configures where the session chrome sends the browser when a token-based sign-in
latches into a revoked state and needs a fresh interactive sign-in.
`AddExplorerAuth` registers a default instance with no challenge path. A provider
such as hosted-web Entra registers its own instance to point at its mapped
re-authentication endpoint.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultReturnUrlParameter` | `string` | `"returnUrl"` | Default query-string parameter name for the local return URL. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `ChallengePath` | `string?` | `null` | Head-relative path of the forced-interactive challenge endpoint. When `null` or empty, the interstitial performs a full-page reload instead. |
| `AppendReturnUrl` | `bool` | `true` | Appends the current local path and query to `ChallengePath` as a return URL. The challenge endpoint must validate it as local. |
| `ReturnUrlParameter` | `string` | `DefaultReturnUrlParameter` (`"returnUrl"`) | Query-string parameter used for the return URL. |

## `ExplorerSignOutOptions`

Configures a federated sign-out endpoint for a provider whose sign-in creates a
separate browser identity-provider session. `AddExplorerAuth` registers the
local-only default; a provider can register an instance that points at its own
server endpoint.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `FederatedSignOutPath` | `string?` | `null` | Head-relative path the identity menu posts to for federated sign-out. When set, it wins over the local sign-out path. When `null` or empty, the session chrome clears only the local State API credential. |

## `ExplorerContentSecurityPolicyOptions`

Carries extra Content-Security-Policy source expressions that the web head folds
into the `form-action` directive. Federated sign-out providers use this when a
local sign-out `POST` redirects to an identity-provider end-session URL.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `AdditionalFormActionSources` | `IList<string>` (get-only) | Empty | Extra sources appended to `form-action`, which already contains `'self'`. Blank entries and entries containing whitespace, `;`, or `,` are dropped when the header is composed. |

## Environment variables

The launcher bootstrap, registered while
`LatticeExplorerWebOptions.UseEnvironmentBootstrap` is `true`, reads every
variable below except `LATTICE_EXPLORER_CONFIG`, which `AddLatticeExplorerWeb`
reads at registration whatever that option says.

| Variable | Meaning |
|---|---|
| `LATTICE_EXPLORER_CONFIG` | Overrides the JSON configuration document path when `LatticeExplorerWebOptions.ConfigFilePath` is unset. |
| `LATTICE_EXPLORER_ENDPOINT` | State API endpoint URL to seed when no configuration is persisted. |
| `LATTICE_EXPLORER_INSECURE_DEV` | Truthy values (`1`, `true`, `yes`, `on`, case-insensitive) seed `InsecureLoopbackDev` and allow unencrypted HTTP/2 for a loopback development endpoint. |
| `LATTICE_EXPLORER_TRANSPORT_HEADERS` | Optional semicolon-separated `Name=Value` pairs for non-secret transport headers attached to every call. Entries without `=` or without a name are skipped; values may be empty or contain `=`. |
| `LATTICE_EXPLORER_USERNAME`, `LATTICE_EXPLORER_PASSWORD` | Optional Basic credential seed. The web head withholds it unless `AllowEnvironmentCredentialSeed` is also `true`. |

The endpoint seed is held in memory and used only when no persisted
configuration exists. The credential seed is exposed through a separate in-memory
credential seam, never through the persisted configuration document.

The bootstrap reads its variables through `IExplorerEnvironment`, which defaults
to the process environment (`ProcessExplorerEnvironment`). A host that registers
its own `IExplorerEnvironment` before calling `AddLatticeExplorerWeb` supplies the
values instead, as the [Explorer sample](../../samples/Explorer/README.md) does.
`LATTICE_EXPLORER_CONFIG` is always read from the process environment.

## The configuration document

The configuration store persists one `ExplorerConfiguration` record as JSON at
`ExplorerConfigStoreOptions.FilePath`, using camelCase names and case-insensitive
reading. A missing, corrupt or unreadable document reads as no configuration, so
the launcher's endpoint seed applies when there is one; a transport-invalid
document leaves the Explorer unconfigured. Saves write a temporary file and then
move it into place.
On the web head, saves are refused, and the browser is offered neither the
connection form nor its test, unless `AllowInteractiveEndpointConfiguration` is
`true`.

| Property | JSON name | Type | Default | Meaning |
|---|---|---|---|---|
| `SchemaVersion` | `schemaVersion` | `int` | `CurrentSchemaVersion` (`2`) | Document schema version. |
| `Endpoint` | `endpoint` | `string` | `""` | State API endpoint, for example `https://host:443` or `http://localhost:5199`. |
| `TransportMode` | `transportMode` | `ExplorerTransportMode` (number) | `Secure` (`0`) | `Secure` requires `https` for non-loopback endpoints. `InsecureLoopbackDev` (`1`) is the explicit plaintext development opt-in for loopback endpoints. |
| `AllowUnencryptedHttp2` | `allowUnencryptedHttp2` | `bool` | `false` | Allows h2c for a plain `http://` local development endpoint. Ignored for `https://`. |
| `Headers` | `headers` | `IReadOnlyDictionary<string, string>?` | `null` | Optional non-secret metadata headers mapped to the authentication seam. Interactive sign-in replaces this seam, so do not use it for routing headers that must survive sign-in. |
| `TransportHeaders` | `transportHeaders` | `IReadOnlyDictionary<string, string>?` | `null` | Optional non-secret transport headers attached to every call regardless of sign-in state, for example `X-Azure-FDID` for an origin-locked proxy. |

## `LatticeConnectionSettings`

`ExplorerConfiguration.ToConnectionSettings()` maps the persisted document to
this immutable connection snapshot. `Endpoint` becomes `Address`,
`AllowUnencryptedHttp2` is copied, non-empty `Headers` becomes
`Authentication`, and non-empty `TransportHeaders` is copied. `TransportMode` is
validated before the settings are applied and has no property on the live record.
It sets no `ActiveTenantProvider`: a `LatticeStateConnection` constructed with a
tenant source, which is the constructor dependency injection selects once the
head registers tenancy, attaches that source to any settings that carry none.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Address` | `string` | none (`required`) | State API endpoint. Invalid addresses leave the connection faulted with an invalid-endpoint status rather than throwing from the UI. |
| `AllowUnencryptedHttp2` | `bool` | `false` | Enables h2c for plain `http://` development endpoints. It is also required before a static sign-in credential is sent to a non-`https` endpoint. |
| `Authentication` | `LatticeCallAuthentication?` | `null` | Authentication seam attached to calls. `null` connects anonymously. |
| `TransportHeaders` | `IReadOnlyDictionary<string, string>?` | `null` | Non-secret headers attached to every call regardless of sign-in state. |
| `ActiveTenantProvider` | `ILatticeActiveTenantProvider?` | `null` | Live source of the tenant every call asserts through the `lattice-active-tenant` header (`LatticeActiveTenantAssertion.DefaultHeaderName`). It is read as each call starts, never when the channel is built, so a tenant switch changes the next call without a rebuild. When it is set, the connection owns that header: a value for it among `TransportHeaders` is replaced by the provider's answer, or removed when the provider asserts none. `null` asserts no tenant. The web head's tenancy registration (`AddExplorerTenantView`) supplies a per-circuit provider that asserts the circuit's active tenant, and nothing for the reserved `default` tenant. |
| `DegradeAfter` | `TimeSpan` | 5 seconds | Time a connection may keep failing transiently before degrading to `Faulted`. |
| `HealthCheckInterval` | `TimeSpan` | 1 second | How often the background monitor probes while connecting, reconnecting, or faulted. |
| `TransientRetryBackoff` | `TimeSpan` | 250 milliseconds | Delay between inline transient retries. |
| `MaxTransientRetries` | `int` | `2` | Inline transient retry count before surfacing a `LatticeStateApiException`; load-shed `ResourceExhausted` failures are not retried inline. |

The four timing properties can be set by code that calls `ConfigureAsync`
directly. They are not exposed through the JSON document, public options, or
environment variables used by the shipped web head.

## Internal session chrome configuration

Area visibility is not host-configured: each native area probes its facade and is
either visible, hidden, or visible with an unavailable reason. The web head
configures the session chrome internally so Basic sign-in posts to `auth/login`
and local sign-out posts to `auth/logout` under the Explorer base href.

## See also

- [Running and hosting the Explorer](running-the-explorer.md)
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Multi-replica and failover hosting](multi-replica-hosting.md)
- [`Orleans.Lattice.Explorer.Entra.Web` configuration](../lattice.explorer.entra.web/configuration.md)
