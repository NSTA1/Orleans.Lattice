# Orleans.Lattice.Explorer configuration

The Explorer libraries expose four primary configuration and hosting options types, documented here. `ExplorerConfigStoreOptions` is bound by the `AddExplorerConfiguration` registration extension. `LatticeExplorerWebOptions` is bound by the `AddLatticeExplorerWeb` extension (and read back by `MapLatticeExplorer`). `ExplorerNavigationOptions` and `ExplorerAuthUiOptions` are head-level options registered as DI singletons by a head; the web head derives both from `LatticeExplorerWebOptions` (the schema-area flag and the server-form-post login flow respectively), and when no instance is registered each type falls back to its own defaults. The authentication layer additionally exposes the public `ExplorerReauthOptions`, `ExplorerSignOutOptions`, and `ExplorerContentSecurityPolicyOptions`, which a sign-in provider package configures (see [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md) and the hosted-web Entra provider) rather than a host binding them directly.

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

Head-level options controlling which registered areas the app-level switcher surfaces, and whether the console offers the interactive connection-settings affordance. Every area stays registered and its services stay wired; this only decides whether an opt-in area is shown. A head registers an instance in DI; when none is registered the switcher falls back to the defaults here.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `EnableSchemaArea` | `bool` | `false` | When `true`, the schema-management area is shown in the switcher. When `false`, the area is hidden: its tab is not rendered and it cannot be activated, though the schema control services stay registered so it can be re-surfaced without new wiring. Withheld by default because its versioning UI cannot yet express what differs between schema versions. |
| `AllowEndpointConfiguration` | `bool` | `true` | When `true`, the navigation panel renders the connection-settings affordance that opens the endpoint configuration dialog. When `false`, the affordance is withheld and the dialog cannot be opened. The desktop head, which is inherently single-user, leaves this at the default; the web head derives it from `LatticeExplorerWebOptions.AllowInteractiveEndpointConfiguration`, which is `false` by default. This is a UI affordance only - the authoritative refusal lives in the configuration store, so hiding the button is not the security boundary. |

## `ExplorerAuthUiOptions`

Per-head options controlling how the shared login / logout UI submits credentials. The desktop head signs in fully in-process; the web head instead posts to a server endpoint so the password never crosses the SignalR circuit and is stored in an encrypted, `HttpOnly` server cookie. A head registers an instance in DI; when none is registered the UI defaults to the in-process desktop flow.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `UseServerFormPost` | `bool` | `false` | When `true`, the login dialog renders a native HTML form that POSTs to `LoginPath` (the web head). When `false`, the dialog signs in in-process (the desktop head). |
| `LoginPath` | `string` | `"/auth/login"` | The server path the login form posts to when `UseServerFormPost` is set. |
| `LogoutPath` | `string` | `"/auth/logout"` | The server path the logout form posts to when `UseServerFormPost` is set. |

## `LatticeExplorerWebOptions`

Options controlling how the embeddable Explorer web head is registered and mapped. An instance is registered in DI by `AddLatticeExplorerWeb` and read back by `MapLatticeExplorer` and the host document component, so the two calls agree on the mount point.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `BasePath` | `string` | `"/"` | The base path the explorer is mounted under, for example `/explorer`. Defaults to `/` (mounted at the application root). A value is normalized on assignment to a single leading slash with no trailing slash (the root stays `/`). |
| `ConfigFilePath` | `string?` | `null` | An explicit path for the explorer's JSON configuration backing store. When `null`, the store falls back to the `LATTICE_EXPLORER_CONFIG` environment variable, then to the per-user app-data default. |
| `UseEnvironmentBootstrap` | `bool` | `true` | When `true`, the launcher-friendly environment bootstrap is registered, seeding the first-run endpoint from process environment variables when nothing is persisted yet. The credential half of that bootstrap is withheld on the web head unless `AllowEnvironmentCredentialSeed` is also set. |
| `AllowEnvironmentCredentialSeed` | `bool` | `false` | When `false` (the default), the environment bootstrap seeds only the secret-free endpoint and the sign-in credential read from the environment is never made available to a browser circuit. When `true`, an unauthenticated visitor's circuit is signed in with that credential and inherits the operator's grant, so enable it only on a host reachable by exactly one trusted operator. Ignored when `UseEnvironmentBootstrap` is `false`. |
| `AllowInteractiveEndpointConfiguration` | `bool` | `false` | When `false` (the default), the shared configuration store refuses writes and the connection-settings affordance is withheld, so a visitor cannot repoint the deployment's cluster endpoint. Configure the endpoint with `ConfigFilePath` or the environment bootstrap instead. Set to `true` only for a web head that is genuinely configured through its own UI by a trusted operator. |
| `EnableSchemaArea` | `bool` | `false` | When `true`, the schema-management area is surfaced in the Explorer's area switcher. When `false`, the area is hidden: its tab is not rendered and it cannot be activated, though the schema control services stay registered so it can be re-surfaced by flipping this flag. Withheld by default because its versioning UI cannot yet express what differs between schema versions. |
| `DataProtectionKeyRingBlobUri` | `Uri?` | `null` | When set, the ASP.NET Data Protection key ring is persisted to this Azure Blob Storage blob (for example `https://account.blob.core.windows.net/keys/explorer-keyring.xml`) instead of the default per-instance ephemeral ring, so every replica shares one key ring and can decrypt the OpenID Connect session cookie any other replica issued. Required for a multi-replica / failover deployment; leave `null` for single-instance behaviour. `DataProtectionKeyRingCredential` must be supplied when this is set. See [Multi-replica and failover hosting](multi-replica-hosting.md). |
| `DataProtectionKeyRingCredential` | `TokenCredential?` | `null` | The `Azure.Core.TokenCredential` used to authenticate to the key-ring blob named by `DataProtectionKeyRingBlobUri` (for example a `DefaultAzureCredential` or a managed-identity credential). Required when `DataProtectionKeyRingBlobUri` is set; ignored otherwise. |
| `DataProtectionApplicationName` | `string?` | `null` | Sets the Data Protection application-discriminator name. Every replica that must decrypt one another's cookies has to share the same value, so set a stable, deployment-wide name (for example `lattice-explorer`) when persisting the key ring to shared storage. When `null`, the framework default (content-root-derived) discriminator is used. |
| `ConfigureDataProtection` | `Action<IDataProtectionBuilder>?` | `null` | Optional escape hatch invoked with the Data Protection builder after the built-in persistence and application-name configuration is applied, so a host can attach additional configuration (a different key store, key encryption at rest, a custom key lifetime). Runs whether or not the blob-persistence options above are set. |

Setting `DataProtectionKeyRingBlobUri` without `DataProtectionKeyRingCredential` throws `InvalidOperationException` at registration time (fail-closed): a half-configured shared key ring wedges every operator at the first failover, so the misconfiguration is surfaced loudly rather than falling back silently to the ephemeral ring.
