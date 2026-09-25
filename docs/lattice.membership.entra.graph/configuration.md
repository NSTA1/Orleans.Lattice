# Orleans.Lattice.Membership.Entra.Graph configuration

The package has one public options type, `LatticeEntraGraphOptions`, which configures the Microsoft Graph-backed group resolver: the Entra application credentials it authenticates with, the Graph scopes it requests, and how it shapes the transitive-group query. It is bound by the `AddEntraGraphGroupResolver` registration extension. The app-only access token is acquired and refreshed transparently, so operators never manage a Graph token directly.

Two mutually exclusive authentication modes are supported. By default the resolver uses the confidential-client path, authenticating app-only with the `TenantId`, `ClientId`, and `ClientSecret` triple. Alternatively, supplying a `Credential` selects a secret-less path where the resolver authenticates app-only with that token credential (for example a federated managed identity) and no client secret is used. Supplying both a `Credential` and a `ClientSecret` is rejected as ambiguous.

## `LatticeEntraGraphOptions`

Bind it through `AddEntraGraphGroupResolver(configure)`.

### Constants

| Constant | Type | Value | Meaning |
|---|---|---|---|
| `DefaultAuthorityHost` | `string` | `"https://login.microsoftonline.com"` | The default Entra login host used to build the token authority. |
| `DefaultScope` | `string` | `"https://graph.microsoft.com/.default"` | The default Graph scope for app-only access. |

### Properties

| Property | Type | Default | Meaning |
|---|---|---|---|
| `TenantId` | `string` | `""` (empty) | The tenant id the app-only Graph token is issued for. Required for the confidential-client (client-secret) path; ignored when `Credential` is set. |
| `ClientId` | `string` | `""` (empty) | The Entra application (client) id used to acquire the Graph token. Required for the confidential-client path; ignored when `Credential` is set. |
| `ClientSecret` | `string` | `""` (empty) | The Entra application client secret used to acquire the Graph token. Required for the confidential-client path; must be left unset when `Credential` is used. |
| `Credential` | `TokenCredential?` | `null` | An optional Azure token credential that selects the secret-less authentication path. When set, the resolver authenticates app-only with this credential (for example `DefaultAzureCredential` or a `ManagedIdentityCredential`) and no `ClientSecret` is used. Mutually exclusive with the client-secret path. `null` selects the confidential-client path. |
| `AuthorityHost` | `string` | `DefaultAuthorityHost` | The Entra login host, combined with `TenantId` to form the MSAL authority. Used only on the confidential-client path. |
| `Scopes` | `IList<string>` | `["https://graph.microsoft.com/.default"]` (the single `DefaultScope`) | The Graph scopes requested for the app-only token, on either authentication path. Must contain at least one scope, and no null or empty entry. Populate the collection in place. |
| `SecurityEnabledOnly` | `bool` | `false` | Whether the transitive-group query returns only security-enabled groups. `false` returns all groups and directory roles. |
| `TokenRefreshSkew` | `TimeSpan` | `5 minutes` | How long before the token's actual expiry it is proactively refreshed, so a call never uses a token that expires mid-flight. Must not be negative. Applies to the confidential-client path; with `Credential` set, token lifetime is left to that credential. |
| `DirectorySubjectIdSource` | `EntraDirectorySubjectIdSource` | `ObjectId` | Which Entra identifier the Graph-backed identity directory records as a directory principal id, so directory validation matches the active authenticator's subject claim. `ObjectId` (the `oid`) aligns with a typical Entra deployment whose subject claim resolves to the object id; `UserPrincipalName` records a user's user principal name instead (a group always records its object id). |

### Methods

| Method | Returns | Meaning |
|---|---|---|
| `ResolveAuthority()` | `string` | The MSAL authority, `AuthorityHost` joined to `TenantId`; an empty string when either is missing. |
