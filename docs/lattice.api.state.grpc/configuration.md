---
agent_spec: "docs/agents/api/state.json"
---

# Configuration

The binding has two options types. `LatticeStateApiGrpcOptions`, bound by `AddLatticeStateApiGrpc(configure)`, configures the server side of the binding: whether the transport authorizer runs, which request headers the default identity bridges read, and what the unauthenticated `GetAuthScheme` RPC advertises. `EnvVarCredentialAuthorizerOptions`, bound by `AddEnvVarCredentialAuthorizer(configure)`, configures the optional turnkey credential authorizer. The client has no options; its transport policy lives on the `GrpcChannel` or `CallInvoker` passed to `LatticeStateApiGrpcClient.Create`.

The state facade's own read bounds, change-feed cadence, metrics cadence and read-visibility posture are `LatticeApiStateOptions`, documented in the [state facade configuration](../lattice.api.state/configuration.md). No options validator is registered for either type in this package.

## `LatticeStateApiGrpcOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor consults `ILatticeStateApiAuthorizer` before every protected state-API RPC. Default-deny: with the default `DenyAllStateApiAuthorizer`, every protected call is refused with `PermissionDenied`. `GetAuthScheme` is never gated. Set `false` only when an outer authentication boundary already guards the endpoint. Read per call through the options monitor. |
| `CredentialHeaderName` | `string` | `"authorization"` | Request-metadata header the default credential bridge reads; the lookup is case-insensitive. An empty value disables the bridge, leaving every caller anonymous - which auth-backed read visibility denies. |
| `CredentialScheme` | `string` | `"Bearer"` | Scheme stamped on the bridged `LatticeCredential`. A matching case-insensitive scheme prefix on the header value is stripped before the token is used. An empty value disables prefix stripping and stamps no scheme. |
| `ActiveTenantHeaderName` | `string` | `"lattice-active-tenant"` (`LatticeActiveTenantAssertion.DefaultHeaderName`) | Request-metadata header carrying the caller's asserted active tenant, stamped onto the ambient active-tenant scope for the call. An absent, blank or syntactically invalid value asserts no tenant. The assertion is re-validated against the caller's membership by the tenancy add-on and grants no access. Set to `null` or empty to disable header-based tenant selection. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty list | Auth schemes returned by the unauthenticated `GetAuthScheme` RPC, in preference order. Get-only: add descriptors to the existing list. Because the advertisement is served without a credential, populate it with public configuration only - never secrets. Read per call through the options monitor. |

## `EnvVarCredentialAuthorizerOptions`

These options apply only after `AddEnvVarCredentialAuthorizer` has replaced the registered authorizer with `EnvVarCredentialAuthorizer`. They are read per call through the options monitor.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `EnvironmentVariablePrefix` | `string` | `"LATTICE_STATE_USER_"` | Prefix prepended to a username to form the environment-variable name holding that user's encoded password hash. |
| `MaxFailedAttempts` | `int` | `5` | Consecutive failed attempts for one configured username that trigger a temporary lockout. The counter resets on a successful attempt and when a lockout begins. |
| `LockoutDuration` | `TimeSpan` | `1 minute` | How long a username stays locked out; attempts are denied during the lockout even with the correct password. |

The authorizer always reads the `authorization` header (it does not use `CredentialHeaderName`) and accepts only a `Basic base64(username:password)` value. A username must start with an ASCII letter or underscore and contain only ASCII letters, digits and underscores, so it forms a valid environment-variable name. Failed-attempt tracking matches usernames case-insensitively on Windows and case-sensitively elsewhere, mirroring how the process environment resolves variable names. Unknown usernames spend the same hashing cost as known ones and are not tracked, so neither response timing nor memory growth reveals which users exist. Stored hashes use the `LatticePasswordHash` format; `LatticePasswordHash.DefaultIterations` is `750000`.

## Registration-time seams

| Seam | Registered by | Default | Replacement rule |
|---|---|---|---|
| `ILatticeStateApiAuthorizer` | `AddLatticeStateApiGrpc` | `DenyAllStateApiAuthorizer` (`TryAdd`) | A host registration made earlier is kept. `AddEnvVarCredentialAuthorizer` removes every existing registration and installs `EnvVarCredentialAuthorizer`. |
| `ILatticeStateApiCredentialBridge` | `AddLatticeStateApiGrpc` | header bridge using `CredentialHeaderName` and `CredentialScheme` (`TryAdd`) | Register a replacement first to source identity elsewhere. |
| `ILatticeStateApiAuthSchemeSource` | `AddLatticeStateApiGrpc` | source backed by `AdvertisedAuthSchemes` (`TryAdd`) | Register a replacement first; it must return public configuration only. |
| `IEnvironmentVariableReader` | `AddEnvVarCredentialAuthorizer` | `ProcessEnvironmentVariableReader` (`TryAdd`) | Register a replacement first to read hashes from another store. |

`AddLatticeStateApiGrpc` also appends the authorization interceptor to the host's gRPC pipeline - on every call, so call it once. It acts only on methods under the `orleans.lattice.api.state` service, so other gRPC services on the same host are unaffected.

## Source map

- [Server options](../../src/lattice.api.state.grpc/LatticeStateApiGrpcOptions.cs)
- [Registration](../../src/lattice.api.state.grpc/LatticeStateApiGrpcServiceCollectionExtensions.cs)
- [Credential authorizer options](../../src/lattice.api.state.grpc/Security/EnvVarCredentialAuthorizerOptions.cs)
- [Credential authorizer registration](../../src/lattice.api.state.grpc/EnvVarCredentialAuthorizerServiceCollectionExtensions.cs)
- [Credential authorizer](../../src/lattice.api.state.grpc/Security/EnvVarCredentialAuthorizer.cs)
- [Password-hash format](../../src/lattice.api.state.grpc/Security/LatticePasswordHash.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [State facade configuration](../lattice.api.state/configuration.md)
- [Security](../lattice.api.state/security.md)
