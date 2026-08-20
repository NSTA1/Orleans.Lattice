# Orleans.Lattice.Api.State configuration

The state facade and its gRPC binding expose three public options types. `LatticeApiStateOptions` carries the read-bounding knobs the read-only cluster state facade honours and is bound through `AddLatticeStateApi`. `LatticeStateApiGrpcOptions` carries the server-side gRPC binding knobs and is bound through `AddLatticeStateApiGrpc`. `EnvVarCredentialAuthorizerOptions` carries the reference environment-variable credential authorizer knobs and is bound through `AddEnvVarCredentialAuthorizer`.

## `LatticeApiStateOptions`

Bounds every read the read-only state API serves so a single request cannot page an unbounded catalog or put whole values on the wire. Bind it through `AddLatticeStateApi(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultScanPageSize` | `int` | `100` | Page size used for an entry scan when the request leaves its page size unset (`0` or negative). |
| `MaxScanPageSize` | `int` | `1000` | Largest entry-scan page size honoured; larger requested page sizes are clamped down. |
| `DefaultScanValuePreviewBytes` | `int` | `256` | Value-preview byte budget used for an entry scan when the request leaves the budget unset (`0` or negative). Keeps whole values off the wire during a list scan. |
| `MaxScanValuePreviewBytes` | `int` | `65536` (`64 * 1024`) | Largest value-preview byte budget honoured for an entry scan; larger requested budgets are clamped down. |
| `SingleEntryValuePreviewBytes` | `int` | `1048576` (`1024 * 1024`) | Value-preview byte budget for a single-key detail read. Larger than the scan budget because a detail pane shows one entry at a time. |
| `DefaultHistoryPageSize` | `int` | `100` | Page size used for a per-key history read when the request leaves its limit unset (`0` or negative). |
| `MaxHistoryPageSize` | `int` | `1000` | Largest per-key history page size honoured; larger requested limits are clamped down. |
| `DefaultHistoryValuePreviewBytes` | `int` | `256` | Per-revision value / delta preview byte budget for a per-key history read when the request leaves the budget unset (`0` or negative). The durable history substrate already clips stored previews to a fixed per-revision ceiling, so a larger budget cannot recover more bytes than were retained. |
| `MaxHistoryValuePreviewBytes` | `int` | `256` | Largest per-revision value / delta preview byte budget honoured for a per-key history read; larger requested budgets are clamped down. This equals the per-revision ceiling the history substrate stores. |
| `ChangeObservationPollInterval` | `TimeSpan` | `250ms` | How long a change-observation subscription waits before re-polling the write-ahead-log tail once it has drained all currently-available changes. Lower values reduce notification latency at the cost of more idle WAL reads. |
| `ChangeObservationPageSize` | `int` | `256` | Maximum number of write-ahead-log entries read per partition per drain cycle by a change-observation subscription. Bounds the work and memory of a single catch-up read. |
| `MetricsSampleInterval` | `TimeSpan` | `1s` | Default cadence at which the metadata / metrics observation feed samples per-tree aggregates when a request does not override it. Because the feed samples already-maintained aggregates on a timer, this trades dashboard-gauge freshness against sampling cost. |
| `ReadVisibility` | `LatticeStateApiReadVisibility` | `Auto` | Whether the state API filters every read through the data-plane access gate using the caller's resolved subject, so it never returns data or catalog / structure metadata the caller lacks read permission for. |

### `LatticeStateApiReadVisibility`

The authorization posture selected by `ReadVisibility`.

| Value | Meaning |
|---|---|
| `Auto` | Auto-detect: auth-backed visibility is on when a real access gate is registered (the `Orleans.Lattice.Auth` add-on) and off otherwise. The default and recommended posture. |
| `Enforced` | Force auth-backed visibility on. Identical to `Auto` in practice, because visibility filtering still requires a real access gate to have anything to enforce; provided so a deployment can make the intent explicit. |
| `Disabled` | Turn auth-backed visibility off even when a real access gate is registered. The state API then performs no per-tree read filtering and no caller-subject resolution, restoring the pre-authorization behaviour. Intended for trusted-network deployments where an outer boundary already governs who may read cluster state. |

## `LatticeStateApiGrpcOptions`

Server-side options for the code-first gRPC binding. Bind them through `AddLatticeStateApiGrpc(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the authorization interceptor enforces `ILatticeStateApiAuthorizer` on inbound protected state-API calls. The binding fails closed by default; set this to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `"authorization"` | Inbound request-header (gRPC metadata) name that carries the caller credential token bridged into the ambient Lattice credential. |
| `CredentialScheme` | `string` | `"Bearer"` | Authentication scheme stamped on the bridged `LatticeCredential`; a matching case-insensitive scheme prefix on the header value is stripped before the token is used. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | Empty `List<AuthSchemeDescriptor>` | Auth schemes advertised from the unauthenticated `GetAuthScheme` RPC, in preference order. Populate with public configuration only - never secrets. |

## `EnvVarCredentialAuthorizerOptions`

Options for the reference environment-variable credential authorizer. Bind them through `AddEnvVarCredentialAuthorizer(configure)`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `EnvironmentVariablePrefix` | `string` | `"LATTICE_STATE_USER_"` | Prefix prepended to a username to form the environment-variable name that holds that user's encoded password hash. |
| `MaxFailedAttempts` | `int` | `5` | Number of consecutive failed authentication attempts for a single username that triggers temporary lockout. |
| `LockoutDuration` | `TimeSpan` | `1 minute` | How long a username stays locked out after `MaxFailedAttempts` consecutive failures; attempts are denied during the lockout even with the correct password. |
