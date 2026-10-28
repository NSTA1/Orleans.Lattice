---
agent_spec: "docs/agents/api/auth.json"
---

# Orleans.Lattice.Api.Auth.Grpc API reference

Every public type and member of `Orleans.Lattice.Api.Auth.Grpc`. The [binding guide](README.md) maps each RPC to its facade method and envelope, explains the two authorization layers and lists the status-code mapping; this page is the complete declaration reference. The service, marshallers, method definitions, default credential bridge and interceptor are internal and are described in [Architecture](architecture.md).

The binding carries the facade's own records - `AuthGroup`, `AuthPageRequest`, `AuthGroupPage`, `AuthRulePage`, `AuthExplanation`, `AuthEffectivePermissions`, `DirectorySearchRequest`, `DirectorySearchResult` and `AccessModelDescriptor` - unchanged; they are listed in the [`Orleans.Lattice.Api.Auth` public API](../lattice.api.auth/api.md).

## Registration

`LatticeAuthApiGrpcServiceCollectionExtensions` registers the server side on an ASP.NET Core host.

| Method | Signature | Purpose |
|---|---|---|
| `AddLatticeAuthApiGrpc` | `IServiceCollection AddLatticeAuthApiGrpc(this IServiceCollection services, Action<LatticeAuthApiGrpcOptions>? configure = null)` | Binds `LatticeAuthApiGrpcOptions`, registers the method definitions and service, `DenyAllAuthApiAuthorizer` as the default `ILatticeAuthApiAuthorizer` and the header-reading default `ILatticeAuthApiCredentialBridge` (both with `TryAdd`, so a host registration made first wins), and adds the authorization interceptor to the gRPC options. Throws `ArgumentNullException` when `services` is null. Each call appends the interceptor again, so call it once. |
| `MapLatticeAuthApiGrpc` | `IEndpointRouteBuilder MapLatticeAuthApiGrpc(this IEndpointRouteBuilder endpoints)` | Maps the unary RPCs of the `orleans.lattice.api.auth` service. The host must have called `AddLatticeAuthApiGrpc` and must expose `ILatticeAuthAdmin` (from `AddLatticeAuthApi`) in the same service provider. Throws `ArgumentNullException` when `endpoints` is null. |

Every option is documented in [Configuration](configuration.md).

## Client

`LatticeAuthApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` builds a client whose wire marshallers come from the Orleans serializers in `serializerProvider`, which must have `AddSerializer()` registered. Both arguments are required. The client exposes one method per RPC, each taking the RPC's request record and an optional `CancellationToken`; it carries no transport policy, so address, TLS, deadlines, retries and call credentials belong to the caller's channel. See [Client](README.md#client).

## Authorization seams

| Type | Role |
|---|---|
| `ILatticeAuthApiAuthorizer` | The transport meta-authorizer: `IsAuthorizedAsync(LatticeAuthApiAuthorizationContext, CancellationToken)` returns `true` to let a call reach the facade. Allowing a call does not grant it; the facade's administrator check still runs. |
| `DenyAllAuthApiAuthorizer` | Returns `false` for every call. The default when the host registers no authorizer. |
| `AllowAllAuthApiAuthorizer` | Returns `true` for every call, leaving all enforcement to the facade. Opt-in, for an endpoint already guarded by an outer boundary. |
| `LatticeAuthApiAuthorizationContext` | The per-call description handed to the authorizer: the raw `ServerCallContext` (`Call`), the decoded `Operation`, and the `TargetId` the call administers (see [Two-layer authorization](README.md#two-layer-authorization) for the id each RPC reports). |
| `LatticeAuthApiOperation` | One value per RPC, plus `Unknown` for a method the interceptor does not recognise, so a deny-by-default authorizer refuses it. |
| `ILatticeAuthApiCredentialBridge` | `Resolve(ServerCallContext)` lifts the caller's credential from the inbound call, or returns `null` for an anonymous caller. Register a replacement before `AddLatticeAuthApiGrpc` for a different identity source, such as a client certificate. |

## Request and response envelopes

The `Auth*` records in the `Orleans.Lattice.Api.Auth.Grpc` namespace wrap facade arguments and results so the wire shape can grow additively. They are immutable `[GenerateSerializer]` records with `init` properties, aliased through the shared `ApiAuthTypeAliases` constants; `AuthAck` and `AuthAccessModelQuery` have no members. The [envelope tables](README.md#request-and-response-envelopes) give each record's fields and defaults.

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Auth.Grpc.ILatticeAuthApiCredentialBridge`

[Source](../../src/lattice.api.auth.grpc/ILatticeAuthApiCredentialBridge.cs) (line 36).

`public interface ILatticeAuthApiCredentialBridge`

- `LatticeCredential? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiGrpcClient`

[Source](../../src/lattice.api.auth.grpc/LatticeAuthApiGrpcClient.cs) (line 24).

`public sealed class LatticeAuthApiGrpcClient`

- `public static LatticeAuthApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public Task<AuthAck> UpsertGroupAsync(AuthGroup request, CancellationToken cancellationToken = default)`
- `public Task<AuthGroupResult> GetGroupAsync(AuthGroupRef request, CancellationToken cancellationToken = default)`
- `public Task<AuthAck> RemoveGroupAsync(AuthGroupRef request, CancellationToken cancellationToken = default)`
- `public Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)`
- `public Task<AuthAck> AddMemberAsync(AuthMemberEdge request, CancellationToken cancellationToken = default)`
- `public Task<AuthAck> RemoveMemberAsync(AuthMemberEdge request, CancellationToken cancellationToken = default)`
- `public Task<AuthStringList> ListGroupMembersAsync(AuthGroupRef request, CancellationToken cancellationToken = default)`
- `public Task<AuthStringList> ListSubjectGroupsAsync(AuthMemberRef request, CancellationToken cancellationToken = default)`
- `public Task<AuthAck> PutRuleAsync(AuthPutRule request, CancellationToken cancellationToken = default)`
- `public Task<AuthRuleResult> GetRuleAsync(AuthRuleRef request, CancellationToken cancellationToken = default)`
- `public Task<AuthRuleRemoved> RemoveRuleAsync(AuthRuleRef request, CancellationToken cancellationToken = default)`
- `public Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)`
- `public Task<AuthRulePage> ListRulesForTreeAsync(AuthTreeRulesPage request, CancellationToken cancellationToken = default)`
- `public Task<AuthExplanation> ExplainAsync(AuthExplainQuery request, CancellationToken cancellationToken = default)`
- `public Task<AuthEffectivePermissions> EffectivePermissionsAsync(AuthSubjectRef request, CancellationToken cancellationToken = default)`
- `public Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)`
- `public Task<AuthDirectoryPrincipalResult> ResolveDirectoryPrincipalAsync(AuthPrincipalRef request, CancellationToken cancellationToken = default)`
- `public Task<AccessModelDescriptor> GetAccessModelAsync(AuthAccessModelQuery request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiGrpcOptions`

[Source](../../src/lattice.api.auth.grpc/LatticeAuthApiGrpcOptions.cs) (line 9).

`public sealed class LatticeAuthApiGrpcOptions`

- `public bool RequireAuthorization { get; set; }`
- `public string CredentialHeaderName { get; set; }`
- `public string CredentialScheme { get; set; }`
- `public string? ActiveTenantHeaderName { get; set; }`

### `Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiGrpcServiceCollectionExtensions`

[Source](../../src/lattice.api.auth.grpc/LatticeAuthApiGrpcServiceCollectionExtensions.cs) (line 36).

`public static class LatticeAuthApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeAuthApiGrpc(this IServiceCollection services, Action<LatticeAuthApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeAuthApiGrpc(this IEndpointRouteBuilder endpoints)`

### `Orleans.Lattice.Api.Auth.Grpc.AuthAccessModelQuery`

[Source](../../src/lattice.api.auth.grpc/Model/AuthAccessModelQuery.cs) (line 12).

`public sealed record AuthAccessModelQuery`

### `Orleans.Lattice.Api.Auth.Grpc.AuthAck`

[Source](../../src/lattice.api.auth.grpc/Model/AuthAck.cs) (line 12).

`public sealed record AuthAck`

### `Orleans.Lattice.Api.Auth.Grpc.AuthDirectoryPrincipalResult`

[Source](../../src/lattice.api.auth.grpc/Model/AuthDirectoryPrincipalResult.cs) (line 14).

`public sealed record AuthDirectoryPrincipalResult`

- `public DirectoryPrincipalDescriptor? Principal { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthExplainQuery`

[Source](../../src/lattice.api.auth.grpc/Model/AuthExplainQuery.cs) (line 13).

`public sealed record AuthExplainQuery`

- `public required string SubjectId { get; init; }`
- `public LatticeSubjectSelectorKind SubjectKind { get; init; }`
- `public LatticeOperation Operation { get; init; }`
- `public required LatticeScope Scope { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthGroupRef`

[Source](../../src/lattice.api.auth.grpc/Model/AuthGroupRef.cs) (line 11).

`public sealed record AuthGroupRef`

- `public required string GroupId { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthGroupResult`

[Source](../../src/lattice.api.auth.grpc/Model/AuthGroupResult.cs) (line 12).

`public sealed record AuthGroupResult`

- `public AuthGroup? Group { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthMemberEdge`

[Source](../../src/lattice.api.auth.grpc/Model/AuthMemberEdge.cs) (line 15).

`public sealed record AuthMemberEdge`

- `public required string GroupId { get; init; }`
- `public required string MemberId { get; init; }`
- `public MembershipMemberKind MemberKind { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthMemberRef`

[Source](../../src/lattice.api.auth.grpc/Model/AuthMemberRef.cs) (line 11).

`public sealed record AuthMemberRef`

- `public required string MemberId { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthPrincipalRef`

[Source](../../src/lattice.api.auth.grpc/Model/AuthPrincipalRef.cs) (line 11).

`public sealed record AuthPrincipalRef`

- `public required string PrincipalId { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthPutRule`

[Source](../../src/lattice.api.auth.grpc/Model/AuthPutRule.cs) (line 12).

`public sealed record AuthPutRule`

- `public required LatticeAuthorizationRule Rule { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthRuleRef`

[Source](../../src/lattice.api.auth.grpc/Model/AuthRuleRef.cs) (line 12).

`public sealed record AuthRuleRef`

- `public required string TreeId { get; init; }`
- `public required string RuleId { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthRuleRemoved`

[Source](../../src/lattice.api.auth.grpc/Model/AuthRuleRemoved.cs) (line 11).

`public sealed record AuthRuleRemoved`

- `public bool Removed { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthRuleResult`

[Source](../../src/lattice.api.auth.grpc/Model/AuthRuleResult.cs) (line 14).

`public sealed record AuthRuleResult`

- `public LatticeAuthorizationRule? Rule { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthStringList`

[Source](../../src/lattice.api.auth.grpc/Model/AuthStringList.cs) (line 12).

`public sealed record AuthStringList`

- `public IReadOnlyList<string> Values { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthSubjectRef`

[Source](../../src/lattice.api.auth.grpc/Model/AuthSubjectRef.cs) (line 12).

`public sealed record AuthSubjectRef`

- `public required string SubjectId { get; init; }`
- `public LatticeSubjectSelectorKind SubjectKind { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.AuthTreeRulesPage`

[Source](../../src/lattice.api.auth.grpc/Model/AuthTreeRulesPage.cs) (line 11).

`public sealed record AuthTreeRulesPage`

- `public required string TreeId { get; init; }`
- `public AuthPageRequest Page { get; init; }`

### `Orleans.Lattice.Api.Auth.Grpc.ILatticeAuthApiAuthorizer`

[Source](../../src/lattice.api.auth.grpc/Security/ILatticeAuthApiAuthorizer.cs) (line 18).

`public interface ILatticeAuthApiAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeAuthApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Auth.Grpc.DenyAllAuthApiAuthorizer`

[Source](../../src/lattice.api.auth.grpc/Security/ILatticeAuthApiAuthorizer.cs) (line 46).

`public sealed class DenyAllAuthApiAuthorizer : ILatticeAuthApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeAuthApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Auth.Grpc.AllowAllAuthApiAuthorizer`

[Source](../../src/lattice.api.auth.grpc/Security/ILatticeAuthApiAuthorizer.cs) (line 62).

`public sealed class AllowAllAuthApiAuthorizer : ILatticeAuthApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeAuthApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiOperation`

[Source](../../src/lattice.api.auth.grpc/Security/LatticeAuthApiAuthorizationContext.cs) (line 12).

`public enum LatticeAuthApiOperation`

- `UpsertGroup`
- `GetGroup`
- `RemoveGroup`
- `ListGroups`
- `AddMember`
- `RemoveMember`
- `ListGroupMembers`
- `ListSubjectGroups`
- `PutRule`
- `GetRule`
- `RemoveRule`
- `ListRules`
- `ListRulesForTree`
- `Explain`
- `EffectivePermissions`
- `SearchDirectory`
- `ResolveDirectoryPrincipal`
- `GetAccessModel`
- `Unknown`

### `Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiAuthorizationContext`

[Source](../../src/lattice.api.auth.grpc/Security/LatticeAuthApiAuthorizationContext.cs) (line 86).

`public readonly struct LatticeAuthApiAuthorizationContext`

- `public LatticeAuthApiAuthorizationContext(ServerCallContext call, LatticeAuthApiOperation operation, string? targetId)`
- `public ServerCallContext Call { get; }`
- `public LatticeAuthApiOperation Operation { get; }`
- `public string? TargetId { get; }`

## Related

- [Binding guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Api.Auth` public API](../lattice.api.auth/api.md) - the facade contract and records this binding carries.
