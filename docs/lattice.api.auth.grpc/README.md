# Orleans.Lattice.Api.Auth.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.Auth](../lattice.api.auth/README.md) - projects the membership and authorization-policy admin facade onto a long-lived gRPC service and a public typed client, marshalled with the Orleans binary serializer over code-first request and response records (which wrap the facade DTOs), with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.Auth.Grpc` is the remote transport for the cluster's control plane. Hosts reference it when a remote admin tool, a CLI, or a dashboard needs to administer groups, membership, and authorization rules - and search the identity directory and introspect verdicts - over the network rather than in-process. (The identity directory is search / resolve only; there is no user create/update/delete surface here.)

It provides:

- **A code-first gRPC service.** A unary RPC per facade operation, bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeAuthApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC channel.
- **Shared Orleans marshalling.** Every wire message is a `[GenerateSerializer]` record - either one of this package's own request/response envelopes (for example `AuthGroupRef`, `AuthPutRule`, `AuthAck`) or a facade DTO reused directly (for example `AuthGroupPage`, `AuthExplanation`) - serialized with the Orleans binary serializer, so client and server stay in lock-step by construction.
- **Two-layer, fail-closed authorization.** A transport meta-authorizer gates every RPC at the edge, and the facade's own administrator check re-authorizes the resolved caller. Both default to deny.

Administering authorization is the most sensitive surface in the cluster, so the binding fails closed: with no authorizer registered, every admin call is rejected with `PermissionDenied`.

## Core Properties

- **Public client, internal service.** Callers consume `LatticeAuthApiGrpcClient`; the service, marshallers, and method definitions are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Two load-bearing gates.** The transport meta-authorizer decides whether a call may run at all; the facade's per-call administrator check then re-authorizes the resolved caller's subject. Neither replaces the other.
- **Fail-closed.** Unconfigured, the binding denies every call rather than serving it unauthenticated. An anonymous caller is denied by the facade check even past a permissive transport gate.

## RPCs

The service is exposed under the fully-qualified gRPC service name **`orleans.lattice.api.auth`**. It is a flat set of unary RPCs, one per facade operation. Each RPC carries a request and a response message: most operations wrap their facade arguments in one of this package's own envelope records (see [Request and response envelopes](#request-and-response-envelopes) below), while a few reuse a facade DTO directly.

| RPC | Facade method | Request message | Response message |
|-----|---------------|-----------------|------------------|
| `UpsertGroup` | `UpsertGroupAsync` | `AuthGroup` | `AuthAck` |
| `GetGroup` | `GetGroupAsync` | `AuthGroupRef` | `AuthGroupResult` |
| `RemoveGroup` | `RemoveGroupAsync` | `AuthGroupRef` | `AuthAck` |
| `ListGroups` | `ListGroupsAsync` | `AuthPageRequest` | `AuthGroupPage` |
| `AddMember` | `AddMemberAsync` | `AuthMemberEdge` | `AuthAck` |
| `RemoveMember` | `RemoveMemberAsync` | `AuthMemberEdge` | `AuthAck` |
| `ListGroupMembers` | `ListGroupMembersAsync` | `AuthGroupRef` | `AuthStringList` |
| `ListSubjectGroups` | `ListSubjectGroupsAsync` | `AuthMemberRef` | `AuthStringList` |
| `PutRule` | `PutRuleAsync` | `AuthPutRule` | `AuthAck` |
| `GetRule` | `GetRuleAsync` | `AuthRuleRef` | `AuthRuleResult` |
| `RemoveRule` | `RemoveRuleAsync` | `AuthRuleRef` | `AuthRuleRemoved` |
| `ListRules` | `ListRulesAsync` | `AuthPageRequest` | `AuthRulePage` |
| `ListRulesForTree` | `ListRulesForTreeAsync` | `AuthTreeRulesPage` | `AuthRulePage` |
| `Explain` | `ExplainAsync` | `AuthExplainQuery` | `AuthExplanation` |
| `EffectivePermissions` | `EffectivePermissionsAsync` | `AuthSubjectRef` | `AuthEffectivePermissions` |
| `SearchDirectory` | `SearchDirectoryAsync` | `DirectorySearchRequest` | `DirectorySearchResult` |
| `ResolveDirectoryPrincipal` | `ResolveDirectoryPrincipalAsync` | `AuthPrincipalRef` | `AuthDirectoryPrincipalResult` |
| `GetAccessModel` | `GetAccessModelAsync` | `AuthAccessModelQuery` | `AccessModelDescriptor` |

### Request and response envelopes

The gRPC contract does **not** send the facade method arguments as bare scalars; it wraps them in code-first `[GenerateSerializer]` records (public, in the `Orleans.Lattice.Api.Auth.Grpc` namespace) so the wire shape can version additively. A few RPCs reuse a facade DTO from `Orleans.Lattice.Api.Auth` directly as their message (`AuthGroup`, `AuthPageRequest`, `AuthGroupPage`, `AuthRulePage`, `AuthExplanation`, `AuthEffectivePermissions`, `DirectorySearchRequest`, `DirectorySearchResult`, `AccessModelDescriptor`); the rest use the envelopes below.

Request envelopes:

| Record | Fields |
|---|---|
| `AuthGroupRef` | `GroupId: string` |
| `AuthMemberEdge` | `GroupId: string`, `MemberId: string`, `MemberKind: MembershipMemberKind` (default `User`) |
| `AuthMemberRef` | `MemberId: string` |
| `AuthPutRule` | `Rule: LatticeAuthorizationRule` |
| `AuthRuleRef` | `TreeId: string`, `RuleId: string` |
| `AuthTreeRulesPage` | `TreeId: string`, `Page: AuthPageRequest` |
| `AuthExplainQuery` | `SubjectId: string`, `Operation: LatticeOperation`, `Scope: LatticeScope`, `SubjectKind: LatticeSubjectSelectorKind` (default `User`) |
| `AuthSubjectRef` | `SubjectId: string`, `SubjectKind: LatticeSubjectSelectorKind` (default `User`) |
| `AuthPrincipalRef` | `PrincipalId: string` |
| `AuthAccessModelQuery` | (empty marker) |

Response envelopes:

| Record | Fields |
|---|---|
| `AuthAck` | (empty acknowledgement for write RPCs) |
| `AuthGroupResult` | `Group: AuthGroup?` (null when no such group) |
| `AuthStringList` | `Values: IReadOnlyList<string>` |
| `AuthRuleResult` | `Rule: LatticeAuthorizationRule?` (null when no such rule) |
| `AuthRuleRemoved` | `Removed: bool` |
| `AuthDirectoryPrincipalResult` | `Principal: DirectoryPrincipalDescriptor?` (null when unresolved) |

## Public surface

| Type | Role |
|------|------|
| `LatticeAuthApiGrpcClient` | Public typed client; one method per RPC over a caller-supplied `CallInvoker`. |
| `LatticeAuthApiGrpcOptions` | Server-side options (`RequireAuthorization`, `CredentialHeaderName`, `CredentialScheme`). |
| `ILatticeAuthApiAuthorizer` | Transport meta-authorization seam. |
| `DenyAllAuthApiAuthorizer` | Default-deny authorizer (registered automatically). |
| `AllowAllAuthApiAuthorizer` | Opt-in permissive authorizer for trusted-network use. |
| `LatticeAuthApiAuthorizationContext` | Per-call description handed to the authorizer (operation, target id, call context). |
| `LatticeAuthApiOperation` | Enumerates the operation behind each RPC. |
| `ILatticeAuthApiCredentialBridge` | Identity seam that lifts the inbound credential onto the ambient context. |
| `Auth*` request/response records | Public request and response DTOs for the unary RPCs. |
| `AddLatticeAuthApiGrpc` / `MapLatticeAuthApiGrpc` | Registration and endpoint-routing extensions. |

## Two-layer authorization

Every admin call passes through two independent gates, both fail-closed:

1. **Transport meta-authorizer.** `ILatticeAuthApiAuthorizer` runs first, in a gRPC interceptor scoped to the auth-API service. It defaults to `DenyAllAuthApiAuthorizer`; every call is rejected with `PermissionDenied` until the host registers a permissive authorizer (or the opt-in `AllowAllAuthApiAuthorizer`) or sets `RequireAuthorization` to `false`.
2. **Facade administrator check.** Once past the transport gate, the service stamps the caller identity onto the ambient credential context (via `ILatticeAuthApiCredentialBridge`, `Bearer`-aware by default) and invokes the facade. The facade's own per-call administrator check then runs against the resolved caller's subject. An anonymous caller (no credential) is denied here even when the transport gate allowed the call.

A denial from the facade check is mapped to `PermissionDenied` with response trailers carrying only non-sensitive fields (`lattice-denied-tree`, `lattice-denied-operation`, `lattice-denied-subject`, `lattice-denied-reason`) - never a policy value.

## Quick Start

Register the binding on a silo that already has `AddLatticeAuthApi`, then map its routes:

```csharp verify
using Orleans.Lattice.Api.Auth.Grpc;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeAuthApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeAuthApiAuthorizer, AllowAllAuthApiAuthorizer>();

var app = builder.Build();
app.MapLatticeAuthApiGrpc();
```

The host must expose `ILatticeAuthAdmin` in the same service provider - typically by co-hosting Orleans with `AddLattice(...).AddLatticeAuth(...).AddLatticeAuthApi()` on the same host.

## Client

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Serialization;

var services = new ServiceCollection();
services.AddSerializer();
var serializerProvider = services.BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://admin.example:443");
var authClient = LatticeAuthApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

await authClient.UpsertGroupAsync(new AuthGroup { GroupId = "admins", DisplayName = "Admins" });
await authClient.PutRuleAsync(new AuthPutRule
{
    Rule = new LatticeAuthorizationRule(
        "admins-read-orders",
        LatticeSubjectSelector.Group("admins"),
        LatticeScope.Tree("orders"),
        LatticeOperation.Read,
        LatticeEffect.Allow),
});
var explanation = await authClient.ExplainAsync(new AuthExplainQuery
{
    SubjectId = "alice",
    Operation = LatticeOperation.Read,
    Scope = LatticeScope.Tree("orders"),
});
```

The `serializerProvider` must have Orleans serialization registered (`AddSerializer()`) so the client and server wire marshallers match exactly. Transport concerns (address, TLS, deadlines, retries, call credentials) are configured on the channel the caller supplies. A call the server rejects arrives as a `PermissionDenied` `RpcException`.

`AuthExplainQuery.SubjectKind` (and `AuthSubjectRef.SubjectKind`) select whether `SubjectId` names a user or a group; both default to `LatticeSubjectSelectorKind.User`, so existing messages deserialize unchanged. Set it to `LatticeSubjectSelectorKind.Group` to explain (or resolve the effective permissions of) a group subject - the decision is then evaluated for a principal that is a member of that group and its ancestors, so `group`-scoped rules match exactly as they would for a real member.
