# Orleans.Lattice.Api.Auth.Grpc

Code-first gRPC transport binding for
[Orleans.Lattice.Api.Auth](https://www.nuget.org/packages/Orleans.Lattice.Api.Auth).
Projects the membership and authorization-policy admin facade onto a flat set of
unary gRPC RPCs so a remote admin tool, CLI, or dashboard can administer users,
groups, membership, and rules - and introspect verdicts - over the wire.

Administering authorization is the most sensitive surface in the cluster, so the
binding fails closed: with no authorizer registered, every admin call is
rejected with `PermissionDenied`.

## RPCs

| RPC | Facade method |
|-----|---------------|
| `UpsertUser` | `UpsertUserAsync` |
| `GetUser` | `GetUserAsync` |
| `RemoveUser` | `RemoveUserAsync` |
| `ListUsers` | `ListUsersAsync` |
| `UpsertGroup` | `UpsertGroupAsync` |
| `GetGroup` | `GetGroupAsync` |
| `RemoveGroup` | `RemoveGroupAsync` |
| `ListGroups` | `ListGroupsAsync` |
| `AddMember` | `AddMemberAsync` |
| `RemoveMember` | `RemoveMemberAsync` |
| `ListGroupMembers` | `ListGroupMembersAsync` |
| `ListSubjectGroups` | `ListSubjectGroupsAsync` |
| `PutRule` | `PutRuleAsync` |
| `GetRule` | `GetRuleAsync` |
| `RemoveRule` | `RemoveRuleAsync` |
| `ListRules` | `ListRulesAsync` |
| `ListRulesForTree` | `ListRulesForTreeAsync` |
| `Explain` | `ExplainAsync` |
| `EffectivePermissions` | `EffectivePermissionsAsync` |

## Two-layer authorization

Every admin call passes through two independent gates, both fail-closed:

1. **Transport meta-authorizer.** The `ILatticeAuthApiAuthorizer` coarse gate
   runs at the edge in a gRPC interceptor. It defaults to
   `DenyAllAuthApiAuthorizer`; every call is rejected with `PermissionDenied`
   until the host registers a permissive authorizer (or the opt-in
   `AllowAllAuthApiAuthorizer`) or turns `RequireAuthorization` off.
2. **Facade administrator check.** Once past the transport gate, the service
   stamps the caller identity onto the ambient credential context (via a
   header-based `ILatticeAuthApiCredentialBridge`, `Bearer`-aware by default) and
   invokes the facade. The facade's own per-call administrator check then runs
   against the resolved caller's subject. An anonymous caller (no credential) is
   denied here even when the transport gate allowed the call.

A denial from the facade check is mapped to `PermissionDenied` with response
trailers carrying only non-sensitive fields (`lattice-denied-tree`,
`lattice-denied-operation`, `lattice-denied-subject`, `lattice-denied-reason`) -
never a policy value.

## Server wiring

```csharp
builder.Services.AddLatticeAuthApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeAuthApiAuthorizer, MyTokenAuthorizer>();
// ... app build ...
app.MapLatticeAuthApiGrpc();
```

The host must expose `ILatticeAuthAdmin` in the same service provider - typically
by co-hosting Orleans with `AddLattice(...).AddLatticeAuth(...).AddLatticeAuthApi()`.

## Client

```csharp
var channel = GrpcChannel.ForAddress("https://admin.example:443");
var client = LatticeAuthApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);
await client.PutRuleAsync(new AuthPutRule { Rule = rule });
var explanation = await client.ExplainAsync(new AuthExplainQuery { SubjectId = "alice", Operation = LatticeOperation.Read, Scope = scope });
```

The `serializerProvider` must have Orleans serialization registered
(`AddSerializer()`) so the client and server wire marshallers match exactly.
Transport concerns (address, TLS, deadlines, retries, call credentials) are
configured on the channel the caller supplies.
