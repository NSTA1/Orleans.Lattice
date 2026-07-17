using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// membership and authorization-policy control API. Each method is a unary RPC
/// over an Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the client invoker and the server-side binder wire up
/// identical marshallers.
/// </summary>
/// <remarks>
/// The contract is a flat set of unary RPCs mirroring the transport-agnostic
/// facade: membership CRUD (users, groups, members), policy CRUD (rules), and
/// policy introspection (explain, effective permissions). Contract-versioning
/// policy: fields on the wire messages are additive-only (new <c>[Id(n)]</c>);
/// aliases and field numbers are never renumbered, so a newer response decodes
/// cleanly under an older client.
/// </remarks>
internal sealed class LatticeAuthApiGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.auth";

    /// <summary>The unary user-upsert RPC method name.</summary>
    public const string UpsertUserMethodName = "UpsertUser";

    /// <summary>The unary user-get RPC method name.</summary>
    public const string GetUserMethodName = "GetUser";

    /// <summary>The unary user-remove RPC method name.</summary>
    public const string RemoveUserMethodName = "RemoveUser";

    /// <summary>The unary user-list RPC method name.</summary>
    public const string ListUsersMethodName = "ListUsers";

    /// <summary>The unary group-upsert RPC method name.</summary>
    public const string UpsertGroupMethodName = "UpsertGroup";

    /// <summary>The unary group-get RPC method name.</summary>
    public const string GetGroupMethodName = "GetGroup";

    /// <summary>The unary group-remove RPC method name.</summary>
    public const string RemoveGroupMethodName = "RemoveGroup";

    /// <summary>The unary group-list RPC method name.</summary>
    public const string ListGroupsMethodName = "ListGroups";

    /// <summary>The unary add-member RPC method name.</summary>
    public const string AddMemberMethodName = "AddMember";

    /// <summary>The unary remove-member RPC method name.</summary>
    public const string RemoveMemberMethodName = "RemoveMember";

    /// <summary>The unary list-group-members RPC method name.</summary>
    public const string ListGroupMembersMethodName = "ListGroupMembers";

    /// <summary>The unary list-subject-groups RPC method name.</summary>
    public const string ListSubjectGroupsMethodName = "ListSubjectGroups";

    /// <summary>The unary rule-put RPC method name.</summary>
    public const string PutRuleMethodName = "PutRule";

    /// <summary>The unary rule-get RPC method name.</summary>
    public const string GetRuleMethodName = "GetRule";

    /// <summary>The unary rule-remove RPC method name.</summary>
    public const string RemoveRuleMethodName = "RemoveRule";

    /// <summary>The unary rule-list RPC method name.</summary>
    public const string ListRulesMethodName = "ListRules";

    /// <summary>The unary list-rules-for-tree RPC method name.</summary>
    public const string ListRulesForTreeMethodName = "ListRulesForTree";

    /// <summary>The unary explain RPC method name.</summary>
    public const string ExplainMethodName = "Explain";

    /// <summary>The unary effective-permissions RPC method name.</summary>
    public const string EffectivePermissionsMethodName = "EffectivePermissions";

    /// <summary>The unary directory-search RPC method name.</summary>
    public const string SearchDirectoryMethodName = "SearchDirectory";

    /// <summary>The unary directory-principal-resolve RPC method name.</summary>
    public const string ResolveDirectoryPrincipalMethodName = "ResolveDirectoryPrincipal";

    /// <summary>The unary access-model-read RPC method name.</summary>
    public const string GetAccessModelMethodName = "GetAccessModel";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeAuthApiGrpcMethods(
        Serializer<AuthUser> userSerializer,
        Serializer<AuthUserRef> userRefSerializer,
        Serializer<AuthUserResult> userResultSerializer,
        Serializer<AuthUserPage> userPageSerializer,
        Serializer<AuthGroup> groupSerializer,
        Serializer<AuthGroupRef> groupRefSerializer,
        Serializer<AuthGroupResult> groupResultSerializer,
        Serializer<AuthGroupPage> groupPageSerializer,
        Serializer<AuthPageRequest> pageRequestSerializer,
        Serializer<AuthMemberRef> memberRefSerializer,
        Serializer<AuthMemberEdge> memberEdgeSerializer,
        Serializer<AuthStringList> stringListSerializer,
        Serializer<AuthPutRule> putRuleSerializer,
        Serializer<AuthRuleRef> ruleRefSerializer,
        Serializer<AuthRuleResult> ruleResultSerializer,
        Serializer<AuthRuleRemoved> ruleRemovedSerializer,
        Serializer<AuthRulePage> rulePageSerializer,
        Serializer<AuthTreeRulesPage> treeRulesPageSerializer,
        Serializer<AuthExplainQuery> explainQuerySerializer,
        Serializer<AuthExplanation> explanationSerializer,
        Serializer<AuthSubjectRef> subjectRefSerializer,
        Serializer<AuthEffectivePermissions> effectivePermissionsSerializer,
        Serializer<AuthAck> ackSerializer,
        Serializer<DirectorySearchRequest> directorySearchRequestSerializer,
        Serializer<DirectorySearchResult> directorySearchResultSerializer,
        Serializer<AuthPrincipalRef> principalRefSerializer,
        Serializer<AuthDirectoryPrincipalResult> directoryPrincipalResultSerializer,
        Serializer<AuthAccessModelQuery> accessModelQuerySerializer,
        Serializer<AccessModelDescriptor> accessModelSerializer)
    {
        ArgumentNullException.ThrowIfNull(userSerializer);
        ArgumentNullException.ThrowIfNull(userRefSerializer);
        ArgumentNullException.ThrowIfNull(userResultSerializer);
        ArgumentNullException.ThrowIfNull(userPageSerializer);
        ArgumentNullException.ThrowIfNull(groupSerializer);
        ArgumentNullException.ThrowIfNull(groupRefSerializer);
        ArgumentNullException.ThrowIfNull(groupResultSerializer);
        ArgumentNullException.ThrowIfNull(groupPageSerializer);
        ArgumentNullException.ThrowIfNull(pageRequestSerializer);
        ArgumentNullException.ThrowIfNull(memberRefSerializer);
        ArgumentNullException.ThrowIfNull(memberEdgeSerializer);
        ArgumentNullException.ThrowIfNull(stringListSerializer);
        ArgumentNullException.ThrowIfNull(putRuleSerializer);
        ArgumentNullException.ThrowIfNull(ruleRefSerializer);
        ArgumentNullException.ThrowIfNull(ruleResultSerializer);
        ArgumentNullException.ThrowIfNull(ruleRemovedSerializer);
        ArgumentNullException.ThrowIfNull(rulePageSerializer);
        ArgumentNullException.ThrowIfNull(treeRulesPageSerializer);
        ArgumentNullException.ThrowIfNull(explainQuerySerializer);
        ArgumentNullException.ThrowIfNull(explanationSerializer);
        ArgumentNullException.ThrowIfNull(subjectRefSerializer);
        ArgumentNullException.ThrowIfNull(effectivePermissionsSerializer);
        ArgumentNullException.ThrowIfNull(ackSerializer);
        ArgumentNullException.ThrowIfNull(directorySearchRequestSerializer);
        ArgumentNullException.ThrowIfNull(directorySearchResultSerializer);
        ArgumentNullException.ThrowIfNull(principalRefSerializer);
        ArgumentNullException.ThrowIfNull(directoryPrincipalResultSerializer);
        ArgumentNullException.ThrowIfNull(accessModelQuerySerializer);
        ArgumentNullException.ThrowIfNull(accessModelSerializer);

        UpsertUser = Unary(UpsertUserMethodName, userSerializer, ackSerializer);
        GetUser = Unary(GetUserMethodName, userRefSerializer, userResultSerializer);
        RemoveUser = Unary(RemoveUserMethodName, userRefSerializer, ackSerializer);
        ListUsers = Unary(ListUsersMethodName, pageRequestSerializer, userPageSerializer);

        UpsertGroup = Unary(UpsertGroupMethodName, groupSerializer, ackSerializer);
        GetGroup = Unary(GetGroupMethodName, groupRefSerializer, groupResultSerializer);
        RemoveGroup = Unary(RemoveGroupMethodName, groupRefSerializer, ackSerializer);
        ListGroups = Unary(ListGroupsMethodName, pageRequestSerializer, groupPageSerializer);

        AddMember = Unary(AddMemberMethodName, memberEdgeSerializer, ackSerializer);
        RemoveMember = Unary(RemoveMemberMethodName, memberEdgeSerializer, ackSerializer);
        ListGroupMembers = Unary(ListGroupMembersMethodName, groupRefSerializer, stringListSerializer);
        ListSubjectGroups = Unary(ListSubjectGroupsMethodName, memberRefSerializer, stringListSerializer);

        PutRule = Unary(PutRuleMethodName, putRuleSerializer, ackSerializer);
        GetRule = Unary(GetRuleMethodName, ruleRefSerializer, ruleResultSerializer);
        RemoveRule = Unary(RemoveRuleMethodName, ruleRefSerializer, ruleRemovedSerializer);
        ListRules = Unary(ListRulesMethodName, pageRequestSerializer, rulePageSerializer);
        ListRulesForTree = Unary(ListRulesForTreeMethodName, treeRulesPageSerializer, rulePageSerializer);

        Explain = Unary(ExplainMethodName, explainQuerySerializer, explanationSerializer);
        EffectivePermissions = Unary(EffectivePermissionsMethodName, subjectRefSerializer, effectivePermissionsSerializer);

        SearchDirectory = Unary(SearchDirectoryMethodName, directorySearchRequestSerializer, directorySearchResultSerializer);
        ResolveDirectoryPrincipal = Unary(ResolveDirectoryPrincipalMethodName, principalRefSerializer, directoryPrincipalResultSerializer);
        GetAccessModel = Unary(GetAccessModelMethodName, accessModelQuerySerializer, accessModelSerializer);
    }

    private static Method<TRequest, TResponse> Unary<TRequest, TResponse>(
        string name,
        Serializer<TRequest> requestSerializer,
        Serializer<TResponse> responseSerializer)
        where TRequest : class
        where TResponse : class
        => new(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: name,
            requestMarshaller: LatticeAuthApiGrpcMarshallers.Create(requestSerializer),
            responseMarshaller: LatticeAuthApiGrpcMarshallers.Create(responseSerializer));

    /// <summary>The unary <c>UpsertUser</c> RPC.</summary>
    public Method<AuthUser, AuthAck> UpsertUser { get; }

    /// <summary>The unary <c>GetUser</c> RPC.</summary>
    public Method<AuthUserRef, AuthUserResult> GetUser { get; }

    /// <summary>The unary <c>RemoveUser</c> RPC.</summary>
    public Method<AuthUserRef, AuthAck> RemoveUser { get; }

    /// <summary>The unary <c>ListUsers</c> RPC.</summary>
    public Method<AuthPageRequest, AuthUserPage> ListUsers { get; }

    /// <summary>The unary <c>UpsertGroup</c> RPC.</summary>
    public Method<AuthGroup, AuthAck> UpsertGroup { get; }

    /// <summary>The unary <c>GetGroup</c> RPC.</summary>
    public Method<AuthGroupRef, AuthGroupResult> GetGroup { get; }

    /// <summary>The unary <c>RemoveGroup</c> RPC.</summary>
    public Method<AuthGroupRef, AuthAck> RemoveGroup { get; }

    /// <summary>The unary <c>ListGroups</c> RPC.</summary>
    public Method<AuthPageRequest, AuthGroupPage> ListGroups { get; }

    /// <summary>The unary <c>AddMember</c> RPC.</summary>
    public Method<AuthMemberEdge, AuthAck> AddMember { get; }

    /// <summary>The unary <c>RemoveMember</c> RPC.</summary>
    public Method<AuthMemberEdge, AuthAck> RemoveMember { get; }

    /// <summary>The unary <c>ListGroupMembers</c> RPC.</summary>
    public Method<AuthGroupRef, AuthStringList> ListGroupMembers { get; }

    /// <summary>The unary <c>ListSubjectGroups</c> RPC.</summary>
    public Method<AuthMemberRef, AuthStringList> ListSubjectGroups { get; }

    /// <summary>The unary <c>PutRule</c> RPC.</summary>
    public Method<AuthPutRule, AuthAck> PutRule { get; }

    /// <summary>The unary <c>GetRule</c> RPC.</summary>
    public Method<AuthRuleRef, AuthRuleResult> GetRule { get; }

    /// <summary>The unary <c>RemoveRule</c> RPC.</summary>
    public Method<AuthRuleRef, AuthRuleRemoved> RemoveRule { get; }

    /// <summary>The unary <c>ListRules</c> RPC.</summary>
    public Method<AuthPageRequest, AuthRulePage> ListRules { get; }

    /// <summary>The unary <c>ListRulesForTree</c> RPC.</summary>
    public Method<AuthTreeRulesPage, AuthRulePage> ListRulesForTree { get; }

    /// <summary>The unary <c>Explain</c> RPC.</summary>
    public Method<AuthExplainQuery, AuthExplanation> Explain { get; }

    /// <summary>The unary <c>EffectivePermissions</c> RPC.</summary>
    public Method<AuthSubjectRef, AuthEffectivePermissions> EffectivePermissions { get; }

    /// <summary>The unary <c>SearchDirectory</c> RPC.</summary>
    public Method<DirectorySearchRequest, DirectorySearchResult> SearchDirectory { get; }

    /// <summary>The unary <c>ResolveDirectoryPrincipal</c> RPC.</summary>
    public Method<AuthPrincipalRef, AuthDirectoryPrincipalResult> ResolveDirectoryPrincipal { get; }

    /// <summary>The unary <c>GetAccessModel</c> RPC.</summary>
    public Method<AuthAccessModelQuery, AccessModelDescriptor> GetAccessModel { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>. Shared by the server-side DI factory
    /// and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeAuthApiGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeAuthApiGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<AuthUser>>(),
            serializerProvider.GetRequiredService<Serializer<AuthUserRef>>(),
            serializerProvider.GetRequiredService<Serializer<AuthUserResult>>(),
            serializerProvider.GetRequiredService<Serializer<AuthUserPage>>(),
            serializerProvider.GetRequiredService<Serializer<AuthGroup>>(),
            serializerProvider.GetRequiredService<Serializer<AuthGroupRef>>(),
            serializerProvider.GetRequiredService<Serializer<AuthGroupResult>>(),
            serializerProvider.GetRequiredService<Serializer<AuthGroupPage>>(),
            serializerProvider.GetRequiredService<Serializer<AuthPageRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthMemberRef>>(),
            serializerProvider.GetRequiredService<Serializer<AuthMemberEdge>>(),
            serializerProvider.GetRequiredService<Serializer<AuthStringList>>(),
            serializerProvider.GetRequiredService<Serializer<AuthPutRule>>(),
            serializerProvider.GetRequiredService<Serializer<AuthRuleRef>>(),
            serializerProvider.GetRequiredService<Serializer<AuthRuleResult>>(),
            serializerProvider.GetRequiredService<Serializer<AuthRuleRemoved>>(),
            serializerProvider.GetRequiredService<Serializer<AuthRulePage>>(),
            serializerProvider.GetRequiredService<Serializer<AuthTreeRulesPage>>(),
            serializerProvider.GetRequiredService<Serializer<AuthExplainQuery>>(),
            serializerProvider.GetRequiredService<Serializer<AuthExplanation>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSubjectRef>>(),
            serializerProvider.GetRequiredService<Serializer<AuthEffectivePermissions>>(),
            serializerProvider.GetRequiredService<Serializer<AuthAck>>(),
            serializerProvider.GetRequiredService<Serializer<DirectorySearchRequest>>(),
            serializerProvider.GetRequiredService<Serializer<DirectorySearchResult>>(),
            serializerProvider.GetRequiredService<Serializer<AuthPrincipalRef>>(),
            serializerProvider.GetRequiredService<Serializer<AuthDirectoryPrincipalResult>>(),
            serializerProvider.GetRequiredService<Serializer<AuthAccessModelQuery>>(),
            serializerProvider.GetRequiredService<Serializer<AccessModelDescriptor>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeAuthApiGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI
/// dependencies directly). Setting it more than once is allowed: subsequent
/// registrations replace the prior instance, matching the "last-host-wins"
/// semantics integration-test fixtures rely on.
/// </summary>
internal static class LatticeAuthApiGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeAuthApiGrpcMethods? Current { get; set; }
}
