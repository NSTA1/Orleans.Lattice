namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Centralised Orleans serialization alias constants for every
/// <c>Orleans.Lattice.Api.Auth</c> type that participates in the wire format.
/// Each alias is a short, fixed string that gives a type a stable wire identity
/// independent of its CLR name. Auth-API facade aliases use the <c>oli.</c>
/// prefix (Orleans Lattice Api) and are at most six characters, mirroring the
/// core (<c>ol.</c>) and authorization (<c>olz.</c>) alias tables; the
/// invariants are enforced by <c>ApiAuthTypeAliasesTests</c>.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class ApiAuthTypeAliases
{
    /// <summary>Alias for <see cref="AuthUser"/>.</summary>
    public const string AuthUser = "oli.us";

    /// <summary>Alias for <see cref="AuthGroup"/>.</summary>
    public const string AuthGroup = "oli.gr";

    /// <summary>Alias for <see cref="AuthPageRequest"/>.</summary>
    public const string AuthPageRequest = "oli.pq";

    /// <summary>Alias for <see cref="AuthUserPage"/>.</summary>
    public const string AuthUserPage = "oli.up";

    /// <summary>Alias for <see cref="AuthGroupPage"/>.</summary>
    public const string AuthGroupPage = "oli.gp";

    /// <summary>Alias for <see cref="AuthRulePage"/>.</summary>
    public const string AuthRulePage = "oli.rp";

    /// <summary>Alias for <see cref="AuthExplanation"/>.</summary>
    public const string AuthExplanation = "oli.ex";

    /// <summary>Alias for <see cref="AuthEffectivePermissions"/>.</summary>
    public const string AuthEffectivePermissions = "oli.ep";

    // ----- gRPC binding wire envelopes -----
    // The sibling gRPC binding (Orleans.Lattice.Api.Auth.Grpc) adds a small set
    // of request / response envelope records for RPCs whose facade signature is
    // not already a single serializable DTO (a bare id argument, a void return,
    // or a nullable result). They reuse this table (the compact oli. namespace)
    // rather than defining a separate registry, so the auth-API wire tags all
    // live in one place. The constant lives here in the facade package; the type
    // that carries the alias lives in the gRPC assembly.

    /// <summary>Alias for the gRPC single-user reference request (a bare user id).</summary>
    public const string AuthUserRef = "oli.ur";

    /// <summary>Alias for the gRPC single-group reference request (a bare group id).</summary>
    public const string AuthGroupRef = "oli.gd";

    /// <summary>Alias for the gRPC single-member reference request (a bare member id).</summary>
    public const string AuthMemberRef = "oli.mr";

    /// <summary>Alias for the gRPC single-rule reference request (governed tree id + rule id).</summary>
    public const string AuthRuleRef = "oli.rr";

    /// <summary>Alias for the gRPC membership-edge request (group id, member id, member kind).</summary>
    public const string AuthMemberEdge = "oli.me";

    /// <summary>Alias for the gRPC put-rule request (wraps the authored rule).</summary>
    public const string AuthPutRule = "oli.pr";

    /// <summary>Alias for the gRPC list-rules-for-tree request (tree id + page request).</summary>
    public const string AuthTreeRulesPage = "oli.tp";

    /// <summary>Alias for the gRPC explain request (subject, operation, scope).</summary>
    public const string AuthExplainQuery = "oli.xq";

    /// <summary>Alias for the gRPC single-subject reference request (a bare subject id).</summary>
    public const string AuthSubjectRef = "oli.sr";

    /// <summary>Alias for the gRPC acknowledgement response for a void operation.</summary>
    public const string AuthAck = "oli.ak";

    /// <summary>Alias for the gRPC nullable-user result response.</summary>
    public const string AuthUserResult = "oli.ut";

    /// <summary>Alias for the gRPC nullable-group result response.</summary>
    public const string AuthGroupResult = "oli.gt";

    /// <summary>Alias for the gRPC nullable-rule result response.</summary>
    public const string AuthRuleResult = "oli.lt";

    /// <summary>Alias for the gRPC ordered-string-list response (members / groups).</summary>
    public const string AuthStringList = "oli.sl";

    /// <summary>Alias for the gRPC rule-removed response (a single boolean).</summary>
    public const string AuthRuleRemoved = "oli.rv";
}
