namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>lattice_auth_list_group_members</c> tool: the
/// direct members (users and nested groups) of a single group.
/// </summary>
/// <remarks>
/// <para>
/// A dedicated record (rather than a bare list return) so the tool emits a single
/// authoritative object shape. When a tool returns a bare collection the MCP SDK
/// projects the text block as the raw array (<c>[]</c>) but wraps the structured
/// block under a synthetic <c>result</c> property (<c>{"result":[]}</c>), so the
/// two emitted copies disagree on shape; returning an object makes both copies
/// identical.
/// </para>
/// <para>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </para>
/// </remarks>
public sealed record AuthGroupMembersResult
{
    /// <summary>The group whose direct members were listed.</summary>
    public required string GroupId { get; init; }

    /// <summary>
    /// The direct members (users and nested group ids) of the group, in ascending
    /// ordinal order. Empty when the group has no members or does not exist.
    /// </summary>
    public required IReadOnlyList<string> Members { get; init; }
}

/// <summary>
/// Structured result of the <c>lattice_auth_list_subject_groups</c> tool: the
/// full transitive set of group ids a subject belongs to.
/// </summary>
/// <remarks>
/// <para>
/// A dedicated record (rather than a bare list return) so the tool emits a single
/// authoritative object shape, for the same reason described on
/// <see cref="AuthGroupMembersResult"/>.
/// </para>
/// <para>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </para>
/// </remarks>
public sealed record AuthSubjectGroupsResult
{
    /// <summary>The subject whose transitive group membership was listed.</summary>
    public required string MemberId { get; init; }

    /// <summary>
    /// The full transitive set of group ids the subject belongs to, walking nested
    /// groups. Empty when the subject belongs to no group.
    /// </summary>
    public required IReadOnlyList<string> Groups { get; init; }
}
