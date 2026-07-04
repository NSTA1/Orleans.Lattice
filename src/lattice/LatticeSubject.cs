namespace Orleans.Lattice;

/// <summary>
/// The resolved identity of the caller behind a Lattice operation: a stable
/// subject id, the fully transitively-expanded set of group ids the subject
/// belongs to, and an optional flat claim bag. The Membership layer produces a
/// <see cref="LatticeSubject"/> by resolving the ambient
/// <see cref="LatticeCredential"/>; the core library only defines the type so
/// the later access-gate seam can consume it without taking a dependency on
/// Membership.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="GroupIds"/> always carries the <b>full transitive closure</b> of
/// group membership (nested groups are supported and expanded, with cycle
/// detection, when the subject is built). Downstream policy evaluation can
/// therefore treat group membership as a flat set and never has to walk the
/// group graph itself.
/// </para>
/// <para>
/// Two well-known singletons are provided: <see cref="Anonymous"/> for a caller
/// that carries no (or an unresolvable) credential, and <see cref="System"/>
/// for library-internal, infrastructure-authored operations that run outside
/// any user identity.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeSubject)]
[Immutable]
public readonly record struct LatticeSubject
{
    /// <summary>The stable subject id for the well-known anonymous subject.</summary>
    public const string AnonymousSubjectId = "anonymous";

    /// <summary>The stable subject id for the well-known system subject.</summary>
    public const string SystemSubjectId = "system";

    /// <summary>
    /// Initializes a new <see cref="LatticeSubject"/> with the supplied subject
    /// id, transitively-expanded group closure, and optional claims.
    /// </summary>
    /// <param name="subjectId">The stable subject id. Must not be <c>null</c>.</param>
    /// <param name="groupIds">
    /// The full transitively-expanded set of group ids the subject belongs to,
    /// or <c>null</c> for none (treated as an empty set).
    /// </param>
    /// <param name="claims">
    /// An optional flat claim bag carried from the identity provider, or
    /// <c>null</c> when the subject carries no claims.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    public LatticeSubject(
        string subjectId,
        IReadOnlyCollection<string>? groupIds = null,
        IReadOnlyDictionary<string, string>? claims = null)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        SubjectId = subjectId;
        GroupIds = groupIds ?? Array.Empty<string>();
        Claims = claims;
    }

    /// <summary>The stable subject id (for example a user id or service principal id).</summary>
    [Id(0)]
    public string SubjectId { get; init; }

    /// <summary>
    /// The full transitively-expanded set of group ids the subject belongs to.
    /// Never <c>null</c>; an empty set means the subject belongs to no groups.
    /// </summary>
    [Id(1)]
    public IReadOnlyCollection<string> GroupIds { get; init; }

    /// <summary>
    /// An optional flat claim bag carried from the identity provider, or
    /// <c>null</c> when the subject carries no claims.
    /// </summary>
    [Id(2)]
    public IReadOnlyDictionary<string, string>? Claims { get; init; }

    /// <summary>
    /// <c>true</c> when this subject is the well-known <see cref="Anonymous"/>
    /// subject (no resolved identity).
    /// </summary>
    public bool IsAnonymous => string.Equals(SubjectId, AnonymousSubjectId, StringComparison.Ordinal);

    /// <summary>
    /// The well-known anonymous subject: no groups, no claims. Returned when a
    /// caller carries no credential or the credential cannot be resolved.
    /// </summary>
    public static LatticeSubject Anonymous { get; } = new(AnonymousSubjectId);

    /// <summary>
    /// The well-known system subject used by library-internal,
    /// infrastructure-authored operations that run outside any user identity.
    /// </summary>
    public static LatticeSubject System { get; } = new(SystemSubjectId);
}
