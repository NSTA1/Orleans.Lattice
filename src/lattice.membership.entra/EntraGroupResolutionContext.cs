namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// The in-process request passed to an <see cref="IEntraGroupResolver"/> when a
/// token's group membership overflowed and full membership must be resolved out
/// of band. Carries only what a resolver needs to identify the caller; it never
/// crosses a grain boundary and is not persisted.
/// </summary>
public sealed class EntraGroupResolutionContext
{
    /// <summary>
    /// Initializes a new <see cref="EntraGroupResolutionContext"/>.
    /// </summary>
    /// <param name="subjectId">The caller's stable subject id (the Entra <c>oid</c>). Must not be <c>null</c> or empty.</param>
    /// <param name="tenantId">The caller's tenant id (the Entra <c>tid</c>), or <c>null</c> when the token carried none.</param>
    /// <param name="tokenAssertedGroups">
    /// The partial group ids the token still carried (for example app roles that
    /// did not overflow), or <c>null</c> when none. A resolver may merge these
    /// with the membership it resolves.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="subjectId"/> is <c>null</c> or empty.</exception>
    public EntraGroupResolutionContext(
        string subjectId,
        string? tenantId = null,
        IReadOnlyCollection<string>? tokenAssertedGroups = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        SubjectId = subjectId;
        TenantId = tenantId;
        TokenAssertedGroups = tokenAssertedGroups;
    }

    /// <summary>The caller's stable subject id (the Entra <c>oid</c>).</summary>
    public string SubjectId { get; }

    /// <summary>The caller's tenant id (the Entra <c>tid</c>), or <c>null</c> when the token carried none.</summary>
    public string? TenantId { get; }

    /// <summary>The partial group ids the token still carried, or <c>null</c> when none.</summary>
    public IReadOnlyCollection<string>? TokenAssertedGroups { get; }
}
