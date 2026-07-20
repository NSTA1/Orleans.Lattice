namespace Orleans.Lattice.Membership;

/// <summary>
/// Thrown by an administrative membership-reference create path (upserting a
/// group, adding a member to a group) when
/// <see cref="LatticeIdentityDirectoryOptions.ValidationRequired"/> is set, a real
/// <see cref="ILatticeIdentityDirectory"/> provider is active (not
/// <see cref="NullIdentityDirectory"/>), and the supplied principal id fails to
/// validate: either it resolves to no principal, or the resolved
/// <see cref="DirectoryPrincipal.Kind"/> does not match the
/// <see cref="ExpectedKind"/> the operation expects (for example a user id
/// supplied where a group id was required). The create is <b>fail-closed</b>:
/// nothing is written before this exception is raised, so an unresolved or
/// wrong-kind reference never leaves a partial membership edge behind.
/// </summary>
/// <remarks>
/// <para>
/// When the active provider is <see cref="NullIdentityDirectory"/> no validation
/// is performed regardless of <see cref="LatticeIdentityDirectoryOptions.ValidationRequired"/>,
/// matching the documented contract that the no-op provider accepts ids without
/// validation - so this exception is never raised in that configuration.
/// </para>
/// <para>
/// Derives from <see cref="ArgumentException"/> because the rejection is caused by
/// a caller-supplied id value that the configured identity source cannot honour;
/// the transport binding maps it to a client-facing invalid-argument status rather
/// than an internal error.
/// </para>
/// </remarks>
public sealed class LatticeDirectoryValidationException : ArgumentException
{
    /// <summary>
    /// The principal id that failed validation. Empty on the parameterless /
    /// message-only constructors.
    /// </summary>
    public string PrincipalId { get; }

    /// <summary>The <see cref="DirectoryPrincipalKind"/> the create path expected the id to resolve to.</summary>
    public DirectoryPrincipalKind ExpectedKind { get; }

    /// <summary>
    /// The <see cref="DirectoryPrincipalKind"/> the id actually resolved to, or
    /// <c>null</c> when the id resolved to no principal at all.
    /// </summary>
    public DirectoryPrincipalKind? ResolvedKind { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception-construction contract;
    /// production throw sites use the context-carrying factory methods.
    /// </summary>
    public LatticeDirectoryValidationException()
    {
        PrincipalId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty context.</summary>
    /// <param name="message">Diagnostic context describing the validation failure.</param>
    public LatticeDirectoryValidationException(string message) : base(message)
    {
        PrincipalId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the validation failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeDirectoryValidationException(string message, Exception innerException)
        : base(message, innerException)
    {
        PrincipalId = string.Empty;
    }

    private LatticeDirectoryValidationException(
        string message,
        string paramName,
        string principalId,
        DirectoryPrincipalKind expectedKind,
        DirectoryPrincipalKind? resolvedKind)
        : base(message, paramName)
    {
        PrincipalId = principalId;
        ExpectedKind = expectedKind;
        ResolvedKind = resolvedKind;
    }

    /// <summary>
    /// Creates an exception for a principal id that resolved to no principal in the
    /// active identity source.
    /// </summary>
    /// <param name="principalId">The unresolved principal id. Must not be <c>null</c>.</param>
    /// <param name="expectedKind">The kind the create path expected the id to resolve to.</param>
    /// <param name="paramName">The name of the offending create-path parameter. Must not be <c>null</c>.</param>
    /// <returns>A configured <see cref="LatticeDirectoryValidationException"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="principalId"/> or <paramref name="paramName"/> is <c>null</c>.</exception>
    public static LatticeDirectoryValidationException Unresolved(
        string principalId,
        DirectoryPrincipalKind expectedKind,
        string paramName)
    {
        ArgumentNullException.ThrowIfNull(principalId);
        ArgumentNullException.ThrowIfNull(paramName);
        var message = $"Directory validation failed: the {expectedKind} id '{principalId}' does not "
            + "resolve to any principal in the configured identity directory.";
        return new LatticeDirectoryValidationException(message, paramName, principalId, expectedKind, resolvedKind: null);
    }

    /// <summary>
    /// Creates an exception for a principal id that resolved to a principal of the
    /// wrong <see cref="DirectoryPrincipalKind"/>.
    /// </summary>
    /// <param name="principalId">The mis-kinded principal id. Must not be <c>null</c>.</param>
    /// <param name="expectedKind">The kind the create path expected the id to resolve to.</param>
    /// <param name="resolvedKind">The kind the id actually resolved to.</param>
    /// <param name="paramName">The name of the offending create-path parameter. Must not be <c>null</c>.</param>
    /// <returns>A configured <see cref="LatticeDirectoryValidationException"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="principalId"/> or <paramref name="paramName"/> is <c>null</c>.</exception>
    public static LatticeDirectoryValidationException KindMismatch(
        string principalId,
        DirectoryPrincipalKind expectedKind,
        DirectoryPrincipalKind resolvedKind,
        string paramName)
    {
        ArgumentNullException.ThrowIfNull(principalId);
        ArgumentNullException.ThrowIfNull(paramName);
        var message = $"Directory validation failed: the id '{principalId}' resolves to a "
            + $"{resolvedKind} principal, but a {expectedKind} was expected.";
        return new LatticeDirectoryValidationException(message, paramName, principalId, expectedKind, resolvedKind);
    }
}
