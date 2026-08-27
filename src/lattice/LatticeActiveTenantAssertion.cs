namespace Orleans.Lattice;

/// <summary>
/// The single implementation of "lift a caller-asserted active tenant off a
/// transport header onto the ambient <see cref="LatticeActiveTenantContext"/>".
/// A transport binding supplies only a header lookup, so the parsing, the
/// fail-closed rules, and the zero-cost absent path live in one place rather than
/// being re-implemented per binding.
/// </summary>
/// <remarks>
/// <para>
/// This exists because getting it wrong is silent. A binding that never stamps
/// the ambient tenant does not fault: its facade's tenant-scoped name resolution
/// simply resolves the reserved default tenant and the caller is served the
/// shared cluster-global namespace, which is the very isolation failure the
/// scoping was added to close. Centralising the behaviour means a binding
/// contributes only a lookup, and cannot get the semantics subtly different.
/// </para>
/// <para>
/// Deliberately transport-agnostic: the lookup is a plain delegate, so core takes
/// no dependency on gRPC, ASP.NET Core, or any other transport, while every
/// binding shares one definition of the behaviour.
/// </para>
/// <para>
/// <b>Assertion, not fact.</b> The tenant carried here is a caller assertion. It
/// is re-validated against the caller's own subject membership by the tenancy
/// add-on's resolver before it can scope anything, so stamping it grants no
/// access on its own.
/// </para>
/// </remarks>
public static class LatticeActiveTenantAssertion
{
    /// <summary>
    /// The conventional transport header name carrying the caller's asserted
    /// active tenant. A binding may make this configurable, but every binding
    /// defaults to this value so one client works against all of them.
    /// </summary>
    public const string DefaultHeaderName = "lattice-active-tenant";

    /// <summary>
    /// Resolves the asserted active tenant from <paramref name="lookup"/> and, when
    /// one is present and syntactically valid, opens an ambient
    /// <see cref="LatticeActiveTenantContext"/> scope for the caller to dispose at
    /// the end of the call. Returns <see langword="null"/> when no tenant is
    /// asserted, so a call on a tenancy-off cluster allocates nothing and is
    /// byte-for-byte unchanged.
    /// </summary>
    /// <remarks>
    /// Fail-closed on every ambiguous input: an absent, empty, whitespace, or
    /// syntactically invalid assertion yields no scope rather than a guessed
    /// tenant, so a malformed header can never attribute a call to a tenant the
    /// caller did not assert.
    /// </remarks>
    /// <param name="lookup">
    /// Reads the named header from the inbound request, returning
    /// <see langword="null"/> when it is absent. Must not be <c>null</c>.
    /// </param>
    /// <param name="headerName">
    /// The header to read. When <c>null</c> or empty the assertion is disabled and
    /// no scope is opened.
    /// </param>
    /// <returns>
    /// A scope restoring the prior ambient active tenant on dispose, or
    /// <see langword="null"/> when no tenant was asserted.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="lookup"/> is <c>null</c>.</exception>
    public static IDisposable? Stamp(Func<string, string?> lookup, string? headerName)
    {
        ArgumentNullException.ThrowIfNull(lookup);

        var tenant = Resolve(lookup, headerName);
        return tenant is null ? null : LatticeActiveTenantContext.With(tenant);
    }

    /// <summary>
    /// Resolves the asserted active tenant without stamping it, for a binding that
    /// needs the value itself. Applies the same fail-closed rules as
    /// <see cref="Stamp"/>.
    /// </summary>
    /// <param name="lookup">Reads the named header. Must not be <c>null</c>.</param>
    /// <param name="headerName">The header to read; <c>null</c> or empty disables the assertion.</param>
    /// <returns>The asserted tenant, or <see langword="null"/> when none is asserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="lookup"/> is <c>null</c>.</exception>
    public static TenantId? Resolve(Func<string, string?> lookup, string? headerName)
    {
        ArgumentNullException.ThrowIfNull(lookup);

        if (string.IsNullOrEmpty(headerName))
        {
            return null;
        }

        // Transport metadata keys are conventionally lower-cased (gRPC normalises
        // them outright), so the lookup is normalised here rather than in each
        // binding, where a configured name with different casing would silently
        // miss the inbound entry.
        var raw = lookup(headerName.ToLowerInvariant());
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return TenantId.TryParse(raw.Trim(), out var tenant) ? tenant : null;
    }
}
