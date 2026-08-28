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
/// <b>This runs on every inbound request of every bound facade</b>, so it is
/// written to allocate nothing on the common path. The lookup takes an explicit
/// state parameter so a call site can pass a <see langword="static"/> lambda and
/// avoid a per-call closure; the header name is normalised without allocating
/// when it is already lower-case (as the default is); the header value is trimmed
/// and validated over spans; and a valid tenant reuses the header string itself
/// rather than copying it. A call that asserts no tenant allocates nothing at
/// all.
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
    /// Memoised lower-cased form of the most recently seen header name, for the
    /// case where a host configures one that is not already lower-case. Held as a
    /// single reference so the pair is read and published atomically - a torn read
    /// of two separate fields could pair one name with another's normalisation -
    /// and a benign race only recomputes. The common case never reaches this.
    /// </summary>
    private static NormalizedHeaderName? _normalizedCache;

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
    /// <typeparam name="TState">The state the lookup reads the header from (typically the call context).</typeparam>
    /// <param name="state">Passed back to <paramref name="lookup"/>, so a <see langword="static"/> lambda needs no closure.</param>
    /// <param name="lookup">
    /// Reads the named header from <paramref name="state"/>, returning
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
    public static IDisposable? Stamp<TState>(TState state, Func<TState, string, string?> lookup, string? headerName)
    {
        var tenant = Resolve(state, lookup, headerName);
        return tenant is null ? null : LatticeActiveTenantContext.With(tenant);
    }

    /// <summary>
    /// Resolves the asserted active tenant without stamping it, for a binding that
    /// needs the value itself. Applies the same fail-closed rules as
    /// <see cref="Stamp{TState}"/>.
    /// </summary>
    /// <typeparam name="TState">The state the lookup reads the header from.</typeparam>
    /// <param name="state">Passed back to <paramref name="lookup"/>.</param>
    /// <param name="lookup">Reads the named header. Must not be <c>null</c>.</param>
    /// <param name="headerName">The header to read; <c>null</c> or empty disables the assertion.</param>
    /// <returns>The asserted tenant, or <see langword="null"/> when none is asserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="lookup"/> is <c>null</c>.</exception>
    public static TenantId? Resolve<TState>(TState state, Func<TState, string, string?> lookup, string? headerName)
    {
        ArgumentNullException.ThrowIfNull(lookup);

        if (string.IsNullOrEmpty(headerName))
        {
            return null;
        }

        var raw = lookup(state, Normalize(headerName));
        if (raw is null)
        {
            return null;
        }

        // Trimmed over a span: an untrimmed value (the overwhelmingly common case)
        // costs nothing, and a padded one is validated without materialising a
        // copy that would be thrown away if it turned out to be invalid.
        var span = raw.AsSpan().Trim();
        if (span.IsEmpty || !TenantId.IsValid(span))
        {
            // Fail-closed: absent, blank, or not a syntactically valid tenant id
            // is not an assertion we honour.
            return null;
        }

        // Reuse the header string itself when it needed no trimming, so the common
        // path adds no allocation at all; only a padded value is copied.
        return TenantId.ForValidated(span.Length == raw.Length ? raw : new string(span));
    }

    /// <summary>
    /// Returns <paramref name="headerName"/> lower-cased, without allocating when
    /// it already is - which the default, and any conventional configuration,
    /// satisfies. Transport metadata keys are conventionally lower-cased (gRPC
    /// normalises them outright), so the lookup is normalised here rather than in
    /// each binding, where a configured name with different casing would silently
    /// miss the inbound entry.
    /// </summary>
    private static string Normalize(string headerName)
    {
        if (!NeedsLowering(headerName))
        {
            return headerName;
        }

        // A host that configures a non-lower-case name pays one allocation on the
        // first request and reads it back from the memo afterwards, rather than
        // re-lowering on every request for the life of the process.
        var cached = _normalizedCache;
        if (cached is not null && ReferenceEquals(cached.Source, headerName))
        {
            return cached.Normalized;
        }

        var normalized = headerName.ToLowerInvariant();
        _normalizedCache = new NormalizedHeaderName(headerName, normalized);
        return normalized;
    }

    /// <summary>
    /// Scans for any character that lower-casing would change. Allocation-free,
    /// and over a header name it is a handful of comparisons.
    /// </summary>
    private static bool NeedsLowering(string value)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (char.IsUpper(value[i]))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Pairs a configured header name with its lower-cased form so both are published together.</summary>
    private sealed class NormalizedHeaderName(string source, string normalized)
    {
        /// <summary>The configured name, compared by reference to detect a hit.</summary>
        public string Source { get; } = source;

        /// <summary>Its lower-cased form.</summary>
        public string Normalized { get; } = normalized;
    }
}
