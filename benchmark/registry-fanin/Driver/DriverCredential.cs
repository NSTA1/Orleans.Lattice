namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// Stamps the driver's calls with the credential the target host trusts.
/// <para>
/// The repocontext host installs a DEFAULT-DENY authorization gate
/// (<c>AddLatticeAuth(options =&gt; options.DefaultEffect = Deny)</c>) and seeds
/// Allow rules only for its own fixed set of trees. A driver that called
/// without a credential resolves as the anonymous subject, matches no rule, and
/// is refused with <c>LatticeAuthorizationDeniedException</c> on the very first
/// write - which is what happens, and it is worth naming because the failure
/// surfaces as a data-plane fault rather than as anything that mentions
/// authentication.
/// </para>
/// <para>
/// The driver therefore presents the host's BOOTSTRAP ADMINISTRATOR subject.
/// That subject short-circuits the gate as Allow on every tree and operation -
/// it is the root of trust that exists so a policy misconfiguration cannot lock
/// every operator out of the policy tree itself - so the driver can create,
/// populate and tear down its own trees without seeding policy for them, and
/// without mutating the host's policy state at all. Leaving the estate's policy
/// untouched matters for a measurement rig: seeding rules per driver tree would
/// write to the reserved policy tree on every run, which is itself load on the
/// seam under study.
/// </para>
/// <para>
/// The credential travels on Orleans' <c>RequestContext</c>, which propagates
/// from an external client through to the silo, so scoping it around the whole
/// workload is enough - no per-call plumbing and no client-side filter.
/// </para>
/// </summary>
internal static class DriverCredential
{
    /// <summary>The host's bootstrap-administrator subject id.</summary>
    public const string BootstrapAdministrator = "repocontext-bootstrap-admin";

    /// <summary>The credential scheme the host's authenticator claims.</summary>
    public const string Scheme = "repocontext-local";

    /// <summary>
    /// Scopes the ambient credential for the caller's lifetime.
    /// </summary>
    /// <param name="subject">The subject id to present.</param>
    /// <param name="scheme">The credential scheme to present.</param>
    /// <returns>A scope that restores the previous credential when disposed.</returns>
    public static IDisposable Use(string subject, string scheme)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentException.ThrowIfNullOrWhiteSpace(scheme);

        return LatticeCredentialContext.Use(subject, scheme: scheme);
    }
}
