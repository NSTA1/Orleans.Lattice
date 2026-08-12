namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Resolves the fixed <see cref="LatticeCredential"/> the background indexing run
/// assumes for the whole of its pass, so its structural and vector writes carry a
/// caller subject the access gate can authorize.
/// </summary>
/// <remarks>
/// <para>
/// The indexing runner is decoupled from the request that triggers it: a run can
/// be started by an MCP tool call (which carries the caller's ambient credential)
/// or re-started by the durable resume reminder after a host restart (a
/// system-origin grain call that carries <b>no</b> credential). Relying on the
/// ambient credential captured at enqueue time therefore authorizes the
/// request-initiated run but leaves the reminder-initiated resume anonymous, which
/// a fail-closed access gate denies. This seam removes that asymmetry: the runner
/// stamps the authority's credential onto every run at the single point all runs
/// funnel through, so a resume writes under the same subject as the original pass.
/// </para>
/// <para>
/// The credential is a <b>fixed host identity</b> (the box's single trusted local
/// agent), not a per-caller credential, so a singleton implementation is correct
/// and does not re-globalise any per-request credential state. A host that does
/// not register an authority runs indexing under whatever ambient credential the
/// enqueue captured (the default), which is the right behaviour for an in-process
/// host whose access gate is not enabled.
/// </para>
/// </remarks>
public interface IRepoIndexRunAuthority
{
    /// <summary>
    /// Returns the credential the next background indexing run should assume, or
    /// <see langword="null"/> to leave the run's ambient credential untouched.
    /// </summary>
    /// <returns>
    /// The fixed run credential, or <see langword="null"/> when the run should
    /// carry whatever credential the enqueue captured.
    /// </returns>
    LatticeCredential? Resolve();
}

/// <summary>
/// The default <see cref="IRepoIndexRunAuthority"/>: resolves no credential, so the
/// background indexing run carries whatever ambient credential the enqueue
/// captured. This preserves the pre-existing behaviour for an in-process host that
/// registers no run authority (typically one whose access gate is not enabled).
/// </summary>
internal sealed class NullRepoIndexRunAuthority : IRepoIndexRunAuthority
{
    /// <inheritdoc />
    public LatticeCredential? Resolve() => null;
}
