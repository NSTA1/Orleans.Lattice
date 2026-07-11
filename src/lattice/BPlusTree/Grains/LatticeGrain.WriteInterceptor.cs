using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Write-path value-interceptor wiring for the <see cref="LatticeGrain"/>
/// data-plane choke point. This partial resolves the registered
/// <see cref="ILatticeWriteInterceptor"/> once per activation and consults it on
/// every value-carrying local mutation entry point, <b>after</b> the access gate
/// has authorized the caller and <b>before</b> the value is appended to the WAL.
/// </summary>
/// <remarks>
/// <para>
/// <b>Zero-cost default.</b> With only <c>AddLattice</c> registered the
/// interceptor is <see cref="NullLatticeWriteInterceptor"/>, so
/// <see cref="WriteInterceptionActive"/> is <c>false</c> and every call site
/// skips interception without constructing a request, calling the interceptor,
/// or allocating: the default write path is byte-for-byte identical to a build
/// without the seam. The interceptor is consulted only once a real interceptor
/// is registered.
/// </para>
/// <para>
/// The system-origin bypass (replication apply, saga legs, maintenance) is
/// honoured inside <see cref="LatticeWriteInterceptorEnforcement"/>: a real
/// interceptor is skipped on a system-origin turn unless it opts in through
/// <see cref="ILatticeWriteInterceptor.InterceptsSystemOrigin"/>.
/// </para>
/// </remarks>
internal sealed partial class LatticeGrain
{
    private ILatticeWriteInterceptor? _writeInterceptor;
    private bool _writeInterceptorResolved;

    private static readonly ILatticeWriteInterceptor NullWriteInterceptorFallback = new NullLatticeWriteInterceptor();

    /// <summary>
    /// The registered write interceptor, resolved once per activation. Always
    /// non-<c>null</c> in a normally configured host because <c>AddLattice</c>
    /// registers <see cref="NullLatticeWriteInterceptor"/>; falls back to the null
    /// interceptor if the service is somehow unregistered so the write path never
    /// throws on a missing interceptor.
    /// </summary>
    private ILatticeWriteInterceptor WriteInterceptor
    {
        get
        {
            if (!_writeInterceptorResolved)
            {
                _writeInterceptor = services.GetService<ILatticeWriteInterceptor>();
                _writeInterceptorResolved = true;
            }
            return _writeInterceptor ?? NullWriteInterceptorFallback;
        }
    }

    /// <summary>
    /// <c>true</c> when a real (non-null) write interceptor is registered, so the
    /// value-carrying mutation entry points must consult it. <c>false</c> under
    /// the default no-op interceptor, in which case every call site short-circuits
    /// with no per-call work. Cached per activation via <see cref="WriteInterceptor"/>.
    /// </summary>
    private bool WriteInterceptionActive => WriteInterceptor is not NullLatticeWriteInterceptor;

    /// <summary>
    /// Consults the write interceptor for a single-key value, returning the effect
    /// the choke point applies: proceed with the original or a transformed value,
    /// or drop the write (dead-letter). Throws
    /// <see cref="LatticeWriteRejectedException"/> when the interceptor rejects.
    /// Only called from the slow path, guarded by <see cref="WriteInterceptionActive"/>.
    /// </summary>
    private ValueTask<LatticeWriteInterceptionOutcome> InterceptWriteAsync(
        LatticeOperation operation, string key, byte[] value, TimeSpan? ttl, CancellationToken cancellationToken) =>
        LatticeWriteInterceptorEnforcement.InterceptPointAsync(
            WriteInterceptor, TreeId, operation, key, value, ttl, cancellationToken);

    /// <summary>
    /// Consults the write interceptor for every entry of a batch, returning the
    /// effective entry list (the same reference when nothing changed). Throws on a
    /// rejected entry, and - when <paramref name="atomic"/> is <c>true</c> - on a
    /// dead-lettered entry, aborting the whole batch before any write is applied.
    /// Only called from the slow path, guarded by <see cref="WriteInterceptionActive"/>.
    /// </summary>
    private ValueTask<List<KeyValuePair<string, byte[]>>> InterceptEntriesAsync(
        LatticeOperation operation, List<KeyValuePair<string, byte[]>> entries, bool atomic, CancellationToken cancellationToken) =>
        LatticeWriteInterceptorEnforcement.InterceptEntriesAsync(
            WriteInterceptor, TreeId, operation, entries, atomic, cancellationToken);
}
