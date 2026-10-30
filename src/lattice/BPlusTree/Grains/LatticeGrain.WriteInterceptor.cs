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
/// The system-origin bypass is honoured inside
/// <see cref="LatticeWriteInterceptorEnforcement"/> for writes that reach this
/// choke point: a real interceptor is skipped on a system-origin turn unless it
/// opts in through <see cref="ILatticeWriteInterceptor.InterceptsSystemOrigin"/>.
/// Internal paths that write below this choke point do not consult the seam.
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
    /// <see cref="InterceptWriteAsync"/> for an apply that may run more than once
    /// for one delivery. The first call consults the interceptor and records its
    /// verdict in <paramref name="judgement"/>; a later call with the same record
    /// reuses that verdict instead of judging (and dead-lettering) the value again
    /// (issue #5285). With no record every call judges, as
    /// <see cref="InterceptWriteAsync"/> does. A rejection throws and records
    /// nothing, so it is never reused as a verdict.
    /// </summary>
    private async ValueTask<LatticeWriteInterceptionOutcome> InterceptWriteOnceAsync(
        WriteJudgement? judgement, LatticeOperation operation, string key, byte[] value, TimeSpan? ttl, CancellationToken cancellationToken)
    {
        if (judgement is { Judged: true })
        {
            return judgement.Proceed
                ? LatticeWriteInterceptionOutcome.Write(judgement.Value)
                : LatticeWriteInterceptionOutcome.Drop();
        }

        var outcome = await InterceptWriteAsync(operation, key, value, ttl, cancellationToken);
        judgement?.Record(outcome);
        return outcome;
    }

    /// <summary>
    /// The write interceptor's verdict on one delivered value, shared by the
    /// passes that apply that same delivery: a replicated prepare staged (or
    /// settled) on the held copy and mirrored onto the original tree during a
    /// bootstrap hold (#4791). It is created by the apply that owns the delivery
    /// and passed explicitly, never carried ambiently or across a grain call,
    /// so it cannot exempt any other write from interception.
    /// </summary>
    private sealed class WriteJudgement
    {
        /// <summary>Whether the interceptor has judged the value.</summary>
        public bool Judged { get; private set; }

        /// <summary>Whether the value is to be written (not dead-lettered).</summary>
        public bool Proceed { get; private set; }

        /// <summary>The value to write when <see cref="Proceed"/> is <c>true</c>.</summary>
        public byte[] Value { get; private set; } = [];

        /// <summary>Records the interceptor's verdict.</summary>
        public void Record(LatticeWriteInterceptionOutcome outcome)
        {
            Judged = true;
            Proceed = outcome.Proceed;
            Value = outcome.Value;
        }
    }

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
