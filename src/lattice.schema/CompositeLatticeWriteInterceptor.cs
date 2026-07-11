namespace Orleans.Lattice.Schema;

/// <summary>
/// Chains an ordered set of <see cref="ILatticeWriteInterceptor"/> stages into a
/// single interceptor so the schema-enforcement and schema-versioning add-ons
/// compose when both are registered. The stages run in order, threading each
/// stage's accepted (possibly transformed) bytes into the next; a
/// <see cref="LatticeWriteDecisionKind.Reject"/> or
/// <see cref="LatticeWriteDecisionKind.DeadLetter"/> from any stage short-circuits
/// the remainder.
/// </summary>
/// <remarks>
/// <para>
/// <b>Composition order.</b> Enforcement runs <b>before</b> versioning, so a value
/// is validated as a plain document first and then wrapped in the version envelope.
/// This realises the issue's resolved rule that, when both features are on, values
/// are validated against the target (post-upcast) shape and the envelope is applied
/// last (a validator must never see the opaque enveloped bytes).
/// </para>
/// <para>
/// <b>Zero overhead when off.</b> With a single stage the composite forwards
/// directly to it, so wiring the composite never adds an interceptor round-trip
/// beyond the single stage that would otherwise run.
/// </para>
/// </remarks>
internal sealed class CompositeLatticeWriteInterceptor : ILatticeWriteInterceptor
{
    private readonly ILatticeWriteInterceptor[] _stages;

    /// <summary>
    /// Composes the schema write interceptors that are registered. The
    /// <paramref name="versioning"/> stage is always present; the
    /// <paramref name="enforcement"/> stage is present only when schema enforcement
    /// was also added, and when present runs first.
    /// </summary>
    /// <param name="versioning">The versioning (envelope-stamping) stage. Must not be <c>null</c>.</param>
    /// <param name="enforcement">The enforcement (validation) stage, or <c>null</c> when enforcement is not registered.</param>
    /// <exception cref="ArgumentNullException"><paramref name="versioning"/> is <c>null</c>.</exception>
    public CompositeLatticeWriteInterceptor(
        LatticeSchemaVersionWriteInterceptor versioning,
        LatticeSchemaWriteInterceptor? enforcement = null)
        : this((ILatticeWriteInterceptor)versioning, enforcement)
    {
    }

    /// <summary>
    /// Composes an arbitrary versioning stage with an optional enforcement stage.
    /// This overload takes the interface so the composition is unit-testable with a
    /// stand-in stage; the DI wiring uses the concrete-typed constructor above.
    /// </summary>
    /// <param name="versioning">The versioning (envelope-stamping) stage. Must not be <c>null</c>.</param>
    /// <param name="enforcement">The enforcement (validation) stage, or <c>null</c> when enforcement is not registered.</param>
    /// <exception cref="ArgumentNullException"><paramref name="versioning"/> is <c>null</c>.</exception>
    internal CompositeLatticeWriteInterceptor(
        ILatticeWriteInterceptor versioning,
        ILatticeWriteInterceptor? enforcement)
    {
        ArgumentNullException.ThrowIfNull(versioning);
        _stages = enforcement is null
            ? new[] { versioning }
            : new[] { enforcement, versioning };
    }

    /// <inheritdoc />
    public bool InterceptsSystemOrigin
    {
        get
        {
            foreach (var stage in _stages)
            {
                if (stage.InterceptsSystemOrigin)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <inheritdoc />
    public ValueTask<LatticeWriteDecision> OnWriteAsync(
        in LatticeWriteRequest request,
        CancellationToken cancellationToken = default)
    {
        // Single-stage fast path: forward directly, so a silo that enabled only one
        // of the two add-ons pays no composition overhead.
        if (_stages.Length == 1)
        {
            return _stages[0].OnWriteAsync(in request, cancellationToken);
        }

        return RunChainAsync(request, cancellationToken);
    }

    private async ValueTask<LatticeWriteDecision> RunChainAsync(
        LatticeWriteRequest request, CancellationToken cancellationToken)
    {
        var value = request.Value;
        var transformed = false;

        foreach (var stage in _stages)
        {
            var stageRequest = transformed
                ? new LatticeWriteRequest(request.TreeId, request.Key, value, request.Operation, request.Ttl)
                : request;

            var decision = await stage.OnWriteAsync(in stageRequest, cancellationToken).ConfigureAwait(false);
            switch (decision.Kind)
            {
                case LatticeWriteDecisionKind.Accept:
                    break;
                case LatticeWriteDecisionKind.AcceptTransformed:
                    value = decision.TransformedValue!;
                    transformed = true;
                    break;
                default:
                    // Reject / DeadLetter short-circuits the remaining stages.
                    return decision;
            }
        }

        return transformed
            ? LatticeWriteDecision.AcceptTransformed(value)
            : LatticeWriteDecision.Accept();
    }
}
