using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Views;

internal sealed partial class ViewMaintainerGrain
{
    private ILatticeEnvelopeCodec? _projectionCodec;
    private bool _projectionCodecResolved;
    private bool _projectionCodecActive;

    /// <summary>
    /// The single projection-input seam (#5240). Every derivation path - incremental
    /// apply, atomic publication, joint repair, rebuild and reconcile, over an aliased
    /// or unaliased source - hands its mutation to
    /// <see cref="ILatticeViewProjection.Project"/> /
    /// <see cref="ILatticeAggregationProjection.Project"/> through here, so a
    /// projection over a versioning-enabled source always receives the
    /// envelope-stripped body at its stamped version, never upcast, whichever path
    /// derived it. Each path reads the stored form (the WAL entry or the stored
    /// row), so the strip is the only transformation and is identical on all of
    /// them; a target advance therefore reshapes no contribution and needs no
    /// rebuild.
    /// </summary>
    /// <remarks>
    /// The strip is <see cref="ILatticeEnvelopeCodec.StripForFold"/>: version-agnostic
    /// header removal that never upcasts, so a replayed or rebuilt projection sees
    /// exactly the bytes the first apply saw (the determinism contract on
    /// <see cref="ILatticeEnvelopeCodec"/>). The codec is resolved once per activation;
    /// with no versioning registered the mutation is returned unchanged with no
    /// allocation, and an un-enveloped value on an active codec is likewise handed
    /// through by reference.
    /// </remarks>
    /// <param name="registration">The view registration whose source the mutation came from.</param>
    /// <param name="mutation">The stored-form source mutation.</param>
    /// <returns>The mutation the projection must see.</returns>
    private LatticeMutation ProjectionInput(ViewRegistration registration, in LatticeMutation mutation)
    {
        if (!ProjectionCodecActive(registration.SourceTreeId) || mutation.Value is not { } value)
        {
            return mutation;
        }

        var body = _projectionCodec!.StripForFold(value);
        return ReferenceEquals(body, value) ? mutation : mutation with { Value = body };
    }

    private bool ProjectionCodecActive(string sourceTreeId)
    {
        if (!_projectionCodecResolved)
        {
            _projectionCodec = context.ActivationServices.GetService<ILatticeEnvelopeCodec>();
            _projectionCodecActive = _projectionCodec is not null
                and not NullLatticeEnvelopeCodec
                && _projectionCodec.IsActive(sourceTreeId);
            _projectionCodecResolved = true;
        }

        return _projectionCodecActive;
    }
}
