namespace Orleans.Lattice.Scaling;

/// <summary>
/// Inert <see cref="ISplitActivityProbe"/>: reports no shard splits in flight.
/// With this probe the scale-in gate is never suppressed on split grounds.
/// <para>
/// It is no longer the default - <see cref="LatticeSplitActivityProbe"/> is -
/// but it is what <c>AddLatticeScalingSignal</c> registers when
/// <see cref="LatticeScalingSignalOptions.SplitAwareScaleIn"/> is set to
/// <see langword="false"/>, which is the correct choice for a deployment with
/// autonomic splitting disabled where the cluster query would be pure overhead.
/// </para>
/// </summary>
internal sealed class NoOpSplitActivityProbe : ISplitActivityProbe
{
    /// <inheritdoc />
    public ValueTask<bool> AnySplitInFlightAsync(CancellationToken cancellationToken)
        => ValueTask.FromResult(false);
}
