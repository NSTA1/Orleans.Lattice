namespace Orleans.Lattice.Scaling;

/// <summary>
/// Internal seam reporting whether any adaptive shard split is currently in
/// flight anywhere in the cluster. The scale-in safety gate consults this: a
/// split in flight suppresses scale-in, because relocating load off a silo while
/// a shard is mid-split risks stranding the split's in-flight work.
/// <para>
/// The gate only affects scale-<em>in</em>; scale-out is never influenced by
/// this probe.
/// </para>
/// <para>
/// The default registration is <see cref="LatticeSplitActivityProbe"/>, which
/// reads the cluster's split-activity snapshot from
/// <see cref="Orleans.Lattice.ILatticeAdmin.GetSplitActivityAsync"/>. Setting
/// <see cref="LatticeScalingSignalOptions.SplitAwareScaleIn"/> to
/// <see langword="false"/> substitutes <see cref="NoOpSplitActivityProbe"/> to
/// make the axis inert.
/// </para>
/// </summary>
internal interface ISplitActivityProbe
{
    /// <summary>
    /// Returns <see langword="true"/> when at least one adaptive shard split is
    /// currently in flight in the cluster; otherwise <see langword="false"/>.
    /// <para>
    /// Read once per sample tick, off the scrape path, alongside the compute and
    /// storage collectors - so an implementation may make a single cheap cluster
    /// call, but must not fan out per tree or per shard. It must never throw: a
    /// probe that cannot determine split activity reports <see langword="false"/>
    /// rather than failing the sample, so a degraded split-activity source cannot
    /// wedge scale-in permanently.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>Whether any shard split is in flight cluster-wide.</returns>
    ValueTask<bool> AnySplitInFlightAsync(CancellationToken cancellationToken);
}
