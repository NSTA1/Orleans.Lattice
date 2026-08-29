namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// How one quota dimension's reading should be presented. The four members are
/// the four combinations of "does it have a ceiling?" and "did the reading carry
/// a consumption figure?", kept distinct because collapsing any pair of them
/// renders a bar that lies.
/// <para>
/// A renderer branches on this rather than substituting <c>0</c> for a missing
/// ceiling or a missing sample: an unbounded dimension is not one capped at
/// nothing, and an unmeasured one is not one measured at nothing.
/// </para>
/// </summary>
public enum TenantQuotaReadingState
{
    /// <summary>
    /// The dimension has a ceiling and the reading carried a consumption figure,
    /// so consumption against the ceiling is a real fraction and a bar is
    /// meaningful. This is the only state in which a bar may be drawn.
    /// </summary>
    Bounded = 0,

    /// <summary>
    /// The dimension has no ceiling at all, though consumption was measured.
    /// Render the consumption alongside "unlimited"; a bar would have to invent
    /// a denominator, and drawing a full one would say the opposite of the
    /// truth.
    /// </summary>
    Unlimited = 1,

    /// <summary>
    /// The dimension has a ceiling but the reading carried no consumption
    /// figure - the operation-rate dimension normally reports exactly this,
    /// because the sampler takes no rate sample. Render the ceiling alongside
    /// "not measured"; an empty bar would read as "you are using none of your
    /// rate limit" when the truth is that nothing is being measured.
    /// </summary>
    NotMeasured = 2,

    /// <summary>
    /// The dimension has neither a ceiling nor a consumption figure, so nothing
    /// at all is asserted about it. Render both sides as absent rather than
    /// implying an idle, uncapped dimension.
    /// </summary>
    Unknown = 3,
}
