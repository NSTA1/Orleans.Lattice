namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// How one quota dimension must be presented, resolved from the two
/// distinctions the reading keeps that a naive <c>long</c>/<c>0</c> model would
/// flatten: whether the dimension has a ceiling at all, and whether the reading
/// carries a consumption figure for it.
/// <para>
/// Branching on this - rather than null-coalescing either figure to zero - is
/// what stops the surface rendering a bar that lies.
/// </para>
/// </summary>
public enum TenantQuotaPresentation
{
    /// <summary>
    /// A real ceiling and a real usage sample, so a proportional bar is
    /// meaningful. A ceiling of exactly <c>0</c> lands here too: it is a genuine
    /// cap permitting nothing, and any usage against it is already overage.
    /// </summary>
    Bar = 0,

    /// <summary>
    /// No ceiling at all, and a usage sample. The figure is shown on its own
    /// with no bar, because there is nothing to be a proportion of. This is
    /// <em>not</em> a limit of zero.
    /// </summary>
    UnboundedWithUsage = 1,

    /// <summary>
    /// A real ceiling, but the reading carries no usage sample for this
    /// dimension - the local sampler measures stored bytes, live keys, resident
    /// memory, and owned trees, but no operation rate. The ceiling is shown and
    /// the consumption is reported as not measured, never as a measured zero.
    /// </summary>
    UnmeasuredWithLimit = 2,

    /// <summary>
    /// Neither a ceiling nor a usage sample: nothing is known about this
    /// dimension, which is distinct from both "unlimited" and "unused".
    /// </summary>
    Unknown = 3,
}
