using Orleans.Lattice;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Internal seam that supplies the monotonically increasing
/// <see cref="HybridLogicalClock"/> stamp a tenant-administration write uses to
/// author a last-writer-wins register update on a <c>TenantRecord</c>. The
/// tenant registry's stored status register keeps its incumbent stamp internal
/// (unreadable from this add-on), so the facade cannot read-then-supersede; it
/// instead stamps every write with a strictly increasing clock from this seam,
/// which guarantees each successive write supersedes the last for a single
/// control-plane writer.
/// </summary>
/// <remarks>
/// Abstracted behind an interface so tests inject a deterministic, strictly
/// increasing fake clock instead of depending on wall-time.
/// </remarks>
internal interface ITenantAdminClock
{
    /// <summary>
    /// Returns the next clock stamp, strictly greater than every stamp this
    /// instance has previously returned.
    /// </summary>
    /// <returns>The next monotonically increasing clock stamp.</returns>
    HybridLogicalClock Next();
}
