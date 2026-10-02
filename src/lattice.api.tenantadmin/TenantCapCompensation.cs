using Microsoft.Extensions.Logging;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The withdrawal step of the D13 verify-and-compensate cap checks: after an
/// addition was written and the post-write verification found the tenant over its
/// cap, the addition this call made is withdrawn. The withdrawal is a second write
/// that can itself fail (for example
/// <see cref="TenantRegistryConcurrencyException"/> under sustained contention), so
/// it is retried a bounded number of times; it is safe to retry because it removes
/// one exact id, edge or entry and is idempotent. It runs uncancellable, so a
/// caller that gives up does not abandon a half-finished compensation.
/// </summary>
/// <remarks>
/// When every attempt fails the addition stays in place and the tenant stays over
/// its cap until it is removed. That is surfaced to the caller as a
/// <see cref="LatticeQuotaExceededException"/> whose message says the cap may be
/// exceeded until the addition is removed and whose <c>Current</c> is the
/// over-cap count observed, and it is logged at <see cref="LogLevel.Warning"/>
/// with the tenant and dimension only: no subject, group or rule id is logged.
/// </remarks>
internal static partial class TenantCapCompensation
{
    /// <summary>The bounded number of withdrawal attempts.</summary>
    internal const int MaxAttempts = 4;

    /// <summary>
    /// Withdraws this call's addition, then refuses the call with
    /// <see cref="LatticeQuotaExceededException"/>: the ordinary cap refusal when the
    /// withdrawal landed, or the overshoot refusal when it did not.
    /// </summary>
    /// <param name="withdraw">The idempotent withdrawal of exactly this call's addition.</param>
    /// <param name="logger">The facade's logger.</param>
    /// <param name="tenant">The tenant.</param>
    /// <param name="treeId">The tree the dimension is reported against.</param>
    /// <param name="dimension">The capped dimension.</param>
    /// <param name="cap">The cap.</param>
    /// <param name="observed">The over-cap count the verification observed.</param>
    /// <returns>Never returns normally.</returns>
    /// <exception cref="LatticeQuotaExceededException">Always.</exception>
    internal static async Task WithdrawAndRefuseAsync(
        Func<CancellationToken, Task> withdraw,
        ILogger logger,
        TenantId tenant,
        string treeId,
        string dimension,
        long cap,
        long observed)
    {
        Exception? last = null;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                await withdraw(CancellationToken.None).ConfigureAwait(false);
                last = null;
                break;
            }
            catch (Exception ex)
            {
                last = ex;
            }
        }

        if (last is null)
        {
            TenantAccessCaps.AdmitAddition(tenant, treeId, dimension, cap, cap);
        }

        LogWithdrawalFailed(logger, tenant.Value, dimension, cap, observed, MaxAttempts, last!.GetType().Name);
        throw new LatticeQuotaExceededException(
            $"Tenant '{tenant}' is over its {dimension} cap of {cap} (observed {observed}). This call's addition "
                + $"exceeded the cap and could not be withdrawn after {MaxAttempts} attempts, so the cap may stay "
                + "exceeded until the addition is removed; remove it and retry.",
            treeId,
            dimension,
            observed,
            cap,
            tenant.Value);
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Tenant {TenantId} is over its {Dimension} cap of {Cap} (observed {Observed}): withdrawing the over-cap addition failed after {Attempts} attempts ({FailureType}); the cap may stay exceeded until the addition is removed.")]
    private static partial void LogWithdrawalFailed(
        ILogger logger, string? tenantId, string dimension, long cap, long observed, int attempts, string failureType);
}
