using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Fails silo startup fast when <see cref="LatticeTenancyOptions"/> carries an
/// invalid value, rather than deferring the error to the first registry
/// operation. Registered with <c>ValidateOnStart()</c> so a misconfiguration is
/// reported at host build time with an actionable message.
/// </summary>
/// <remarks>
/// This validates the <em>options</em> surface only. The per-tenant
/// <see cref="TenantQuotas.BurstPercent"/> is authored data stored per record,
/// not startup configuration, so it is guarded where a record is authored
/// (<see cref="TenantRecord.Create"/> / <see cref="TenantRecord.SetQuotas"/>)
/// rather than here.
/// </remarks>
internal sealed class LatticeTenancyOptionsValidator : IValidateOptions<LatticeTenancyOptions>
{
    /// <summary>
    /// The longest duration a timer-backed wait accepts: <c>0xFFFFFFFE</c>
    /// milliseconds (about 49.7 days). A longer value would pass a lower-bound check
    /// and then throw from every <c>Task.Delay</c> that arms it.
    /// </summary>
    internal static readonly TimeSpan MaxTimerDuration = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeTenancyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.HistoryRetentionWindow is { } window && window <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                "LatticeTenancyOptions.HistoryRetentionWindow must be strictly positive when " +
                $"supplied, but was {window}. Leave it null for no age bound.");
        }

        if (options.PolicySnapshotLeaseDuration <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"LatticeTenancyOptions.{nameof(LatticeTenancyOptions.PolicySnapshotLeaseDuration)} must be strictly " +
                $"positive, but was {options.PolicySnapshotLeaseDuration}.");
        }

        if (options.PolicySnapshotLeaseDuration > MaxTimerDuration)
        {
            return ValidateOptionsResult.Fail(
                $"LatticeTenancyOptions.{nameof(LatticeTenancyOptions.PolicySnapshotLeaseDuration)} must be at most " +
                $"{MaxTimerDuration}, the longest duration a timer can wait, but was {options.PolicySnapshotLeaseDuration}.");
        }

        return ValidateOptionsResult.Success;
    }
}
