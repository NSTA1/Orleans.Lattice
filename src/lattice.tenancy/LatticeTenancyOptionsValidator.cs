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

        return ValidateOptionsResult.Success;
    }
}
