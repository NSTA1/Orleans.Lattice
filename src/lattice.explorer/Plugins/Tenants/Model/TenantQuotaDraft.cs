using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// The editable form of a tenant's quota ceilings: one text field per dimension
/// plus the burst percent, held as text so an operator can clear a field to mean
/// <b>unbounded</b> rather than being forced to type a number.
/// <para>
/// A blank field is unbounded; <c>0</c> is a real ceiling permitting nothing.
/// The draft never collapses the two, in either direction: loading renders an
/// unbounded ceiling as blank, and saving turns a blank back into
/// <see langword="null"/>.
/// </para>
/// </summary>
public sealed class TenantQuotaDraft
{
    /// <summary>The message shown when a field holds something that is not a non-negative whole number.</summary>
    public const string InvalidLimitMessage =
        "Every ceiling must be blank (unbounded) or a whole number of zero or more. "
        + "Blank means no ceiling at all; zero is a real ceiling that permits nothing.";

    /// <summary>The message shown when the burst percent is negative.</summary>
    public const string InvalidBurstMessage = "The burst percent must be a whole number of zero or more.";

    /// <summary>The stored-bytes ceiling, blank for unbounded.</summary>
    public string MaxBytes { get; set; } = string.Empty;

    /// <summary>The live-key ceiling, blank for unbounded.</summary>
    public string MaxKeys { get; set; } = string.Empty;

    /// <summary>The resident-memory ceiling in bytes, blank for unbounded.</summary>
    public string MaxMemoryBytes { get; set; } = string.Empty;

    /// <summary>The owned-tree ceiling, blank for unbounded.</summary>
    public string MaxTreeCount { get; set; } = string.Empty;

    /// <summary>The operation-rate ceiling, blank for unbounded.</summary>
    public string MaxOpsPerSecond { get; set; } = string.Empty;

    /// <summary>
    /// The burst allowance as a percentage above each steady-state ceiling.
    /// Blank is read as no burst headroom, which is <c>0</c> and not an absence:
    /// the control API's burst percent is not nullable.
    /// </summary>
    public string BurstPercent { get; set; } = string.Empty;

    /// <summary>
    /// Loads <paramref name="limits"/> into the draft, rendering each unbounded
    /// ceiling as a blank field.
    /// </summary>
    /// <param name="limits">The ceilings currently in effect.</param>
    public void Load(ExplorerTenantQuotaLimits limits)
    {
        MaxBytes = TenantQuotaFormat.ToEditorText(limits.MaxBytes);
        MaxKeys = TenantQuotaFormat.ToEditorText(limits.MaxKeys);
        MaxMemoryBytes = TenantQuotaFormat.ToEditorText(limits.MaxMemoryBytes);
        MaxTreeCount = TenantQuotaFormat.ToEditorText(limits.MaxTreeCount);
        MaxOpsPerSecond = TenantQuotaFormat.ToEditorText(limits.MaxOpsPerSecond);
        BurstPercent = TenantQuotaFormat.ToEditorText(limits.BurstPercent);
    }

    /// <summary>
    /// Reads the draft back into ceilings, turning every blank field into
    /// <see langword="null"/> (unbounded).
    /// </summary>
    /// <param name="limits">The parsed ceilings, or <see langword="default"/> when the draft is invalid.</param>
    /// <param name="error">The reason the draft is invalid, or <see langword="null"/> when it is valid.</param>
    /// <returns><see langword="true"/> when every field parsed.</returns>
    public bool TryBuild(out ExplorerTenantQuotaLimits limits, out string? error)
    {
        limits = default;
        error = null;

        if (!TenantQuotaFormat.TryParseLimit(MaxBytes, out var bytes)
            || !TenantQuotaFormat.TryParseLimit(MaxKeys, out var keys)
            || !TenantQuotaFormat.TryParseLimit(MaxMemoryBytes, out var memory)
            || !TenantQuotaFormat.TryParseLimit(MaxTreeCount, out var trees)
            || !TenantQuotaFormat.TryParseLimit(MaxOpsPerSecond, out var ops))
        {
            error = InvalidLimitMessage;
            return false;
        }

        // The burst percent is not nullable on the control API, so a blank field
        // is no headroom rather than an absence. It is still range-checked, so a
        // negative value is refused here instead of being sent.
        if (!TenantQuotaFormat.TryParseLimit(BurstPercent, out var burst) || burst > int.MaxValue)
        {
            error = InvalidBurstMessage;
            return false;
        }

        limits = new ExplorerTenantQuotaLimits
        {
            MaxBytes = bytes,
            MaxKeys = keys,
            MaxMemoryBytes = memory,
            MaxTreeCount = trees,
            MaxOpsPerSecond = ops,
            BurstPercent = (int)(burst ?? 0),
        };

        return true;
    }
}
