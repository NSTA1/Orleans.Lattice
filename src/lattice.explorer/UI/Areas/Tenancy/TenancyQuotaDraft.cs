using System.Globalization;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The editable form of a tenant's quota ceilings: one text field per dimension
/// and the burst percent. A blank ceiling means <b>no ceiling</b>; <c>0</c> is a
/// real ceiling that permits nothing, and the draft never confuses the two in
/// either direction. Byte ceilings accept a binary unit (<c>KiB</c> to
/// <c>TiB</c>), and every field ignores thousands separators. The four
/// delegated access caps read the other way round: a blank cap applies the
/// cluster's default cap, never no cap.
/// </summary>
internal sealed class TenancyQuotaDraft
{
    /// <summary>The error beside a ceiling that is not blank or a whole number of zero or more.</summary>
    public const string InvalidCeilingMessage = "Enter a whole number of zero or more, or leave it blank for no ceiling.";

    /// <summary>The error beside a byte ceiling that is not blank, a whole number, or a number with a binary unit.</summary>
    public const string InvalidByteCeilingMessage = "Enter a whole number of bytes, or a number with KiB, MiB, GiB or TiB, or leave it blank for no ceiling.";

    /// <summary>The error beside a burst percent that is not a whole number of zero or more.</summary>
    public const string InvalidBurstMessage = "Enter a whole number of zero or more. Blank means no burst allowance.";

    private static readonly (string Suffix, long Factor)[] ByteSuffixes =
    [
        ("kib", 1L << 10),
        ("mib", 1L << 20),
        ("gib", 1L << 30),
        ("tib", 1L << 40),
    ];

    /// <summary>The error beside a delegated access cap that is not blank or a whole number of zero or more.</summary>
    public const string InvalidCapMessage = "Enter a whole number of zero or more, or leave it blank for the default cap.";

    private readonly Dictionary<TenancyQuotaDimension, string> _ceilings = [];
    private readonly Dictionary<TenancyAccessCap, string> _caps = [];
    private readonly Dictionary<TenancyAccessCap, string> _capErrors = [];

    /// <summary>The text of <paramref name="dimension"/>'s ceiling field.</summary>
    /// <param name="dimension">The dimension.</param>
    public string this[TenancyQuotaDimension dimension]
    {
        get => _ceilings.TryGetValue(dimension, out var text) ? text : string.Empty;
        set => _ceilings[dimension] = value ?? string.Empty;
    }

    /// <summary>The text of <paramref name="cap"/>'s field; blank applies the default cap.</summary>
    /// <param name="cap">The delegated access cap.</param>
    public string this[TenancyAccessCap cap]
    {
        get => _caps.TryGetValue(cap, out var text) ? text : string.Empty;
        set => _caps[cap] = value ?? string.Empty;
    }

    /// <summary>The error of each delegated access cap field from the last <see cref="TryBuild"/>.</summary>
    public IReadOnlyDictionary<TenancyAccessCap, string> CapErrors => _capErrors;

    /// <summary>The text of the burst percent field.</summary>
    public string BurstPercent { get; set; } = string.Empty;

    /// <summary>A draft holding <paramref name="quotas"/>, with every unbounded ceiling blank.</summary>
    /// <param name="quotas">The quotas in effect.</param>
    public static TenancyQuotaDraft From(TenantQuotasDescriptor quotas)
    {
        var draft = new TenancyQuotaDraft();
        draft[TenancyQuotaDimension.Bytes] = Text(quotas.MaxBytes);
        draft[TenancyQuotaDimension.Keys] = Text(quotas.MaxKeys);
        draft[TenancyQuotaDimension.MemoryBytes] = Text(quotas.MaxMemoryBytes);
        draft[TenancyQuotaDimension.TreeCount] = Text(quotas.MaxTreeCount);
        draft[TenancyQuotaDimension.OpsPerSecond] = Text(quotas.MaxOpsPerSecond);
        draft.BurstPercent = quotas.BurstPercent == 0 ? string.Empty : quotas.BurstPercent.ToString(CultureInfo.InvariantCulture);
        foreach (var cap in TenancyAccessCaps.All)
        {
            draft[cap] = Text(TenancyAccessCaps.Of(quotas, cap));
        }

        return draft;
    }

    /// <summary>
    /// Reads the draft back into quotas, or reports the error beside each field
    /// that does not parse. Nothing is sent unless every field parses.
    /// </summary>
    /// <param name="quotas">The quotas, when the draft is valid.</param>
    /// <param name="errors">The error of each invalid field, keyed by dimension; the burst percent's is <see cref="BurstError"/>.</param>
    /// <returns><see langword="true"/> when every field parsed.</returns>
    public bool TryBuild(out TenantQuotasDescriptor quotas, out IReadOnlyDictionary<TenancyQuotaDimension, string> errors)
    {
        var found = new Dictionary<TenancyQuotaDimension, string>();
        var values = new Dictionary<TenancyQuotaDimension, long?>();
        foreach (var dimension in TenancyFormat.Dimensions)
        {
            var isBytes = dimension is TenancyQuotaDimension.Bytes or TenancyQuotaDimension.MemoryBytes;
            if (TryParseCeiling(this[dimension], isBytes, out var value))
            {
                values[dimension] = value;
            }
            else
            {
                found[dimension] = isBytes ? InvalidByteCeilingMessage : InvalidCeilingMessage;
            }
        }

        BurstError = TryParseCeiling(BurstPercent, false, out var burst) && (burst ?? 0) <= int.MaxValue ? null : InvalidBurstMessage;
        errors = found;

        _capErrors.Clear();
        var caps = new Dictionary<TenancyAccessCap, long?>();
        foreach (var cap in TenancyAccessCaps.All)
        {
            if (TryParseCeiling(this[cap], false, out var value))
            {
                caps[cap] = value;
            }
            else
            {
                _capErrors[cap] = InvalidCapMessage;
            }
        }

        if (found.Count > 0 || BurstError is not null || _capErrors.Count > 0)
        {
            quotas = default;
            return false;
        }

        quotas = new TenantQuotasDescriptor
        {
            MaxBytes = values[TenancyQuotaDimension.Bytes],
            MaxKeys = values[TenancyQuotaDimension.Keys],
            MaxMemoryBytes = values[TenancyQuotaDimension.MemoryBytes],
            MaxTreeCount = values[TenancyQuotaDimension.TreeCount],
            MaxOpsPerSecond = values[TenancyQuotaDimension.OpsPerSecond],
            BurstPercent = (int)(burst ?? 0),
            MaxGroups = caps[TenancyAccessCap.Groups],
            MaxMembershipEdges = caps[TenancyAccessCap.MembershipEdges],
            MaxMemberSubjects = caps[TenancyAccessCap.MemberSubjects],
            MaxTenantRules = caps[TenancyAccessCap.TenantRules],
        };
        return true;
    }

    /// <summary>The burst percent's error from the last <see cref="TryBuild"/>, or <see langword="null"/>.</summary>
    public string? BurstError { get; private set; }

    /// <summary>
    /// Parses one ceiling: blank is <see langword="null"/> (no ceiling), anything
    /// else must be a whole number of zero or more, optionally with a binary unit
    /// when <paramref name="bytes"/>.
    /// </summary>
    /// <param name="text">The field text.</param>
    /// <param name="bytes">Whether a binary unit is accepted.</param>
    /// <param name="value">The ceiling, or <see langword="null"/> for none.</param>
    /// <returns><see langword="true"/> when the text parsed.</returns>
    public static bool TryParseCeiling(string? text, bool bytes, out long? value)
    {
        value = null;
        var trimmed = (text ?? string.Empty).Trim().Replace(",", string.Empty, StringComparison.Ordinal).Replace("_", string.Empty, StringComparison.Ordinal);
        if (trimmed.Length == 0)
        {
            return true;
        }

        var factor = 1L;
        if (bytes)
        {
            foreach (var (suffix, multiplier) in ByteSuffixes)
            {
                if (trimmed.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                {
                    trimmed = trimmed[..^suffix.Length].TrimEnd();
                    factor = multiplier;
                    break;
                }
            }
        }

        if (!long.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var number)
            || number > long.MaxValue / factor)
        {
            return false;
        }

        value = number * factor;
        return true;
    }

    private static string Text(long? value) => value is { } number ? number.ToString(CultureInfo.InvariantCulture) : string.Empty;
}
