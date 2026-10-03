using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITenantRuleLayer"/>: registered by
/// <c>AddLatticeTenancy</c> in place of the auth package's null layer, with
/// <see cref="IsActive"/> answering the live
/// <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/> flag.
/// While the flag is off the authorization engine builds no tenant partition and
/// never enters the tenant layer, exactly as with the null layer.
/// </summary>
/// <param name="flag">The silo's live delegated-access flag.</param>
internal sealed class TenancyTenantRuleLayer(DelegatedTenantAccessFlag flag) : ITenantRuleLayer
{
    private readonly DelegatedTenantAccessFlag _flag = flag ?? throw new ArgumentNullException(nameof(flag));

    /// <inheritdoc />
    /// <remarks>One field read; never allocates.</remarks>
    public bool IsActive => _flag.IsEnabled;
}
