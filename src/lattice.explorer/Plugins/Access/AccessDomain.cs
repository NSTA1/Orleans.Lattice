using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The default <see cref="IAccessDomain"/>: the plugin's own services, gathered
/// behind the one contract its views are handed.
/// </summary>
/// <remarks>
/// The two <c>Create*</c> members exist so a view can own a per-instance model
/// without reaching for the container: the domain is the only thing the plugin
/// holds, so it is also the only place a factory may live.
/// </remarks>
/// <param name="membership">The membership admin service.</param>
/// <param name="policy">The policy admin service.</param>
/// <param name="catalog">The tree catalog reader.</param>
/// <param name="gate">The plugin's own access gate, which publishes the active authentication mode.</param>
/// <param name="debounce">
/// Creates the per-picker search debounce. A factory rather than an instance,
/// because each picker owns its own single in-flight timer.
/// </param>
internal sealed class AccessDomain(
    IMembershipAdminService membership,
    IPolicyAdminService policy,
    ICatalogReader catalog,
    IAuthAdminCapabilityService gate,
    Func<ISubjectSearchDebounce> debounce) : IAccessDomain
{
    private readonly IAuthAdminCapabilityService _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    private readonly Func<ISubjectSearchDebounce> _debounce =
        debounce ?? throw new ArgumentNullException(nameof(debounce));

    /// <inheritdoc />
    public IMembershipAdminService Membership { get; } =
        membership ?? throw new ArgumentNullException(nameof(membership));

    /// <inheritdoc />
    public IPolicyAdminService Policy { get; } = policy ?? throw new ArgumentNullException(nameof(policy));

    /// <inheritdoc />
    public ICatalogReader Catalog { get; } = catalog ?? throw new ArgumentNullException(nameof(catalog));

    /// <inheritdoc />
    public ExplorerAccessAuthenticationMode AuthenticationMode => _gate.AuthenticationMode;

    /// <inheritdoc />
    public PrincipalLabelResolver CreateLabelResolver() => new(Membership);

    /// <inheritdoc />
    public SubjectPickerModel CreateSubjectPicker() => new(Membership, _debounce());
}
