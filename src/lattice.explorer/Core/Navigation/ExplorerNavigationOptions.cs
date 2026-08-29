namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Head-level options controlling which registered areas the app-level switcher
/// actually surfaces. Every area is still registered in
/// <see cref="AppAreas.Ordered"/> (and its services stay wired in DI); this only
/// decides whether an opt-in area is shown. A head registers an instance in DI;
/// when none is registered the switcher falls back to the defaults here.
/// </summary>
public sealed class ExplorerNavigationOptions
{
    /// <summary>
    /// When <see langword="true"/>, the schema-management area is shown in the
    /// switcher. When <see langword="false"/> (the default), the area is hidden:
    /// its tab is not rendered and it cannot be activated, though the schema
    /// control services stay registered so it can be re-surfaced without new
    /// wiring. The area is withheld by default because its versioning UI cannot
    /// yet express what differs between schema versions (see the Explorer
    /// re-surface tracking issue).
    /// </summary>
    public bool EnableSchemaArea { get; set; }

    /// <summary>
    /// When <see langword="true"/> (the default), the navigation panel offers the
    /// connection-settings affordance that edits the head's endpoint configuration.
    /// When <see langword="false"/>, the affordance is withheld because the head's
    /// configuration store does not accept browser writes.
    /// </summary>
    /// <remarks>
    /// This is presentation only: withholding a rendered control does not make the
    /// operation unreachable, since a component's event handlers remain invokable
    /// over the circuit. The enforcing check lives at the configuration store, and
    /// this flag exists so a head that refuses the write does not advertise it.
    /// </remarks>
    public bool AllowEndpointConfiguration { get; set; } = true;
}
