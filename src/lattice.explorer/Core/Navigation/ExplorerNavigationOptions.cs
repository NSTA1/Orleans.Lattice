namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Head-level options controlling how the navigation panel presents itself. A
/// head registers an instance in DI; when none is registered the defaults here
/// apply, which is what the desktop head relies on.
/// </summary>
/// <remarks>
/// This type once also carried a per-area <c>EnableSchemaArea</c> switch. That
/// flag is retired: under the plugin model a head surfaces an area by
/// registering its plugin and withholds it by not registering it, so there is
/// no per-area option to set. What remains here is genuinely a presentation
/// concern of the navigation panel rather than an area toggle.
/// </remarks>
public sealed class ExplorerNavigationOptions
{
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
