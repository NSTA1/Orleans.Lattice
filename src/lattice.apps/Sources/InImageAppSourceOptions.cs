using System.Reflection;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Options for <see cref="InImageAppSource"/>: the declarative list of apps present in the image.
/// Registrations are read once, when the source is constructed.
/// </summary>
public sealed class InImageAppSourceOptions
{
    /// <summary>The apps present in the image, in registration order.</summary>
    public IList<InImageAppRegistration> Registrations { get; } = new List<InImageAppRegistration>();

    /// <summary>Adds a registration and returns these options for chaining.</summary>
    /// <param name="slug">The slug the app is resolved under.</param>
    /// <param name="assembly">The already-loaded assembly carrying the app and its manifest resource.</param>
    /// <param name="manifestResourceName">The manifest's embedded-resource name within <paramref name="assembly"/>.</param>
    public InImageAppSourceOptions Register(AppSlug slug, Assembly assembly, string manifestResourceName)
    {
        Registrations.Add(new InImageAppRegistration(slug, assembly, manifestResourceName));
        return this;
    }
}
