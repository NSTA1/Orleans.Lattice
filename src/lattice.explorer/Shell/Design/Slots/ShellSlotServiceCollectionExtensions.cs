using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Explorer.Shell.Design.Slots;

/// <summary>Registers contributions to the Shell's named chrome slots.</summary>
internal static class ShellSlotServiceCollectionExtensions
{
    /// <summary>
    /// Contributes <typeparamref name="TComponent"/> to the chrome slot named
    /// <paramref name="name"/>. Contributing the same component to the same slot
    /// twice registers it once.
    /// </summary>
    /// <typeparam name="TComponent">The parameterless component to render in the slot.</typeparam>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="name">One of <see cref="ShellSlotNames"/>.</param>
    /// <param name="order">The position within the slot; lower renders first.</param>
    /// <returns>The same service collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> is not a declared slot.</exception>
    public static IServiceCollection AddShellSlot<TComponent>(
        this IServiceCollection services,
        string name,
        int order = 0)
        where TComponent : IComponent
    {
        ArgumentNullException.ThrowIfNull(services);
        var slot = new ShellSlot<TComponent>(name, order);

        var alreadyContributed = services.Any(descriptor =>
            descriptor.ServiceType == typeof(IShellSlot)
            && descriptor.ImplementationInstance is IShellSlot existing
            && string.Equals(existing.Name, slot.Name, StringComparison.Ordinal)
            && existing.ComponentType == slot.ComponentType);

        if (!alreadyContributed)
        {
            services.AddSingleton<IShellSlot>(slot);
        }

        return services;
    }
}
