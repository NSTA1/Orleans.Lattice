using System.Collections.Concurrent;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

internal static class MaterialisedViewRuntimeProjectionProvider
{
    private const string ProviderKey = "tests.materialised-view.runtime.v1";

    private static readonly ConcurrentDictionary<string, LatticeViewDefinition> Definitions =
        new(StringComparer.Ordinal);

    public static void Configure(LatticeViewRegistrationBuilder views) =>
        views.AddRuntimeProjectionProvider(
            ProviderKey,
            (_, context) => Definitions.TryGetValue(context.ViewName, out var definition)
                ? definition
                : throw new InvalidOperationException(
                    $"No test projection is registered for runtime view '{context.ViewName}'."));

    public static LatticeRuntimeViewProjectionDescriptor DescriptorFor(LatticeViewDefinition definition)
    {
        Definitions[definition.ViewName] = definition;
        return new LatticeRuntimeViewProjectionDescriptor(ProviderKey, []);
    }
}
