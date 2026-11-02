using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Views;

/// <summary>
/// The built-in runtime provider for change-history views (#5611), registered by
/// <c>AddLatticeViews</c> under <see cref="LatticeHistoryView.ProviderKey"/>, so a
/// host, the tree-administration facade, or an operator console can create a
/// durable change history on a tree without registering a provider of its own.
/// </summary>
internal static class HistoryRuntimeViewProvider
{
    /// <summary>What an operator console lists the provider with.</summary>
    public static readonly LatticeRuntimeViewProviderDescription Description = new(
        "Keeps every revision of every key in the source tree, including values later overwritten or deleted, until the view is dropped or the tree's history retention window expires them");

    /// <summary>
    /// Builds the history definition for <paramref name="context"/>. The payload is
    /// either empty or a JSON object with no properties; anything else is refused,
    /// so a later version can give the object meaning without misreading an older one.
    /// </summary>
    /// <param name="services">The silo service provider.</param>
    /// <param name="context">The view being created or re-hydrated.</param>
    /// <returns>The history-view definition.</returns>
    public static LatticeViewDefinition Create(IServiceProvider services, LatticeRuntimeViewProjectionContext context)
    {
        ThrowIfInvalidPayload(context.PayloadSpan);
        return new LatticeViewDefinition(
            context.ViewName,
            services.GetRequiredService<HistoryLatticeViewProjection>(),
            accumulative: true);
    }

    internal static void ThrowIfInvalidPayload(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty)
        {
            return;
        }

        var reader = new Utf8JsonReader(payload);
        try
        {
            if (reader.Read()
                && reader.TokenType == JsonTokenType.StartObject
                && reader.Read()
                && reader.TokenType == JsonTokenType.EndObject
                && !reader.Read())
            {
                return;
            }
        }
        catch (JsonException)
        {
        }

        throw new ArgumentException(
            "A change-history view takes no settings: its payload must be empty or an empty JSON object ({}).",
            nameof(payload));
    }
}
