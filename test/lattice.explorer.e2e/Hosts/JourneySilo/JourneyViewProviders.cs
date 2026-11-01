using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The runtime view providers the journey silo registers, so the Explorer's New view dialog
/// can build the story's views: <see cref="OrderStatus"/> projects each JSON order to its
/// status, which is how A3 creates <c>orders-by-status</c> over <c>factory-orders</c>, and
/// <see cref="ChangeHistory"/> is the durable change history A3 enables on it before the seed.
/// </summary>
internal static class JourneyViewProviders
{
    /// <summary>The provider key of the order-status projection.</summary>
    public const string OrderStatus = "order-status";

    /// <summary>
    /// The provider key of the durable per-key change history (<see cref="LatticeHistoryView"/>):
    /// a view of this provider over a tree is what keeps every revision of every key beyond the
    /// write-ahead log, so History can show a key as it stood at any earlier time.
    /// </summary>
    public const string ChangeHistory = "change-history";

    /// <summary>The JSON property projected when the payload names none.</summary>
    public const string DefaultField = "status";

    /// <summary>Registers every provider on <paramref name="views"/>.</summary>
    /// <param name="views">The silo's view registration builder.</param>
    public static void Register(LatticeViewRegistrationBuilder views)
    {
        ArgumentNullException.ThrowIfNull(views);
        views.AddRuntimeProjectionProvider(
            OrderStatus,
            new LatticeRuntimeViewProviderDescription(
                "Projects each JSON order to its status",
                [
                    new LatticeRuntimeViewProviderField("field", "Status property")
                    {
                        Description = "The JSON property holding the status; 'status' when left empty.",
                    },
                ]),
            (_, context) =>
            {
                var field = FieldOf(context.Payload);
                return new LatticeViewDefinition(
                    context.ViewName,
                    new PredicateLatticeViewProjection(
                        valueSelector: value => Project(value, field),
                        valueSelectorVersion: "order-status:" + field));
            });

        // The library's change-history view, offered as a runtime provider so the Explorer's
        // New view dialog can enable durable history on a tree (A3 does, before the seed).
        views.AddRuntimeProjectionProvider(
            ChangeHistory,
            new LatticeRuntimeViewProviderDescription("Keeps every revision of every key, for History and Show as of", []),
            (services, context) => LatticeHistoryView.Definition(context.ViewName, services));
    }

    /// <summary>The status <paramref name="value"/> carries in <paramref name="field"/>, or <see langword="null"/> to leave the entry out.</summary>
    /// <param name="value">A source value.</param>
    /// <param name="field">The JSON property to project.</param>
    internal static byte[]? Project(byte[]? value, string field)
    {
        if (value is null)
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(value);
            return document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty(field, out var status)
                && status.ValueKind == JsonValueKind.String
                    ? Encoding.UTF8.GetBytes(status.GetString()!)
                    : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string FieldOf(ReadOnlyMemory<byte> payload)
    {
        if (payload.IsEmpty)
        {
            return DefaultField;
        }

        using var document = JsonDocument.Parse(payload);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("The order-status payload must be a JSON object.");
        }

        return document.RootElement.TryGetProperty("field", out var field)
            && field.ValueKind == JsonValueKind.String
            && field.GetString() is { Length: > 0 } name
                ? name
                : DefaultField;
    }
}
