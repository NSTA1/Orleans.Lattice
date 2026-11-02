using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The runtime view providers the test world's host registers (issue #5369), so the
/// Explorer's New view dialog has something to list: one described with a payload
/// form, and one described with free-form JSON.
/// </summary>
internal static class WorldViewProviders
{
    /// <summary>The provider whose payload is a form: it copies each entry with a marker appended to its value.</summary>
    public const string MarkedCopy = "uitests.marked-copy.v1";

    /// <summary>The provider whose payload is JSON: it copies each entry unchanged.</summary>
    public const string PlainCopy = "uitests.plain-copy.v1";

    /// <summary>Registers both providers on <paramref name="views"/>.</summary>
    /// <param name="views">The silo's view registration builder.</param>
    public static void Register(LatticeViewRegistrationBuilder views)
    {
        views.AddRuntimeProjectionProvider(
            MarkedCopy,
            new LatticeRuntimeViewProviderDescription(
                "Copies each entry, appending a marker to its value",
                [
                    new LatticeRuntimeViewProviderField("marker", "Marker")
                    {
                        Required = true,
                        Description = "Text appended to every copied value.",
                    },
                    new LatticeRuntimeViewProviderField("delayMs", "Delay per entry in ms")
                    {
                        Kind = LatticeRuntimeViewProviderFieldKind.Number,
                        Description = "Slows each projected entry, so a rebuild's progress can be watched.",
                    },
                ]),
            (_, context) =>
            {
                using var payload = JsonDocument.Parse(context.Payload);
                var marker = payload.RootElement.GetProperty("marker").GetString()
                    ?? throw new ArgumentException("The marker must be text.");
                var delay = payload.RootElement.TryGetProperty("delayMs", out var ms) ? Math.Clamp(ms.GetInt32(), 0, 2000) : 0;
                var suffix = Encoding.UTF8.GetBytes(" [" + marker + "]");
                return new LatticeViewDefinition(
                    context.ViewName,
                    new PredicateLatticeViewProjection(
                        valueSelector: value =>
                        {
                            if (delay > 0)
                            {
                                Thread.Sleep(delay);
                            }

                            return value is null ? null : [.. value, .. suffix];
                        },
                        valueSelectorVersion: "marker:" + marker));
            });

        views.AddRuntimeProjectionProvider(
            PlainCopy,
            new LatticeRuntimeViewProviderDescription("Copies every entry unchanged"),
            (_, context) =>
            {
                using var payload = JsonDocument.Parse(context.Payload);
                return new LatticeViewDefinition(context.ViewName, new PredicateLatticeViewProjection());
            });
    }
}
