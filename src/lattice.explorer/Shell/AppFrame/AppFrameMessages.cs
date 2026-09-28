using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// Builds the host-to-frame control and event messages of AppKit protocol v1 as JSON text.
/// The host module parses them and posts the objects over the port; asset bytes are
/// attached in the browser, never embedded here.
/// </summary>
internal static class AppFrameMessages
{
    /// <summary>
    /// The <c>lattice.bundle</c> message without asset bytes: appearance, entry, styles,
    /// scripts in manifest order, the bundle digest, and every asset's media type and digest.
    /// </summary>
    /// <param name="bundle">The verified bundle.</param>
    /// <param name="appearance">The current appearance.</param>
    /// <returns>The JSON text.</returns>
    public static string Bundle(AppFrameBundle bundle, AppFrameAppearance appearance)
    {
        ArgumentNullException.ThrowIfNull(bundle);
        ArgumentNullException.ThrowIfNull(appearance);
        var ui = bundle.Launch.Ui;
        return Write((bundle, ui, appearance: appearance.Sanitise()), static (writer, state) =>
        {
            writer.WriteString("type", AppFrameProtocol.Bundle);
            writer.WriteNumber("protocol", AppFrameProtocol.Version);
            WriteAppearance(writer, "appearance", state.appearance);
            writer.WriteStartObject("bundle");
            writer.WriteString("entry", state.ui.Entry);
            writer.WriteStartArray("styles");
            if (!state.ui.Styles.IsDefault)
            {
                foreach (var style in state.ui.Styles)
                {
                    writer.WriteStringValue(style);
                }
            }

            writer.WriteEndArray();
            writer.WriteStartArray("scripts");
            if (!state.ui.Scripts.IsDefault)
            {
                foreach (var script in state.ui.Scripts)
                {
                    writer.WriteStartObject();
                    writer.WriteString("path", script.Path);
                    writer.WriteBoolean("module", script.Module);
                    writer.WriteEndObject();
                }
            }

            writer.WriteEndArray();
            writer.WriteString("bundleDigest", state.ui.BundleDigest);
            writer.WriteStartObject("assets");
            foreach (var asset in state.bundle.Assets)
            {
                writer.WriteStartObject(asset.Path);
                writer.WriteString("mediaType", asset.MediaType);
                writer.WriteString("digest", asset.Digest);
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        });
    }

    /// <summary>The <c>nav.changed</c> event.</summary>
    /// <param name="path">The in-app path the host navigated to.</param>
    /// <returns>The JSON text.</returns>
    public static string NavChanged(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return Write(path, static (writer, value) =>
        {
            writer.WriteString("type", AppFrameProtocol.NavChanged);
            writer.WriteStartObject("data");
            writer.WriteString("path", value);
            writer.WriteEndObject();
        });
    }

    /// <summary>The <c>context.changed</c> event.</summary>
    /// <param name="appearance">The new appearance.</param>
    /// <returns>The JSON text.</returns>
    public static string ContextChanged(AppFrameAppearance appearance)
    {
        ArgumentNullException.ThrowIfNull(appearance);
        return Write(appearance.Sanitise(), static (writer, value) =>
        {
            writer.WriteString("type", AppFrameProtocol.ContextChanged);
            WriteAppearance(writer, "data", value);
        });
    }

    private static void WriteAppearance(Utf8JsonWriter writer, string name, AppFrameAppearance appearance)
    {
        writer.WriteStartObject(name);
        writer.WriteString("theme", appearance.Theme);
        writer.WriteString("contrast", appearance.Contrast);
        writer.WriteString("density", appearance.Density);
        writer.WriteBoolean("reducedMotion", appearance.ReducedMotion);
        writer.WriteEndObject();
    }

    private static string Write<TState>(TState state, Action<Utf8JsonWriter, TState> body)
    {
        var buffer = new ArrayBufferWriter<byte>(512);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            body(writer, state);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
