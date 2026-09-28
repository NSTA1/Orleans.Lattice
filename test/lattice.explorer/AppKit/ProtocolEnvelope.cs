using System.Text;
using Orleans.Lattice.Explorer.AppKit;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The byte bounds on whole envelopes that JSON Schema cannot express, measured
/// the way both ends of the port measure them: the UTF-8 length of the JSON.
/// </summary>
internal static class ProtocolEnvelope
{
    /// <summary>Whether a request envelope's JSON is within <see cref="AppKitProtocol.Limits.MaxRequestBytes"/>.</summary>
    public static bool IsWithinRequestBound(string json) =>
        Encoding.UTF8.GetByteCount(json) <= AppKitProtocol.Limits.MaxRequestBytes;

    /// <summary>Whether a response's JSON is within <see cref="AppKitProtocol.Limits.MaxResponseBytes"/>.</summary>
    public static bool IsWithinResponseBound(string json) =>
        Encoding.UTF8.GetByteCount(json) <= AppKitProtocol.Limits.MaxResponseBytes;
}
