namespace Orleans.Lattice.Api.Apps.Grpc;

internal static partial class LatticeAppsGrpcMarshallers
{
    /// <summary>
    /// The per-method bound of an app bridge request: the bridge's 64 KiB value cap plus an allowance for the
    /// target, key and framing. Every bridge request is bounded by it, so an oversized request is refused before
    /// it is deserialized.
    /// </summary>
    public const int MaxBridgeRequestBytes = (64 * 1024) + (8 * 1024);

    /// <summary>
    /// The per-method bound of an app bridge response: the bridge's 1 MiB response budget plus an allowance for
    /// framing. The budget is measured with base64 values, which are larger than the serialized bytes.
    /// </summary>
    public const int MaxBridgeResponseBytes = (1024 * 1024) + (64 * 1024);
}
