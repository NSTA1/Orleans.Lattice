namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a WAL shard activation resolved from the durable placement pin: the
/// provider backing its partition, the placement version and key it resolved
/// them under, and - when a placement move still holds the partition on that
/// provider - the UTC instant the move's durable fence lapses (issue #4525).
/// </summary>
/// <param name="Provider">The resolved WAL storage provider.</param>
/// <param name="PlacementVersion">The placement version the provider was resolved at.</param>
/// <param name="ProviderKey">The catalog key the provider was resolved under.</param>
/// <param name="FenceExpiresUtcTicks">
/// The UTC tick at which the partition's move fence lapses, or
/// <see langword="null"/> when the activation may serve appends.
/// </param>
internal readonly record struct WalShardPlacementResolution(
    IWalStorageProvider Provider,
    long PlacementVersion,
    string ProviderKey,
    long? FenceExpiresUtcTicks);
