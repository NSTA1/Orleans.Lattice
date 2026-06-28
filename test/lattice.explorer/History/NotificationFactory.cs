using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Tests.History;

/// <summary>
/// Builds <see cref="StateChangeNotification"/> fixtures for the live-follow
/// tests, defaulting every field so a test sets only what it asserts on.
/// </summary>
internal static class NotificationFactory
{
    public static StateChangeNotification Set(
        string key,
        long ticks,
        string? position = null,
        string treeId = "tree-1") => new()
    {
        TreeId = treeId,
        Key = key,
        Kind = StateChangeKind.Set,
        Hlc = Hlc(ticks),
        Category = MutationCategory.User,
        Position = position ?? $"pos-{ticks}",
    };

    public static StateChangeNotification Delete(
        string key,
        long ticks,
        string? position = null,
        string treeId = "tree-1") => new()
    {
        TreeId = treeId,
        Key = key,
        Kind = StateChangeKind.Delete,
        Hlc = Hlc(ticks),
        Category = MutationCategory.User,
        Position = position ?? $"pos-{ticks}",
    };

    public static StateChangeNotification DeleteRange(
        string startInclusive,
        string endExclusive,
        long ticks,
        string? position = null,
        string treeId = "tree-1") => new()
    {
        TreeId = treeId,
        Key = startInclusive,
        EndExclusiveKey = endExclusive,
        Kind = StateChangeKind.DeleteRange,
        Hlc = Hlc(ticks),
        Category = MutationCategory.User,
        Position = position ?? $"pos-{ticks}",
    };

    public static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };
}
