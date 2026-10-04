using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup;

/// <summary>A captured write, ready to emit, with the descriptor fields the manifest records for it.</summary>
/// <param name="Entry">The entry as it is streamed.</param>
/// <param name="DescriptorMode">The merge mode its key descriptor records.</param>
/// <param name="Origin">Its normalized origin, or <see langword="null"/> when unstamped.</param>
internal sealed record CapturedWrite(LwwEntry Entry, BackupKeyMergeMode DescriptorMode, string? Origin);
