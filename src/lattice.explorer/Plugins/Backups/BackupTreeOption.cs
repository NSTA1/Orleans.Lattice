namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// One tree the Backups plugin may capture: the plugin's own projection of a
/// cluster tree, carrying only the three facts the capture picker renders.
/// <para>
/// The plugin declares this shape rather than consuming the Explorer's
/// navigation catalog record directly, so the surface it reads is stated in its
/// own source and cannot silently widen when the shared catalog record grows a
/// field (epic decision D3).
/// </para>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>: the picker holds one per visible tree and reads
/// them on the render path, so the projection costs one array rather than an
/// object per tree.
/// </para>
/// </summary>
/// <param name="Id">The tree id a capture targets.</param>
/// <param name="RestoreShadowOfTreeId">
/// The logical tree this one is a shadow-cutover restore shadow of, or
/// <see langword="null"/> for an ordinary tree.
/// </param>
public readonly record struct BackupTreeOption(string Id, string? RestoreShadowOfTreeId)
{
    /// <summary><see langword="true"/> when this tree is a shadow-cutover restore shadow.</summary>
    public bool IsRestoreShadow => RestoreShadowOfTreeId is not null;
}
