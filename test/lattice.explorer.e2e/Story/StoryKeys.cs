namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// The ledger's fixed keys: the run-time facts one chapter hands another, named by the journey
/// contracts (#5371-#5399) as <c>ledger: ...</c>. A fact the estate assigns at run time (a
/// backup's id, a created task's id) cannot be a constant in <see cref="StoryNames"/>, so it
/// travels here.
/// </summary>
public static class StoryKeys
{
    /// <summary>olivia's appearance after A1 (Board, compact).</summary>
    public static readonly StoryKey<string> OliviaAppearance = new("olivia.appearance");

    /// <summary>sam's appearance preference after U5.</summary>
    public static readonly StoryKey<string> SamAppearance = new("sam.appearance");

    /// <summary>The task board's tree, as A5's install created it.</summary>
    public static readonly StoryKey<string> TaskBoardTree = new("taskboard.tree");

    /// <summary>The ids of the three tasks priya's team created in U3.</summary>
    public static readonly StoryKey<IReadOnlyList<string>> TaskBoardTasks = new("taskboard.tasks");

    /// <summary>The correction file U1 applied, which C1 re-imports.</summary>
    public static readonly StoryKey<string> CorrectionsFile = new("file.corrections");

    /// <summary>The backup id of <see cref="StoryNames.Contoso.Backups.OrdersBaseline"/> (A6).</summary>
    public static readonly StoryKey<string> OrdersBaselineBackup = Backup(StoryNames.Contoso.Backups.OrdersBaseline);

    /// <summary>The key holding the backup id the estate assigned to the backup named <paramref name="name"/>.</summary>
    /// <param name="name">A backup name from <see cref="StoryNames"/>.</param>
    public static StoryKey<string> Backup(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return new StoryKey<string>("backup." + name);
    }
}
