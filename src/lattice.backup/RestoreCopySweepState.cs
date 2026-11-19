namespace Orleans.Lattice.Backup;

/// <summary>Persistent state of the estate-wide restore-copy sweep (issue #6103), for observability.</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.RestoreCopySweepState)]
internal sealed class RestoreCopySweepState
{
    /// <summary>When the most recent sweep completed, or <see langword="null"/> when none has.</summary>
    [Id(0)] public DateTimeOffset? LastSweepUtc { get; set; }

    /// <summary>How many trees the most recent sweep scheduled.</summary>
    [Id(1)] public int LastSweepTreeCount { get; set; }

    /// <summary>The period the sweep reminder was last armed with, so a restart keeps an unchanged schedule.</summary>
    [Id(2)] public TimeSpan? ArmedPeriod { get; set; }
}
