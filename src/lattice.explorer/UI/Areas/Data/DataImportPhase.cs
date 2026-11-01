namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>Where an import is (issue #5365).</summary>
internal enum DataImportPhase
{
    /// <summary>Not started.</summary>
    Ready,

    /// <summary>A seed's pre-flight pass: reading the whole file to check it before anything is written.</summary>
    Checking,

    /// <summary>Reading the file and writing its entries.</summary>
    Importing,

    /// <summary>Read to its end; every line was written or listed as refused.</summary>
    Succeeded,

    /// <summary>Stopped by the user; what was written before the stop is reported exactly.</summary>
    Stopped,

    /// <summary>Ended by a fault, or refused by the pre-flight pass.</summary>
    Failed,

    /// <summary>Abandoned because its circuit ended.</summary>
    Cancelled,
}
