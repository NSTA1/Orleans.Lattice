namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>Where a download ticket is in its life.</summary>
internal enum ExplorerDownloadState
{
    /// <summary>Minted and waiting for the browser to present it.</summary>
    Ready = 0,

    /// <summary>Presented and redeemed; the file is streaming.</summary>
    Running = 1,

    /// <summary>The file finished streaming, complete or not; its trailer says which.</summary>
    Finished = 2,

    /// <summary>Not presented in time.</summary>
    Expired = 3,

    /// <summary>Presented without its circuit's binding, so it was burned unredeemed.</summary>
    Refused = 4,

    /// <summary>Withdrawn by its circuit before it finished: a sign-out, a new endpoint, or the circuit ending.</summary>
    Revoked = 5,
}
