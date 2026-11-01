namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>The outcome of presenting a download token, as the endpoint sees it.</summary>
internal enum ExplorerDownloadRedemption
{
    /// <summary>The token was valid: the ticket is now running and must be streamed.</summary>
    Redeemed = 0,

    /// <summary>No ticket carries the token: never minted, already presented, or revoked.</summary>
    Unknown = 1,

    /// <summary>The ticket's lifetime had passed. It is burned.</summary>
    Expired = 2,

    /// <summary>The token was presented without its circuit's binding. It is burned.</summary>
    WrongCircuit = 3,
}
