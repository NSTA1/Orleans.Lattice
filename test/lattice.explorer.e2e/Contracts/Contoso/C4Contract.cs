using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// Running the console (C4, #5392): a read-only chapter of hosting proofs. It establishes
/// nothing, and its verifier asserts the offboarded estate C3 left is unchanged.
/// </summary>
public sealed class C4Contract() : ChapterContract("C4")
{
    /// <inheritdoc />
    public override Task EstablishAsync(ChapterContext context) => Task.CompletedTask;

    /// <inheritdoc />
    public override Task VerifyAsync(ChapterContext context) => VerifyConsumesAsync(context);
}
