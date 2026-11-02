using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// The auditor's review (U4, #5388): a read-only chapter. It establishes nothing, and its
/// verifier asserts the estate dana read is unchanged: every chapter it consumes still verifies.
/// </summary>
public sealed class U4Contract() : ChapterContract("U4")
{
    /// <inheritdoc />
    public override Task EstablishAsync(ChapterContext context) => Task.CompletedTask;

    /// <inheritdoc />
    public override Task VerifyAsync(ChapterContext context) => VerifyConsumesAsync(context);
}
