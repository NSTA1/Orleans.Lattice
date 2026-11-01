using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Fabrikam;

/// <summary>
/// The operator's support visit (T4, #5397): a read-only chapter. It establishes nothing, and
/// its verifier asserts the tenancy hal visited is unchanged: T1's and T2's produces still hold.
/// </summary>
public sealed class T4Contract() : ChapterContract("T4")
{
    /// <inheritdoc />
    public override Task EstablishAsync(ChapterContext context) => Task.CompletedTask;

    /// <inheritdoc />
    public override Task VerifyAsync(ChapterContext context) => VerifyConsumesAsync(context);
}
