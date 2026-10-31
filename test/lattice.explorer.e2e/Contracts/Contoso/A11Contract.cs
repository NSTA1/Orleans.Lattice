using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A11, telemetry in depth (#5381): a read-only chapter. It produces nothing, so it establishes
/// nothing, and its verifier asserts the estate it read is unchanged: A3's and A8's produces
/// still hold.
/// </summary>
public sealed class A11Contract() : ChapterContract("A11")
{
    /// <inheritdoc />
    public override Task EstablishAsync(ChapterContext context) => Task.CompletedTask;

    /// <inheritdoc />
    public override Task VerifyAsync(ChapterContext context) => VerifyConsumesAsync(context);
}
