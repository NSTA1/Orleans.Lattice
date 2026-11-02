using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>Holds an <see cref="ILattice.DeleteTreeAsync"/> call while <see cref="TreeDeleteHolds"/> holds its tree.</summary>
/// <param name="holds">The world's holds.</param>
internal sealed class TreeDeleteHoldFilter(TreeDeleteHolds holds) : IIncomingGrainCallFilter
{
    /// <inheritdoc />
    public async Task Invoke(IIncomingGrainCallContext context)
    {
        if (context.MethodName == nameof(ILattice.DeleteTreeAsync)
            && context.InterfaceMethod?.DeclaringType == typeof(ILattice))
        {
            await holds.WaitAsync(context.TargetId.Key.ToString()!);
        }

        await context.Invoke();
    }
}
