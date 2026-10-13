namespace Orleans.Lattice.Views;

/// <summary>
/// Manifest marker for builds that preserve joint-view repair ownership through
/// publication, checkpointing and activation recovery. Used only for capability
/// discovery, never as a remotely invoked grain interface.
/// </summary>
[Alias(TypeAliases.IViewJointRepairCapable)]
internal interface IViewJointRepairCapable : IGrainWithStringKey
{
}
