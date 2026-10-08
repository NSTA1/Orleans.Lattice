# Architecture

The in-process facade validates caller input, authorizes the requested tree or cluster scope and delegates to core administration and schema seams. It does not hold a separate copy of tree topology or implement another storage engine.

## Request path

Registration composes `ILatticeTreeAdmin` with the grain factory, access gate and schema control service. Direct calls validate their request, authorize before reading/changing protected state, and translate logical control operations into core calls. Schema methods delegate to the schema facade rather than duplicating enforcement policy logic.

Telemetry and storage diagnostics have their own capability boundary. A caller authorized for a tree lifecycle action is not thereby entitled to every cluster-wide metric or raw storage detail. Reserved system trees are rejected by operations that would destructively relocate or reclaim user-tree WAL state.

## Durable operation path

`ILatticeTreeAdminOperations` accepts a caller's optional operation id and starts tracked work. The durable record carries the operation kind, tenant and target tree ids, phase and result; it is not a stored caller ACL. Status and list require current Read authority over every targeted tree and hide an unauthorized operation as absent. Cancellation additionally requires the grant needed to start that operation kind. Guessing an operation id does not bypass those checks.

View operations authorize over the view's source tree. WAL-move operations copy and validate the target before flipping placement; retained source-tail reclamation is separate. `ILatticeStorageUsageOperations` refreshes measurements as tracked work because the fresh measurement can outlive an individual request.

## WAL reclamation diagnostics

`ILatticeWalReclamation.GetWalReclamationAsync` reads durable pin and leaf-state information to report whether the tree's trim floor is held. An unreadable pin store or leaf is reported conservatively; a missing durable offset is not silently interpreted as successful coverage. This diagnostic does not trim or rebuild anything.

## Source map

- [Registration](../../src/lattice.api.treeadmin/LatticeApiTreeAdminServiceCollectionExtensions.cs)
- [Direct facade](../../src/lattice.api.treeadmin/LatticeTreeAdmin.cs)
- [Tracked operations](../../src/lattice.api.treeadmin/LatticeTreeAdmin.Operations.cs)
- [Storage refresh](../../src/lattice.api.treeadmin/LatticeStorageUsageOperations.cs)
- [WAL diagnostics](../../src/lattice.api.treeadmin/LatticeTreeAdmin.WalReclamation.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Operation details](operations.md)
