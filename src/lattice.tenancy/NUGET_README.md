# Orleans.Lattice.Tenancy

Optional, opt-in **tenant registry** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Holds the durable,
conflict-free-mergeable definition of every tenant in a multi-tenant cluster -
its status, resource quotas and burst allowance, placement binding,
tenant-admin subjects, and cross-tenant grants - persisted in reserved
`sys-tenant-*` Lattice trees under system-origin. `AddLatticeTenancy()` wires the
registry, seeds the reserved `default` tenant with an unbounded quota, and fails
fast at startup when tenancy is enabled without the `Orleans.Lattice.Auth` and
`Orleans.Lattice.Membership` add-ons it depends on. When the add-on is absent the
core tenancy seams stay inert and core behaves exactly as it did before tenancy
existed.
