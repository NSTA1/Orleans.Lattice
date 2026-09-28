# Orleans.Lattice.Explorer.AppKit

Static assets for everything that runs **inside** a Lattice App's sandboxed
frame in the Orleans.Lattice Explorer: the app-agnostic bootstrap document, its
loader, the in-frame `lattice` API, the kit stylesheet and fonts, and the frame
protocol schema.

The package carries no .NET code. The Explorer serves its assets from
`_content/Orleans.Lattice.Explorer.AppKit/appkit/v1/`, and the protocol version
is the path segment, so a breaking change ships beside the old version rather
than over it.

A frame is untrusted: it has an opaque origin, no credential ever enters it,
and it reaches the cluster only through the Explorer's bridge broker. You do
not reference this package directly; the Explorer web head brings it in.

This package is in progress and has not shipped a release.
