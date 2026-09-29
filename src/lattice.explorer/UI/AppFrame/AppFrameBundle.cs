using System.Collections.Immutable;

namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>A launch's verified UI bundle, ready to transfer over the frame's port.</summary>
/// <param name="Launch">The launch the bundle was loaded for.</param>
/// <param name="Assets">Every declared asset, verified, in manifest order.</param>
internal sealed record AppFrameBundle(AppFrameLaunch Launch, ImmutableArray<AppFrameBundleAsset> Assets);
