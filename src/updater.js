/**
 * Checks GitHub Releases for a newer version on startup and offers to
 * install it. Runs only inside the packaged Tauri app — in a plain browser
 * (vite dev / web build) it does nothing.
 *
 * On Windows the installer takes over after download: the app closes,
 * updates, and relaunches by itself.
 */
export async function checkForAppUpdate() {
  if (!window.__TAURI_INTERNALS__) return;
  try {
    const { check } = await import('@tauri-apps/plugin-updater');
    const update = await check();
    if (!update) return;
    const install = window.confirm(
      `A new version of Liquidity Tracker is available (v${update.version}).\n\n` +
        'Install it now? The app will restart.'
    );
    if (!install) return;
    await update.downloadAndInstall();
  } catch (err) {
    // Never block the app on update problems (offline, endpoint down, …).
    console.warn('Update check failed:', err);
  }
}
