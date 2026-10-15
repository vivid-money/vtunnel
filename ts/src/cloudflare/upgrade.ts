import { UpgradeVerifier, type UpgradeRefusal } from "../upgrade.ts";

/**
 * Checks a controller's signed upgrade in the Worker, before it is passed to
 * the tunnel object: signature, host, path and time. It cannot see replays —
 * a Worker isolate is one of many — so the object checks again, with the
 * nonces it has seen. What this buys is that nothing unsigned reaches the
 * object at all, whatever a routing mistake might otherwise let through.
 */
export async function checkUpgrade(request: Request, key: string | undefined): Promise<UpgradeRefusal | "not_configured" | null> {
  if (!key) return "not_configured";
  return new UpgradeVerifier(key).verify(request);
}
