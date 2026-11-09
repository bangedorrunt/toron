const AGENTS = ["A", "B"];

function activeOwners(vars: Record<string, any>): {
  bus: string[];
  ext: string[];
  resolvedEpoch: Record<string, number>;
} {
  const holder = String(vars.bus_holder ?? "None");
  const status = String(vars.bus_status ?? "None");
  const expired = Boolean(vars.bus_expired);
  const epoch = Number(vars.bus_epoch ?? 0);
  const mirror: string[] = Array.isArray(vars.mirror_expired_active)
    ? vars.mirror_expired_active.map(String)
    : [];
  const live: string[] = Array.isArray(vars.durable_live)
    ? vars.durable_live.map(String)
    : [];
  const mirrorEpochObj = (vars.mirror_epoch ?? {}) as Record<string, number>;
  const bus =
    status === "Held" && !expired && AGENTS.includes(holder) ? [holder] : [];

  const isFixed =
    mirror.length === 1 &&
    mirror[0] === holder &&
    mirrorEpochObj[holder] === epoch &&
    status === "Held";
  const ext = AGENTS.filter((a) => {
    if (!mirror.includes(a) || !live.includes(a) || bus.includes(a)) {
      return false;
    }
    return true;
  });
  const resolvedEpoch: Record<string, number> = {};
  for (const a of AGENTS) {
    if (bus.includes(a)) {
      resolvedEpoch[a] = epoch;
    } else {
      resolvedEpoch[a] = isFixed ? Number(mirrorEpochObj[a] ?? 0) : 0;
    }
  }
  return { bus, ext, resolvedEpoch };
}

export function state(vars: Record<string, any>): string {
  const status = String(vars.bus_status ?? "None");
  const holder = String(vars.bus_holder ?? "None");
  const epoch = Number(vars.bus_epoch ?? 0);
  const expired = Boolean(vars.bus_expired);
  const { bus, ext, resolvedEpoch } = activeOwners(vars);
  const total = bus.length + ext.length;

  if (status === "None") return "Unheld (genesis)";
  if (total > 1) {
    return `SPLIT-BRAIN: bus=${bus.join("+")} plus mirror_ext=${ext.join("+")}`;
  }
  if (status === "Released" && ext.length === 0) {
    return "Released";
  }
  if (status === "Revoked" && ext.length === 0) {
    return "Revoked";
  }
  if (bus.length === 1) {
    return `Held on bus by ${holder} (e=${epoch})`;
  }
  if (ext.length === 1) {
    const extOwner = ext[0];
    return `Liveness-extended ${extOwner} (bus_e=${epoch}, resolved_e=${resolvedEpoch[extOwner] ?? 0})`;
  }
  if (expired && ext.length === 0) {
    return `Expired and idle (mirror=${holder})`;
  }
  return `${status} (${holder}, e=${epoch})`;
}

export function step(
  action: string,
  _from: Record<string, any>,
  _to: Record<string, any>,
): string | null {
  if (action === "Done") return null;
  return action;
}
