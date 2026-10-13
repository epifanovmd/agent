// Версия SDK — из package.json пакета (одна на агента, SDK и примеры — scripts/version.sh).
import pkg from "../../../package.json" with { type: "json" };

/** Версия SDK. */
export const SDK_VERSION: string = pkg.version;
