import { useState, useEffect, type ReactNode } from "react";

interface Props {
  children: () => ReactNode;
  fallback?: ReactNode;
}

/** Render children only on the client (avoids SSR for MapLibre). */
export function ClientOnly({ children, fallback = null }: Props) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  return mounted ? <>{children()}</> : <>{fallback}</>;
}
