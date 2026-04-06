"use client";

import Link from "next/link";
import { ReactNode, useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";

import { Navigation } from "@/components/layout/navigation";
import { clearAuthSession, readAuthSession } from "@/lib/auth";
import { AUTH_CHANGE_EVENT, AUTH_ROUTE_PREFIXES, type AuthSession } from "@/lib/auth-shared";

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [session, setSession] = useState<AuthSession | null>(null);
  const isAuthRoute = AUTH_ROUTE_PREFIXES.some((prefix) => pathname.startsWith(prefix));

  useEffect(() => {
    const syncSession = () => setSession(readAuthSession());
    syncSession();
    window.addEventListener("storage", syncSession);
    window.addEventListener(AUTH_CHANGE_EVENT, syncSession);
    return () => {
      window.removeEventListener("storage", syncSession);
      window.removeEventListener(AUTH_CHANGE_EVENT, syncSession);
    };
  }, []);

  if (isAuthRoute) {
    return <>{children}</>;
  }

  const sessionLabel = session?.user.full_name || session?.user.username || "Session active";

  function handleSignOut() {
    clearAuthSession();
    setSession(null);
    router.push("/login");
    router.refresh();
  }

  return (
    <div className="min-h-screen px-5 py-5 md:px-8 md:py-8">
      <div className="mx-auto flex max-w-7xl flex-col gap-6">
        <header className="panel rounded-[28px] border border-white/10 px-6 py-5">
          <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
            <div className="space-y-2">
              <p className="text-xs uppercase tracking-[0.35em] text-amber-300/70">Sopotek Trading AI</p>
              <div className="flex flex-col gap-2 md:flex-row md:items-end md:gap-4">
                <h1 className="text-3xl font-semibold tracking-tight text-sand md:text-4xl">
                  Multi-asset control plane for a Kafka-driven trading core.
                </h1>
                <span className="rounded-full border border-lime-400/30 bg-lime-400/10 px-3 py-1 text-xs uppercase tracking-[0.3em] text-lime-300">
                  Fund Ops Mode
                </span>
              </div>
            </div>
            <div className="flex flex-col gap-3 md:min-w-[320px]">
              <div className="grid grid-cols-2 gap-3 text-sm text-mist/70">
                <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                  <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Realtime</p>
                  <p className="mt-1 font-medium text-mist">WebSocket + Kafka</p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                  <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Auth</p>
                  <p className="mt-1 font-medium text-mist">JWT / RBAC</p>
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-3">
                {session ? (
                  <>
                    <div className="rounded-full border border-lime-400/25 bg-lime-400/10 px-4 py-2 text-xs uppercase tracking-[0.24em] text-lime-200">
                      {sessionLabel}
                    </div>
                    <button
                      type="button"
                      onClick={handleSignOut}
                      className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-mist/80 transition hover:border-white/20 hover:text-mist"
                    >
                      Sign out
                    </button>
                  </>
                ) : (
                  <>
                    <Link
                      href="/login"
                      className="rounded-full border border-amber-300/30 bg-amber-300/10 px-4 py-2 text-sm text-sand transition hover:border-amber-300/50 hover:bg-amber-300/14"
                    >
                      Sign in
                    </Link>
                    <Link
                      href="/register"
                      className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-mist/80 transition hover:border-white/20 hover:text-mist"
                    >
                      Register
                    </Link>
                  </>
                )}
              </div>
            </div>
          </div>
          <div className="mt-5">
            <Navigation />
          </div>
        </header>

        <main>{children}</main>
      </div>
    </div>
  );
}
