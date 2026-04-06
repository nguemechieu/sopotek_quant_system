import "server-only";

import { cookies } from "next/headers";
import { redirect } from "next/navigation";

import { AUTH_COOKIE_NAME, type AuthUser } from "@/lib/auth-shared";

const apiBaseUrl = process.env.SOPOTEK_API_BASE_URL ?? process.env.NEXT_PUBLIC_SOPOTEK_API_BASE_URL ?? "http://127.0.0.1:8000";

export type ServerSession = {
  accessToken: string;
  user: AuthUser;
};

async function fetchCurrentUser(token: string): Promise<AuthUser | null> {
  try {
    const response = await fetch(`${apiBaseUrl}/auth/me`, {
      headers: {
        Authorization: `Bearer ${token}`
      },
      cache: "no-store"
    });
    if (!response.ok) {
      return null;
    }
    return (await response.json()) as AuthUser;
  } catch {
    return null;
  }
}

export async function readServerSession(): Promise<ServerSession | null> {
  const cookieStore = await cookies();
  const accessToken = cookieStore.get(AUTH_COOKIE_NAME)?.value?.trim();
  if (!accessToken) {
    return null;
  }

  const user = await fetchCurrentUser(accessToken);
  if (!user) {
    return null;
  }

  return { accessToken, user };
}

export async function requireServerSession(): Promise<ServerSession> {
  const session = await readServerSession();
  if (!session) {
    redirect("/login");
  }
  return session;
}

export async function redirectIfAuthenticated(): Promise<void> {
  const session = await readServerSession();
  if (session) {
    redirect("/dashboard");
  }
}
