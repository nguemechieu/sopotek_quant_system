"use client";

import type { AuthSession, ForgotPasswordResponse, UserRole } from "@/lib/auth-shared";
import { AUTH_CHANGE_EVENT, AUTH_COOKIE_NAME, AUTH_STORAGE_KEY } from "@/lib/auth-shared";

const apiBaseUrl = process.env.NEXT_PUBLIC_SOPOTEK_API_BASE_URL ?? "http://127.0.0.1:8000";
const SESSION_MAX_AGE_SECONDS = 60 * 60 * 12;

type JsonRecord = Record<string, unknown>;

function extractErrorMessage(payload: unknown, fallback: string): string {
  if (!payload || typeof payload !== "object") {
    return fallback;
  }
  const detail = (payload as JsonRecord).detail;
  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }
  return fallback;
}

async function postJson<T>(path: string, body: JsonRecord): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const payload = (await response.json().catch(() => null)) as unknown;
  if (!response.ok) {
    throw new Error(extractErrorMessage(payload, "Unable to complete the request."));
  }
  return payload as T;
}

function setAuthCookie(token: string) {
  document.cookie = `${AUTH_COOKIE_NAME}=${encodeURIComponent(token)}; path=/; max-age=${SESSION_MAX_AGE_SECONDS}; SameSite=Lax`;
}

export function persistAuthSession(session: AuthSession) {
  if (typeof window === "undefined") {
    return;
  }
  localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(session));
  setAuthCookie(session.access_token);
  window.dispatchEvent(new Event(AUTH_CHANGE_EVENT));
}

export function readAuthSession(): AuthSession | null {
  if (typeof window === "undefined") {
    return null;
  }
  const raw = localStorage.getItem(AUTH_STORAGE_KEY);
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw) as AuthSession;
  } catch {
    localStorage.removeItem(AUTH_STORAGE_KEY);
    return null;
  }
}

export function clearAuthSession() {
  if (typeof window === "undefined") {
    return;
  }
  localStorage.removeItem(AUTH_STORAGE_KEY);
  document.cookie = `${AUTH_COOKIE_NAME}=; path=/; max-age=0; SameSite=Lax`;
  window.dispatchEvent(new Event(AUTH_CHANGE_EVENT));
}

export function readAuthToken(): string | null {
  const session = readAuthSession();
  if (session?.access_token) {
    return session.access_token;
  }
  if (typeof document === "undefined") {
    return null;
  }
  const rawCookie = document.cookie
    .split("; ")
    .find((entry) => entry.startsWith(`${AUTH_COOKIE_NAME}=`));
  if (!rawCookie) {
    return null;
  }
  return decodeURIComponent(rawCookie.slice(AUTH_COOKIE_NAME.length + 1));
}

export function loginUser(payload: { email: string; password: string }) {
  return postJson<AuthSession>("/auth/login", payload);
}

export function registerUser(payload: {
  email: string;
  username: string;
  password: string;
  full_name: string;
  role: UserRole;
}) {
  return postJson<AuthSession>("/auth/register", payload);
}

export function requestPasswordReset(email: string) {
  return postJson<ForgotPasswordResponse>("/auth/forgot-password", { email });
}

export function resetPassword(payload: { token: string; password: string }) {
  return postJson<AuthSession>("/auth/reset-password", payload);
}
