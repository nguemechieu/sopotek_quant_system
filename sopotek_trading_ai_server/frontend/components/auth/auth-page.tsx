"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { FormEvent, useEffect, useState, useTransition } from "react";

import { clearAuthSession, loginUser, persistAuthSession, registerUser, requestPasswordReset, resetPassword } from "@/lib/auth";
import type { AuthSession, UserRole } from "@/lib/auth-shared";

type AuthMode = "login" | "register" | "forgot-password" | "reset-password";

type AuthPageProps = {
  mode: AuthMode;
  initialToken?: string;
};

type ModeContent = {
  eyebrow: string;
  title: string;
  description: string;
  action: string;
  supportLabel: string;
  supportHref: string;
  supportText: string;
  secondaryLabel: string;
  secondaryHref: string;
  secondaryText: string;
  highlights: { label: string; value: string }[];
};

const contentByMode: Record<AuthMode, ModeContent> = {
  login: {
    eyebrow: "Operator Access",
    title: "Sign in to the Sopotek trading control plane.",
    description: "Review live portfolio state, risk alerts, strategy health, and execution flow from one fund-grade surface.",
    action: "Sign In",
    supportLabel: "Need access?",
    supportHref: "/register",
    supportText: "Create a desk account",
    secondaryLabel: "Password issue",
    secondaryHref: "/forgot-password",
    secondaryText: "Reset your credentials",
    highlights: [
      { label: "Realtime control", value: "Kafka-backed command and event flow" },
      { label: "Desk oversight", value: "PnL, exposure, and fills in one surface" },
      { label: "Role security", value: "Admin, trader, and viewer permissions" }
    ]
  },
  register: {
    eyebrow: "Desk Onboarding",
    title: "Create a new operating seat for the platform.",
    description: "Provision a trader or viewer account and move straight into the web console with JWT-based access.",
    action: "Create Account",
    supportLabel: "Already onboarded?",
    supportHref: "/login",
    supportText: "Sign in instead",
    secondaryLabel: "Password support",
    secondaryHref: "/forgot-password",
    secondaryText: "Recover access",
    highlights: [
      { label: "First user wins", value: "The very first account is promoted to admin automatically" },
      { label: "Role-aware access", value: "Choose trader or viewer at signup" },
      { label: "Instant workspace", value: "Default portfolio and strategy state are provisioned at registration" }
    ]
  },
  "forgot-password": {
    eyebrow: "Credential Recovery",
    title: "Prepare a secure password reset for your desk account.",
    description: "We generate a short-lived reset token. In non-production environments you will also see the preview link immediately.",
    action: "Send Reset Link",
    supportLabel: "Remembered it?",
    supportHref: "/login",
    supportText: "Go back to sign in",
    secondaryLabel: "Need a new account?",
    secondaryHref: "/register",
    secondaryText: "Create one now",
    highlights: [
      { label: "Short-lived tokens", value: "Reset links expire automatically" },
      { label: "Operator-safe flow", value: "Responses do not expose whether an email exists" },
      { label: "Dev preview", value: "Local and staging builds can reveal the reset link directly" }
    ]
  },
  "reset-password": {
    eyebrow: "Password Reset",
    title: "Set a fresh credential and return to the control plane.",
    description: "Paste a reset token or open this page from a reset link. A successful reset signs you back in automatically.",
    action: "Reset Password",
    supportLabel: "Token missing?",
    supportHref: "/forgot-password",
    supportText: "Request a new reset link",
    secondaryLabel: "Back to auth",
    secondaryHref: "/login",
    secondaryText: "Return to sign in",
    highlights: [
      { label: "Instant recovery", value: "Successful resets issue a new platform session" },
      { label: "UTC token control", value: "Expiry is enforced server-side" },
      { label: "Desk continuity", value: "No manual re-provisioning or profile recovery needed" }
    ]
  }
};

function Field({
  id,
  label,
  type = "text",
  value,
  onChange,
  placeholder,
  autoComplete,
  required = true
}: {
  id: string;
  label: string;
  type?: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  autoComplete?: string;
  required?: boolean;
}) {
  return (
    <label className="block">
      <span className="auth-label">{label}</span>
      <input
        id={id}
        type={type}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        autoComplete={autoComplete}
        required={required}
        className="auth-input mt-2"
      />
    </label>
  );
}

function HighlightList({ items }: { items: ModeContent["highlights"] }) {
  return (
    <div className="grid gap-3 md:grid-cols-3">
      {items.map((item) => (
        <div key={item.label} className="rounded-[24px] border border-white/10 bg-black/10 px-4 py-4">
          <p className="text-[11px] uppercase tracking-[0.28em] text-mist/45">{item.label}</p>
          <p className="mt-3 text-sm leading-6 text-mist/80">{item.value}</p>
        </div>
      ))}
    </div>
  );
}

function buildSuccessMessage(mode: AuthMode, session: AuthSession | null, fallback: string) {
  if (mode === "register" && session) {
    return `Account created for ${session.user.email}. Redirecting to the trading workspace.`;
  }
  if (mode === "login" && session) {
    return `Welcome back, ${session.user.full_name || session.user.username}. Redirecting to the dashboard.`;
  }
  if (mode === "reset-password" && session) {
    return "Password updated and session restored. Redirecting to the dashboard.";
  }
  return fallback;
}

export function AuthPage({ mode, initialToken = "" }: AuthPageProps) {
  const router = useRouter();
  const content = contentByMode[mode];
  const [isPending, startTransition] = useTransition();
  const [email, setEmail] = useState("");
  const [username, setUsername] = useState("");
  const [fullName, setFullName] = useState("");
  const [role, setRole] = useState<UserRole>("trader");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [token, setToken] = useState(initialToken);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [previewToken, setPreviewToken] = useState<string | null>(null);

  useEffect(() => {
    clearAuthSession();
  }, []);

  function completeSession(session: AuthSession, fallbackMessage: string) {
    persistAuthSession(session);
    setSuccessMessage(buildSuccessMessage(mode, session, fallbackMessage));
    router.push("/dashboard");
    router.refresh();
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setErrorMessage(null);
    setSuccessMessage(null);

    if ((mode === "register" || mode === "reset-password") && password !== confirmPassword) {
      setErrorMessage("Passwords do not match.");
      return;
    }

    startTransition(async () => {
      try {
        if (mode === "login") {
          const session = await loginUser({ email, password });
          completeSession(session, "Signed in successfully.");
          return;
        }

        if (mode === "register") {
          const session = await registerUser({
            email,
            username,
            password,
            full_name: fullName,
            role
          });
          completeSession(session, "Account created successfully.");
          return;
        }

        if (mode === "forgot-password") {
          const response = await requestPasswordReset(email);
          setSuccessMessage(response.message);
          setPreviewUrl(response.reset_url ?? null);
          setPreviewToken(response.reset_token ?? null);
          return;
        }

        const session = await resetPassword({ token, password });
        completeSession(session, "Password reset successfully.");
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unable to complete the auth flow.";
        setErrorMessage(message);
      }
    });
  }

  return (
    <div className="relative min-h-screen overflow-hidden px-5 py-5 md:px-8 md:py-8">
      <div className="pointer-events-none absolute inset-0 grid-lines opacity-15" />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-[24rem] bg-[radial-gradient(circle_at_top,rgba(251,146,60,0.22),transparent_58%)]" />
      <div className="pointer-events-none absolute right-0 top-16 h-[28rem] w-[28rem] rounded-full bg-[radial-gradient(circle,rgba(134,239,172,0.14),transparent_68%)] blur-3xl" />

      <div className="relative mx-auto grid min-h-[calc(100vh-2.5rem)] max-w-7xl gap-6 lg:grid-cols-[1.08fr_0.92fr]">
        <section className="flex flex-col justify-between rounded-[34px] border border-white/10 bg-black/10 px-6 py-7 shadow-[0_32px_90px_rgba(2,8,14,0.4)] backdrop-blur-sm md:px-8 md:py-9">
          <div>
            <p className="text-xs uppercase tracking-[0.34em] text-amber-300/70">Sopotek Trading AI</p>
            <div className="mt-6 max-w-2xl space-y-4">
              <p className="text-xs uppercase tracking-[0.3em] text-mist/45">{content.eyebrow}</p>
              <h1 className="max-w-3xl text-4xl font-semibold tracking-tight text-sand md:text-6xl md:leading-[1.02]">
                {content.title}
              </h1>
              <p className="max-w-xl text-base leading-7 text-mist/72 md:text-lg">{content.description}</p>
            </div>
          </div>

          <div className="space-y-6">
            <div className="flex flex-wrap gap-3">
              <Link href={content.supportHref} className="rounded-full border border-amber-300/30 bg-amber-300/10 px-4 py-2 text-sm text-sand transition hover:border-amber-300/50 hover:bg-amber-300/14">
                {content.supportText}
              </Link>
              <Link href={content.secondaryHref} className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-mist/78 transition hover:border-white/20 hover:text-mist">
                {content.secondaryText}
              </Link>
            </div>
            <HighlightList items={content.highlights} />
          </div>
        </section>

        <section className="panel flex h-full flex-col rounded-[34px] border border-white/12 px-6 py-7 md:px-8 md:py-8">
          <div className="mb-6">
            <p className="text-xs uppercase tracking-[0.3em] text-mist/45">{content.eyebrow}</p>
            <h2 className="mt-3 text-3xl font-semibold tracking-tight text-sand">{content.action}</h2>
          </div>

          <form className="space-y-4" onSubmit={handleSubmit}>
            {(mode === "login" || mode === "register" || mode === "forgot-password") && (
              <Field
                id="email"
                label="Email"
                type="email"
                value={email}
                onChange={setEmail}
                placeholder="desk@sopotek.ai"
                autoComplete="email"
              />
            )}

            {mode === "register" && (
              <>
                <Field
                  id="full-name"
                  label="Full Name"
                  value={fullName}
                  onChange={setFullName}
                  placeholder="Fund Trader"
                  autoComplete="name"
                />
                <Field
                  id="username"
                  label="Username"
                  value={username}
                  onChange={setUsername}
                  placeholder="fundtrader"
                  autoComplete="username"
                />
                <label className="block">
                  <span className="auth-label">Role</span>
                  <select
                    value={role}
                    onChange={(event) => setRole(event.target.value as UserRole)}
                    className="auth-input mt-2"
                  >
                    <option value="trader">Trader</option>
                    <option value="viewer">Viewer</option>
                  </select>
                </label>
              </>
            )}

            {(mode === "login" || mode === "register" || mode === "reset-password") && (
              <Field
                id="password"
                label={mode === "reset-password" ? "New Password" : "Password"}
                type="password"
                value={password}
                onChange={setPassword}
                placeholder="Minimum 8 characters"
                autoComplete={mode === "login" ? "current-password" : "new-password"}
              />
            )}

            {(mode === "register" || mode === "reset-password") && (
              <Field
                id="confirm-password"
                label="Confirm Password"
                type="password"
                value={confirmPassword}
                onChange={setConfirmPassword}
                placeholder="Repeat the password"
                autoComplete="new-password"
              />
            )}

            {mode === "reset-password" && (
              <Field
                id="reset-token"
                label="Reset Token"
                value={token}
                onChange={setToken}
                placeholder="Paste the reset token or open a reset link"
                autoComplete="off"
              />
            )}

            {errorMessage ? (
              <div className="rounded-[24px] border border-rose-400/30 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">
                {errorMessage}
              </div>
            ) : null}

            {successMessage ? (
              <div className="rounded-[24px] border border-lime-400/30 bg-lime-400/10 px-4 py-3 text-sm text-lime-100">
                <p>{successMessage}</p>
                {previewUrl ? (
                  <div className="mt-3 space-y-2 text-lime-50/90">
                    <p className="text-xs uppercase tracking-[0.24em] text-lime-200/75">Reset Link Preview</p>
                    <Link href={previewUrl} className="break-all text-sm text-lime-100 underline underline-offset-4">
                      {previewUrl}
                    </Link>
                    {previewToken ? <p className="font-[var(--font-mono)] text-xs text-lime-100/80">{previewToken}</p> : null}
                  </div>
                ) : null}
              </div>
            ) : null}

            <button type="submit" disabled={isPending} className="auth-submit">
              {isPending ? "Working..." : content.action}
            </button>
          </form>

          <div className="mt-6 space-y-3 border-t border-white/10 pt-5 text-sm text-mist/70">
            <p>
              {content.supportLabel}{" "}
              <Link href={content.supportHref} className="text-sand underline underline-offset-4">
                {content.supportText}
              </Link>
            </p>
            <p>
              {content.secondaryLabel}{" "}
              <Link href={content.secondaryHref} className="text-sand underline underline-offset-4">
                {content.secondaryText}
              </Link>
            </p>
          </div>
        </section>
      </div>
    </div>
  );
}
