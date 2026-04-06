import { AuthPage } from "@/components/auth/auth-page";
import { redirectIfAuthenticated } from "@/lib/server-session";

export default async function LoginPage() {
  await redirectIfAuthenticated();
  return <AuthPage mode="login" />;
}
