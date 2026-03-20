import asyncio
import sys


class VoiceService:
    def __init__(self, logger=None, voice_name="", recognition_provider="windows"):
        self.logger = logger
        self.is_windows = sys.platform.startswith("win")
        self.voice_name = str(voice_name or "").strip()
        self.recognition_provider = str(recognition_provider or "windows").strip().lower() or "windows"

    def available(self):
        return self.is_windows

    def available_recognition_providers(self):
        return [
            ("windows", "Windows"),
            ("google", "Google"),
        ]

    def recognition_provider_available(self, provider):
        normalized = str(provider or "").strip().lower()
        if normalized == "google":
            return self._google_recognition_available()
        if normalized == "windows":
            return self.available()
        return False

    def set_voice(self, voice_name):
        self.voice_name = str(voice_name or "").strip()

    def set_recognition_provider(self, provider):
        normalized = str(provider or "windows").strip().lower() or "windows"
        self.recognition_provider = normalized if normalized in {"windows", "google"} else "windows"

    async def list_voices(self):
        if not self.available():
            return []
        script = (
            "Add-Type -AssemblyName System.Speech\n"
            "$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer\n"
            "$synth.GetInstalledVoices() | ForEach-Object { $_.VoiceInfo.Name }\n"
        )
        result = await self._run_powershell(script)
        if not result.get("ok"):
            return []
        voices = [line.strip() for line in str(result.get("stdout", "") or "").splitlines() if line.strip()]
        return voices

    async def speak(self, text, voice_name=None):
        message = str(text or "").strip()
        if not message:
            return {"ok": False, "message": "No text was provided to speak."}
        if not self.available():
            return {"ok": False, "message": "Voice playback is currently available only on Windows."}

        script = self._powershell_speak_script(message, voice_name=voice_name or self.voice_name)
        return await self._run_powershell(script, success_message="Reply spoken.")

    async def listen(self, timeout_seconds=8, provider=None):
        if not self.available():
            return {"ok": False, "message": "Voice listening is currently available only on Windows.", "text": ""}

        provider_name = str(provider or self.recognition_provider or "windows").strip().lower() or "windows"
        if provider_name == "google":
            return await self._listen_google(timeout_seconds=timeout_seconds)

        try:
            timeout_seconds = max(3, int(timeout_seconds))
        except Exception:
            timeout_seconds = 8

        script = self._powershell_listen_script(timeout_seconds)
        result = await self._run_powershell(script)
        if not result.get("ok"):
            result.setdefault("text", "")
            return result

        text = str(result.get("stdout", "") or "").strip()
        if not text:
            return {"ok": False, "message": "No speech was detected.", "text": ""}
        return {"ok": True, "message": "Voice prompt captured.", "text": text}

    async def _listen_google(self, timeout_seconds=8):
        if not self._google_recognition_available():
            return {
                "ok": False,
                "message": "Google voice recognition requires the optional packages 'SpeechRecognition' and 'sounddevice'.",
                "text": "",
            }

        try:
            timeout_seconds = max(3, int(timeout_seconds))
        except Exception:
            timeout_seconds = 8

        try:
            import sounddevice as sd
            import speech_recognition as sr
        except Exception as exc:
            return {"ok": False, "message": f"Google voice recognition is unavailable: {exc}", "text": ""}

        sample_rate = 16000
        try:
            audio = await asyncio.to_thread(
                self._record_audio_google,
                sd,
                timeout_seconds,
                sample_rate,
            )
            recognizer = sr.Recognizer()
            audio_data = sr.AudioData(audio.tobytes(), sample_rate, 2)
            text = await asyncio.to_thread(recognizer.recognize_google, audio_data)
        except Exception as exc:
            return {"ok": False, "message": f"Google voice recognition failed: {exc}", "text": ""}

        text = str(text or "").strip()
        if not text:
            return {"ok": False, "message": "No speech was detected.", "text": ""}
        return {"ok": True, "message": "Google voice prompt captured.", "text": text}

    def _record_audio_google(self, sounddevice_module, timeout_seconds, sample_rate):
        recording = sounddevice_module.rec(
            int(timeout_seconds * sample_rate),
            samplerate=sample_rate,
            channels=1,
            dtype="int16",
        )
        sounddevice_module.wait()
        return recording.flatten()

    def _google_recognition_available(self):
        try:
            import sounddevice  # noqa: F401
            import speech_recognition  # noqa: F401
        except Exception:
            return False
        return True

    async def _run_powershell(self, script, success_message="OK"):
        try:
            process = await asyncio.create_subprocess_exec(
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                script,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
        except Exception as exc:
            if self.logger is not None:
                self.logger.debug("Voice PowerShell launch failed: %s", exc)
            return {"ok": False, "message": f"Voice service failed to start: {exc}", "stdout": "", "stderr": ""}

        stdout_text = stdout.decode("utf-8", errors="ignore").strip()
        stderr_text = stderr.decode("utf-8", errors="ignore").strip()
        if process.returncode != 0:
            message = stderr_text or stdout_text or "Voice service failed."
            return {"ok": False, "message": message, "stdout": stdout_text, "stderr": stderr_text}
        return {"ok": True, "message": success_message, "stdout": stdout_text, "stderr": stderr_text}

    def _powershell_speak_script(self, text, voice_name=""):
        escaped = self._escape_here_string(text)
        escaped_voice = self._escape_here_string(voice_name)
        return (
            "Add-Type -AssemblyName System.Speech\n"
            "$text = @'\n"
            f"{escaped}\n"
            "'@\n"
            "$voice = @'\n"
            f"{escaped_voice}\n"
            "'@\n"
            "$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer\n"
            "if ($voice.Trim()) {\n"
            "  try { $synth.SelectVoice($voice.Trim()) } catch { }\n"
            "}\n"
            "$synth.SetOutputToDefaultAudioDevice()\n"
            "$synth.Speak($text)\n"
            "Write-Output 'spoken'\n"
        )

    def _powershell_listen_script(self, timeout_seconds):
        return (
            "Add-Type -AssemblyName System.Speech\n"
            "try {\n"
            "  $engine = New-Object System.Speech.Recognition.SpeechRecognitionEngine\n"
            "  $engine.SetInputToDefaultAudioDevice()\n"
            "  $grammar = New-Object System.Speech.Recognition.DictationGrammar\n"
            "  $engine.LoadGrammar($grammar)\n"
            "  $engine.InitialSilenceTimeout = [TimeSpan]::FromSeconds(5)\n"
            "  $engine.BabbleTimeout = [TimeSpan]::FromSeconds(2)\n"
            "  $engine.EndSilenceTimeout = [TimeSpan]::FromSeconds(1)\n"
            f"  $result = $engine.Recognize([TimeSpan]::FromSeconds({int(timeout_seconds)}))\n"
            "  if ($result -and $result.Text) {\n"
            "    Write-Output $result.Text\n"
            "  }\n"
            "} catch {\n"
            "  Write-Error $_.Exception.Message\n"
            "  exit 1\n"
            "}\n"
        )

    def _escape_here_string(self, value):
        return str(value or "").replace("'@", "'@ ")
