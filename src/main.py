import asyncio
import sys

import qasync
from PySide6.QtCore import QSize
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication
from qasync import QEventLoop

from frontend.ui.app_controller import AppController


def _patch_qasync_timer_cleanup():
    simple_timer = getattr(qasync, "_SimpleTimer", None)
    if simple_timer is None or getattr(simple_timer, "_sopotek_safe_timer_patch", False):
        return

    def timer_event(self, event):  # noqa: N802
        timerid = event.timerId()
        self._SimpleTimer__log_debug("Timer event on id %s", timerid)
        callbacks = self._SimpleTimer__callbacks

        if self._stopped:
            self._SimpleTimer__log_debug("Timer stopped, killing %s", timerid)
            self.killTimer(timerid)
            callbacks.pop(timerid, None)
            return

        handle = callbacks.get(timerid)
        if handle is None:
            self._SimpleTimer__log_debug("Timer %s already cleared", timerid)
            self.killTimer(timerid)
            return

        try:
            if handle._cancelled:
                self._SimpleTimer__log_debug("Handle %s cancelled", handle)
            else:
                if self._SimpleTimer__debug_enabled:
                    import time
                    from asyncio.events import _format_handle

                    loop = asyncio.get_event_loop()
                    try:
                        loop._current_handle = handle
                        self._logger.debug("Calling handle %s", handle)
                        t0 = time.time()
                        handle._run()
                        dt = time.time() - t0
                        if dt >= loop.slow_callback_duration:
                            self._logger.warning(
                                "Executing %s took %.3f seconds",
                                _format_handle(handle),
                                dt,
                            )
                    finally:
                        loop._current_handle = None
                else:
                    handle._run()
        finally:
            callbacks.pop(timerid, None)
            self.killTimer(timerid)

    simple_timer.timerEvent = timer_event
    simple_timer._sopotek_safe_timer_patch = True


_patch_qasync_timer_cleanup()


app = QApplication(sys.argv)

loop = QEventLoop(app)
asyncio.set_event_loop(loop)


def _stop_loop():
    if loop.is_running():
        loop.stop()


window = AppController()
window.setIconSize(QSize(48, 48))
app.aboutToQuit.connect(_stop_loop)
if __name__ == "__main__":
 window.setWindowIcon(QIcon("./assets/logo.ico"))
 window.setWindowIconText("Sopotek Trading AI Platform")
 window.setWindowTitle("Sopotek Trading AI")

 window.show()

try:
    with loop:
        loop.run_forever()
except KeyboardInterrupt:
    pass

