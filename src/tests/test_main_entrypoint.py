import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main as app_main


def test_qt_windows_noise_filter_matches_known_console_noise():
    assert app_main._is_qt_windows_noise("External WM_DESTROY received for QWidgetWindow(...)") is True
    assert app_main._is_qt_windows_noise("QWindowsWindow::setGeometry: Unable to set geometry 2139x1290+0+29") is True
    assert app_main._is_qt_windows_noise("OpenThemeData() failed for theme 15 (WINDOW). (The handle is invalid.)") is True
    assert app_main._is_qt_windows_noise("Using polling market data for Oanda") is False
