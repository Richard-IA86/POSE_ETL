"""
src/utils/vpn_check.py
──────────────────────
Verifica y activa la VPN (FortiClient IPSec) antes de la descarga.

Estrategia: automatización GUI con pyautogui + pyperclip.
- Abre FortiClient.exe (GUI nativa).
- La ventana ya tiene VPN y usuario prellenados.
- Pega la contraseña via portapapeles (Ctrl+V) — mismo flujo manual.
- Hace Tab + Enter para disparar "Conectar".

Credenciales en .env:
  VPN_NAME=VPN POSE IP SEC
  VPN_USER=rrivarola
  VPN_PASSWORD=tu_contraseña_vpn
  VPN_TARGET_IP=10.2.1.81
"""

import socket
import subprocess
import time
from pathlib import Path

import pyautogui
import pygetwindow as gw  # type: ignore[import-untyped]
import pyperclip
from loguru import logger

from config.settings import VPN_PASSWORD, VPN_TARGET_IP, VPN_USER

_TITULO_VENTANA = "FortiClient"
# Imagen de referencia del icono en el escritorio
_FORTI_ICON_REF = Path(__file__).parent / "forti_icon_ref.png"
# Segundos de espera tras abrir la GUI antes de buscar la ventana
_ESPERA_GUI_S = 5
# Máximo de segundos esperando que el túnel quede establecido
_ESPERA_TUNEL_MAX_S = 40
_ESPERA_TUNEL_POLL_S = 5


def _vpn_activa() -> bool:
    """Devuelve True si el servidor responde en TCP port 80."""
    try:
        with socket.create_connection((VPN_TARGET_IP, 80), timeout=5):
            return True
    except OSError:
        return False


def _doble_clic_icono_escritorio() -> bool:
    """
    Muestra el escritorio, localiza el icono de FortiClient VPN
    por reconocimiento de imagen y hace doble clic sobre él.

    Retorna True si encontró y clickeó el icono.
    """
    if not _FORTI_ICON_REF.exists():
        logger.error(f"Imagen de referencia no encontrada: {_FORTI_ICON_REF}")
        return False

    # Matar instancias huérfanas previas
    subprocess.run(
        ["taskkill", "/F", "/IM", "FortiClient.exe"],
        capture_output=True,
    )
    time.sleep(0.5)

    # Minimizar todo para mostrar el escritorio
    pyautogui.hotkey("win", "d")
    time.sleep(1.5)

    # Buscar icono en pantalla (confidence requiere opencv-python)
    try:
        pos = pyautogui.locateCenterOnScreen(
            str(_FORTI_ICON_REF),
            confidence=0.75,
        )
    except pyautogui.ImageNotFoundException:
        pos = None

    if pos is None:
        logger.error("No se encontró el icono FortiClient en el escritorio.")
        return False

    logger.info(
        f"Icono FortiClient encontrado en {pos}. Haciendo doble clic..."
    )
    pyautogui.doubleClick(pos)
    time.sleep(_ESPERA_GUI_S)
    return True


def _levantar_vpn_gui() -> bool:
    """
    Conecta la VPN mediante automatización de la GUI de FortiClient.

    Flujo:
      1. Doble clic en el icono del escritorio (imagen de referencia).
      2. Activa la ventana.
      3. Navega al campo Contraseña con Tab desde el campo Usuario.
      4. Pega la contraseña desde el portapapeles.
      5. Presiona Enter para disparar "Conectar".
    """
    if not _doble_clic_icono_escritorio():
        return False

    ventanas = gw.getWindowsWithTitle(_TITULO_VENTANA)
    if not ventanas:
        logger.error("No se encontró ventana FortiClient tras abrir.")
        return False

    ventana = ventanas[0]
    ventana.restore()
    ventana.activate()
    time.sleep(1)

    # Click en zona neutral (logo superior) para forzar foco de teclado
    # en la ventana antes de enviar teclas Tab
    x_centro = ventana.left + ventana.width // 2
    y_logo = ventana.top + int(ventana.height * 0.22)
    pyautogui.click(x_centro, y_logo)
    time.sleep(0.5)

    logger.info(f"Conectando VPN (usuario: {VPN_USER}) via GUI...")

    # Poner contraseña en portapapeles (no queda en pantalla ni en código)
    pyperclip.copy(VPN_PASSWORD)

    # 4 x Tab desde el foco inicial → campo Contraseña
    for _ in range(4):
        pyautogui.press("tab")
        time.sleep(0.2)

    # Seleccionar todo + pegar (por si hay texto residual)
    pyautogui.hotkey("ctrl", "a")
    pyautogui.hotkey("ctrl", "v")
    time.sleep(0.3)

    # Tab → botón Conectar, Enter para disparar
    pyautogui.press("tab")
    time.sleep(0.2)
    pyautogui.press("enter")

    logger.info(
        f"Esperando hasta {_ESPERA_TUNEL_MAX_S} s a que el túnel"
        " se establezca..."
    )
    transcurrido = 0
    while transcurrido < _ESPERA_TUNEL_MAX_S:
        time.sleep(_ESPERA_TUNEL_POLL_S)
        transcurrido += _ESPERA_TUNEL_POLL_S
        if _vpn_activa():
            logger.info(f"Túnel activo tras {transcurrido} s.")
            return True
        logger.debug(f"Túnel aún no disponible ({transcurrido}s)...")
    return False


def asegurar_vpn() -> bool:
    """
    Verifica VPN activa; si no, conecta via GUI de FortiClient.
    Retorna True si la VPN queda activa al final.
    """
    if _vpn_activa():
        logger.debug(f"VPN activa — {VPN_TARGET_IP} alcanzable.")
        return True
    logger.warning(f"VPN inactiva. Intentando conectar '{_TITULO_VENTANA}'...")
    if _levantar_vpn_gui():
        logger.success("VPN activada correctamente.")
        return True
    logger.error(
        "No se pudo establecer VPN. " "Conectar manualmente con FortiClient."
    )
    return False
