"""describe_status(): a tunnel status as short localized UI text (MCPWARP.md section 7).

The six tables are normative: they must equal ws-mixer-spec's
spec/fixtures/mcpwarp/status-text/<locale>.json string for string
(tests/spec/test_status_text.py), and change only with a spec release.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .._jscompat import JS_WHITESPACE, js_trim
from .status import TunnelStatus, TunnelStatusCode


@dataclass(frozen=True)
class StatusText:
    label: str
    """The state, localized."""
    detail: str | None = None
    """Why, localized; None when there is nothing to explain or it would repeat `label`."""

    def to_dict(self) -> dict[str, str]:
        if self.detail is None:
            return {"label": self.label}
        return {"label": self.label, "detail": self.detail}


def _empty() -> Mapping[str, str]:
    return MappingProxyType({})


@dataclass(frozen=True)
class StatusMessages:
    """Per-state labels and per-code sentences, keyed by the JS string values. Holds mappings,
    so it is not hashable (by design)."""

    states: Mapping[str, str] = field(default_factory=_empty)
    codes: Mapping[str, str] = field(default_factory=_empty)


def _table(states: dict[str, str], codes: dict[str, str]) -> StatusMessages:
    return StatusMessages(MappingProxyType(states), MappingProxyType(codes))


_EN = _table(
    {
        "off": "Off",
        "connecting": "Connecting…",
        "online": "Online",
        "disabled": "Disabled in the mcpwarp dashboard",
        "conflict": "Another instance took over this name",
        "error": "Error",
    },
    {
        "UNAUTHORIZED": "Access token was rejected",
        "TAKEN_OVER": "Another instance took over this name",
        "DISCONNECTED": "Connection lost, reconnecting",
        "RETRYING": "Connection failed, retrying",
        "DISABLED": "Disabled in the mcpwarp dashboard",
        "INVALID_CONFIG": "Invalid settings",
        "INVALID_NAME": "Invalid service name (lowercase letters, digits and inner dashes, "
        "at most 30 characters)",
        "USERNAME_REQUIRED": "Set a username in your mcpwarp account",
        "QUOTA_EXCEEDED": "Plan limit reached",
        "CONFLICT": "Name already in use",
        "UNSUPPORTED_VERSION": "Plugin update required",
        "DISCONNECTED_FATAL": "Connection to mcpwarp failed",
        "INTERNAL": "Unexpected error",
    },
)

_DE = _table(
    {
        "off": "Aus",
        "connecting": "Verbinden…",
        "online": "Online",
        "disabled": "Im mcpwarp-Dashboard deaktiviert",
        "conflict": "Eine andere Instanz hat diesen Namen übernommen",
        "error": "Fehler",
    },
    {
        "UNAUTHORIZED": "Zugriffstoken wurde abgelehnt",
        "TAKEN_OVER": "Eine andere Instanz hat diesen Namen übernommen",
        "DISCONNECTED": "Verbindung unterbrochen, wird wiederhergestellt",
        "RETRYING": "Verbindung fehlgeschlagen, neuer Versuch",
        "DISABLED": "Im mcpwarp-Dashboard deaktiviert",
        "INVALID_CONFIG": "Ungültige Einstellungen",
        "INVALID_NAME": "Ungültiger Dienstname (Kleinbuchstaben, Ziffern und innere Bindestriche, "
        "höchstens 30 Zeichen)",
        "USERNAME_REQUIRED": "Legen Sie in Ihrem mcpwarp-Konto einen Benutzernamen fest",
        "QUOTA_EXCEEDED": "Tariflimit erreicht",
        "CONFLICT": "Name wird bereits verwendet",
        "UNSUPPORTED_VERSION": "Plugin-Update erforderlich",
        "DISCONNECTED_FATAL": "Verbindung zu mcpwarp fehlgeschlagen",
        "INTERNAL": "Unerwarteter Fehler",
    },
)

_ES = _table(
    {
        "off": "Desactivado",
        "connecting": "Conectando…",
        "online": "En línea",
        "disabled": "Desactivado en el panel de mcpwarp",
        "conflict": "Otra instancia ha tomado este nombre",
        "error": "Error",
    },
    {
        "UNAUTHORIZED": "Token de acceso rechazado",
        "TAKEN_OVER": "Otra instancia ha tomado este nombre",
        "DISCONNECTED": "Conexión perdida, reconectando",
        "RETRYING": "Error de conexión, reintentando",
        "DISABLED": "Desactivado en el panel de mcpwarp",
        "INVALID_CONFIG": "Configuración no válida",
        "INVALID_NAME": "Nombre de servicio no válido (letras minúsculas, dígitos y guiones "
        "internos, 30 caracteres como máximo)",
        "USERNAME_REQUIRED": "Define un nombre de usuario en tu cuenta de mcpwarp",
        "QUOTA_EXCEEDED": "Límite del plan alcanzado",
        "CONFLICT": "El nombre ya está en uso",
        "UNSUPPORTED_VERSION": "Es necesario actualizar el plugin",
        "DISCONNECTED_FATAL": "No se pudo conectar con mcpwarp",
        "INTERNAL": "Error inesperado",
    },
)

_FR = _table(
    {
        "off": "Désactivé",
        "connecting": "Connexion…",
        "online": "En ligne",
        "disabled": "Désactivé dans le tableau de bord mcpwarp",
        "conflict": "Une autre instance a repris ce nom",
        "error": "Erreur",
    },
    {
        "UNAUTHORIZED": "Jeton d'accès refusé",
        "TAKEN_OVER": "Une autre instance a repris ce nom",
        "DISCONNECTED": "Connexion perdue, reconnexion en cours",
        "RETRYING": "Échec de la connexion, nouvel essai en cours",
        "DISABLED": "Désactivé dans le tableau de bord mcpwarp",
        "INVALID_CONFIG": "Paramètres non valides",
        "INVALID_NAME": "Nom de service non valide (lettres minuscules, chiffres et tirets "
        "internes, 30 caractères au maximum)",
        "USERNAME_REQUIRED": "Définissez un nom d'utilisateur dans votre compte mcpwarp",
        "QUOTA_EXCEEDED": "Limite de l'offre atteinte",
        "CONFLICT": "Nom déjà utilisé",
        "UNSUPPORTED_VERSION": "Mise à jour du plugin requise",
        "DISCONNECTED_FATAL": "Échec de la connexion à mcpwarp",
        "INTERNAL": "Erreur inattendue",
    },
)

_JA = _table(
    {
        "off": "オフ",
        "connecting": "接続中…",
        "online": "オンライン",
        "disabled": "mcpwarp ダッシュボードで無効化されています",
        "conflict": "別のインスタンスがこの名前を取得しました",
        "error": "エラー",
    },
    {
        "UNAUTHORIZED": "アクセストークンが拒否されました",
        "TAKEN_OVER": "別のインスタンスがこの名前を取得しました",
        "DISCONNECTED": "接続が切れたため再接続しています",
        "RETRYING": "接続に失敗したため再試行しています",
        "DISABLED": "mcpwarp ダッシュボードで無効化されています",
        "INVALID_CONFIG": "設定が無効です",
        "INVALID_NAME": "サービス名が無効です（英小文字、数字、"
        "先頭と末尾以外のハイフン、30 文字以内）",
        "USERNAME_REQUIRED": "mcpwarp アカウントでユーザー名を設定してください",
        "QUOTA_EXCEEDED": "プランの上限に達しました",
        "CONFLICT": "この名前は既に使用されています",
        "UNSUPPORTED_VERSION": "プラグインの更新が必要です",
        "DISCONNECTED_FATAL": "mcpwarp への接続に失敗しました",
        "INTERNAL": "予期しないエラー",
    },
)

_ZH_CN = _table(
    {
        "off": "已关闭",
        "connecting": "正在连接…",
        "online": "在线",
        "disabled": "已在 mcpwarp 控制台中停用",
        "conflict": "另一个实例已接管该名称",
        "error": "错误",
    },
    {
        "UNAUTHORIZED": "访问令牌被拒绝",
        "TAKEN_OVER": "另一个实例已接管该名称",
        "DISCONNECTED": "连接已断开，正在重新连接",
        "RETRYING": "连接失败，正在重试",
        "DISABLED": "已在 mcpwarp 控制台中停用",
        "INVALID_CONFIG": "设置无效",
        "INVALID_NAME": "服务名称无效（小写字母、数字和中间的连字符，最多 30 个字符）",
        "USERNAME_REQUIRED": "请在 mcpwarp 账户中设置用户名",
        "QUOTA_EXCEEDED": "已达到套餐限额",
        "CONFLICT": "名称已被占用",
        "UNSUPPORTED_VERSION": "需要更新插件",
        "DISCONNECTED_FATAL": "无法连接到 mcpwarp",
        "INTERNAL": "意外错误",
    },
)

# The shipped locales, in resolution order: a bare language picks the first table with it.
STATUS_LOCALES: tuple[str, ...] = ("en", "de", "es", "fr", "ja", "zh-CN")

_TABLES: Mapping[str, StatusMessages] = MappingProxyType(
    {"en": _EN, "de": _DE, "es": _ES, "fr": _FR, "ja": _JA, "zh-CN": _ZH_CN}
)

# Between the INVALID_CONFIG sentence and last_error: fr has a no-break space (U+00A0) before
# the colon, ja and zh-CN a full-width colon (U+FF1A). Built with chr() so no editor can
# silently turn them into plain ASCII.
_JOINERS: Mapping[str, str] = MappingProxyType(
    {"fr": chr(0xA0) + ": ", "ja": chr(0xFF1A), "zh-CN": chr(0xFF1A)}
)

# The tunnel's INVALID_SETTINGS last_error: it adds nothing to the sentence.
_GENERIC_INVALID = "The tunnel settings are invalid."

# JS `/:\s+/g`, with JS's whitespace set rather than re's `\s`.
_COLON_WS = re.compile(":[" + re.escape(JS_WHITESPACE) + "]+")


def _resolve_locale(locale: object) -> str:
    """Exact tag (case-insensitive, "_" read as "-"), then the language subtag ("de-AT" is
    "de", any "zh-*" is "zh-CN"), then "en"."""
    if not isinstance(locale, str):
        return "en"
    tag = js_trim(locale).replace("_", "-").lower()
    for loc in STATUS_LOCALES:
        if loc.lower() == tag:
            return loc
    lang = tag.split("-")[0]
    for loc in STATUS_LOCALES:
        if loc.lower().split("-")[0] == lang:
            return loc
    return "en"


def _merge(base: Mapping[str, str], override: object) -> Mapping[str, str]:
    """`base` with the keys it already has replaced by `override`'s str values."""
    if not isinstance(override, Mapping):
        return base
    out = dict(base)
    for k in base:
        v = override.get(k)
        if isinstance(v, str):
            out[k] = v
    return out


def _field(status: TunnelStatus | Mapping[str, Any], *keys: str) -> object:
    if isinstance(status, Mapping):
        for k in keys:
            if k in status:
                return status[k]
        return None
    return getattr(status, keys[-1], None)


def describe_status(
    status: TunnelStatus | Mapping[str, Any],
    locale: str | None = None,
    messages: StatusMessages | Mapping[str, Mapping[str, str]] | None = None,
) -> StatusText:
    """`label` is the state; `detail` the code's sentence (for INVALID_CONFIG followed by a
    per-locale colon and `last_error`, which names the field in English), or `last_error` when
    there is no known code; left out when empty or equal to `label`. `messages` overrides single
    strings of the resolved locale. A mapping `status` may spell the key `lastError` or
    `last_error`. An unknown state is labelled `str(state)`, the Python reading of JS `String()`
    (so a missing state is "None", not "undefined"). Never raises.
    """
    loc = _resolve_locale(locale)
    table = _TABLES[loc]
    if isinstance(messages, Mapping):
        over_states: object = messages.get("states")
        over_codes: object = messages.get("codes")
    else:
        # By shape, so a StatusMessages from another vendored copy works too.
        over_states = getattr(messages, "states", None)
        over_codes = getattr(messages, "codes", None)
    states = _merge(table.states, over_states)
    codes = _merge(table.codes, over_codes)

    state = _field(status, "state")
    code = _field(status, "code")
    last_error = _field(status, "lastError", "last_error")

    label = states[state] if isinstance(state, str) and state in states else str(state)
    sentence = codes[code] if isinstance(code, str) and code in codes else None
    detail: object
    if sentence is None:
        detail = last_error
    elif code == TunnelStatusCode.INVALID_CONFIG and isinstance(last_error, str) and last_error:
        if last_error == _GENERIC_INVALID:
            detail = sentence
        elif last_error.startswith(sentence):
            detail = last_error
        else:
            joiner = _JOINERS.get(loc, ": ")
            detail = sentence + joiner + _COLON_WS.sub("; ", last_error)
    else:
        detail = sentence
    if isinstance(detail, str) and detail and detail != label:
        return StatusText(label, detail)
    return StatusText(label)
