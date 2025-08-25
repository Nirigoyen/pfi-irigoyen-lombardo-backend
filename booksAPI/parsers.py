# booksAPI/parsers.py
from __future__ import annotations
from xml.etree import ElementTree as ET
from typing import List, Tuple, Dict, Optional
import html

def _detect_default_ns(root: ET.Element) -> Optional[str]:
    """
    Recorre el árbol hasta encontrar el primer tag con namespace {uri}tag y devuelve la uri.
    (En LT el xmlns está en <ltml>, no en <response>, por eso no alcanza con root.tag).
    """
    for e in root.iter():
        if isinstance(e.tag, str) and e.tag.startswith("{"):
            # formato: "{uri}localname"
            return e.tag.split("}")[0][1:]
    return None

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = html.unescape(s).strip()
    # A veces aparece literalmente "![CDATA[ ... ]]" en el texto (no un CDATA real)
    if s.startswith("![CDATA["):
        s = s[len("![CDATA["):]
        if s.endswith("]]"):
            s = s[:-2]
        s = s.strip()
    return s

def _extract_fact_list_for_field(root: ET.Element, field_name: str) -> List[str]:
    """
    Busca <field name="...">...<factList><fact>...</fact>...
    Soporta documentos con y sin namespace por defecto (xmlns).
    """
    out: List[str] = []
    ns_uri = _detect_default_ns(root)

    if ns_uri:
        # Con namespace por defecto, todos los nodos relevantes vienen namespaced
        field_xpath = f".//{{{ns_uri}}}field[@name='{field_name}']"
        fact_xpath  = f".//{{{ns_uri}}}factList/{{{ns_uri}}}fact"
        fields = root.findall(field_xpath)
        for field in fields:
            for fact in field.findall(fact_xpath):
                t = _clean_text(fact.text)
                if t:
                    out.append(t)
    else:
        # Sin namespace (poco probable en LT, pero por las dudas)
        for field in root.findall(".//field"):
            if field.get("name") == field_name:
                for fact in field.findall(".//factList/fact"):
                    t = _clean_text(fact.text)
                    if t:
                        out.append(t)

    # Dedup conservando orden
    seen = set()
    return [x for x in out if not (x in seen or seen.add(x))]

def parse_librarything_xml(xml_bytes: bytes) -> Tuple[List[str], List[str]]:
    """
    Devuelve (characters, places) desde el XML de LibraryThing CK.
    """
    root = ET.fromstring(xml_bytes)
    characters = _extract_fact_list_for_field(root, "characternames")
    places = _extract_fact_list_for_field(root, "placesmentioned")
    return characters, places

# -------- util de diagnóstico opcional ----------
def debug_parse(xml_bytes: bytes) -> Dict[str, object]:
    root = ET.fromstring(xml_bytes)
    ns_uri = _detect_default_ns(root)
    # Listar los nombres de <field> que encontramos
    field_names = set()
    if ns_uri:
        for f in root.findall(f".//{{{ns_uri}}}field"):
            n = f.get("name")
            if n:
                field_names.add(n)
    else:
        for f in root.findall(".//field"):
            n = f.get("name")
            if n:
                field_names.add(n)

    chars, places = parse_librarything_xml(xml_bytes)
    return {
        "ns_uri": ns_uri,
        "field_names_found": sorted(field_names)[:50],
        "characters_preview": chars[:10],
        "characters_count": len(chars),
        "places_preview": places[:10],
        "places_count": len(places),
    }
