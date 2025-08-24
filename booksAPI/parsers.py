# app/parsers.py
from xml.etree import ElementTree as ET
from typing import List, Tuple

def _extract_fact_list_for_field(root: ET.Element, field_name: str) -> List[str]:
    """
    Busca <field name="..."><versionList><version>...<factList><fact>...</fact>
    Devuelve lista de strings (facts).
    """
    out: List[str] = []
    # campos están en una jerarquía, recorremos cualquier 'field' con name=field_name
    for field in root.findall(".//field"):
        if field.get("name") == field_name:
            for fact in field.findall(".//factList/fact"):
                text = (fact.text or "").strip()
                if text:
                    out.append(text)
    return out

def parse_librarything_xml(xml_bytes: bytes) -> Tuple[List[str], List[str]]:
    """
    Devuelve (characters, places)
    characters de field name="characternames"
    places de field name="placesmentioned"
    """
    root = ET.fromstring(xml_bytes)
    characters = _extract_fact_list_for_field(root, "characternames")
    places = _extract_fact_list_for_field(root, "placesmentioned")
    return (characters, places)
