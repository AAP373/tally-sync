"""
Tally client integration for the on-premise agent.

Architecture notes
------------------
- This module is responsible for talking to the local Tally Desktop instance
  running on the SME's LAN machine (typically via Tally's HTTP/XML or ODBC APIs).
- It should NOT contain any business logic or persistence; it only knows how
  to discover, query, and parse raw data from Tally into Python structures.
- Higher layers (see `extractor.py`) are responsible for converting raw records
  into normalized `shared.models` instances.

In a real deployment this module would:
- Discover the Tally port and company list.
- Expose high-level methods like `fetch_vouchers`, `fetch_ledgers`, etc.
- Handle connection failures and retry/backoff.
"""

from __future__ import annotations

from typing import Any, Dict, List

import logging
import requests
from lxml import etree


logger = logging.getLogger(__name__)


class TallyClient:
    """
    Thin wrapper around a Tally Desktop instance.

    The goal is to keep this API relatively stable while the underlying
    Tally communication details (ports, XML envelopes, etc.) can evolve.
    """

    def __init__(self, base_url: str = "http://localhost:9000"):
        """
        :param base_url: Base HTTP endpoint where Tally is exposed.
                         In practice, this may be configurable per deployment.
        """
        self.base_url = base_url.rstrip("/")
    

    def _post_xml(self, xml_body: str) -> str:
        """
        Low-level helper to send XML to Tally and return raw XML response.

        This is intentionally kept internal; higher-level methods should expose
        typed/parsed responses.
        """
        logger.debug("Sending XML to Tally: %s", xml_body[:200])
        resp = requests.post(
            self.base_url,
            data=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
            timeout=30,
        )
        resp.raise_for_status()
        logger.debug("Received XML from Tally: %s", resp.text[:200])
        return resp.text

    def health_check(self) -> bool:
        """
        Lightweight probe to confirm that Tally is reachable.

        For now this uses a simple HTTP GET; in a real implementation this
        may be a small XML request that Tally supports.
        """
        try:
            resp = requests.get(self.base_url, timeout=5)
            return resp.ok
        except Exception as exc:  # pragma: no cover - placeholder
            logger.warning("Tally health check failed: %s", exc)
            return False
            
            

    def get_daybook(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Fetch Daybook vouchers from Tally for the given date range.

        The request uses the standard Tally XML envelope and `Export` request.
        `from_date` and `to_date` are passed through as-is; in practice they
        should match Tally's expected date format (e.g. YYYYMMDD).
        """
        # NOTE: The core structure strictly follows the shape you specified.
        # Date variables are added via STATICVARIABLES, which is how Tally
        # commonly accepts parameters for reports.
        xml_body = f"""
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Daybook</REPORTNAME>
    <STATICVARIABLES>
      <SVFROMDATE>{from_date}</SVFROMDATE>
      <SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
""".strip()

        raw_xml = self._post_xml(xml_body)
        root = etree.fromstring(raw_xml.encode("utf-8"))

        vouchers: List[Dict[str, Any]] = []

        # Typical Tally structure: ENVELOPE/BODY/IMPORTDATA/REQUESTDATA/TALLYMESSAGE/VOUCHER
        for v in root.xpath(".//VOUCHER"):
            def text_or_empty(tag_name: str) -> str:
                el = v.find(tag_name)
                return el.text.strip() if el is not None and el.text is not None else ""

            ledger_entries: List[Dict[str, Any]] = []
            for le in v.findall(".//ALLLEDGERENTRIES.LIST"):
                ledger_name_el = le.find("LEDGERNAME")
                amount_el = le.find("AMOUNT")
                ledger_entries.append(
                    {
                        "LedgerName": (
                            ledger_name_el.text.strip()
                            if ledger_name_el is not None and ledger_name_el.text
                            else ""
                        ),
                        "Amount": float(amount_el.text) if amount_el is not None and amount_el.text else 0.0,
                    }
                )

            voucher_dict: Dict[str, Any] = {
                "GUID": text_or_empty("GUID"),
                "VoucherNumber": text_or_empty("VOUCHERNUMBER"),
                "VoucherType": text_or_empty("VOUCHERTYPE"),
                "Date": text_or_empty("DATE"),
                "Narration": text_or_empty("NARRATION"),
                "LedgerEntries": ledger_entries,
            }
            vouchers.append(voucher_dict)

        return vouchers


__all__ = ["TallyClient"]




def fetch_raw_ledgers(self) -> List[Dict[str, Any]]:
    xml_body = """
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>List of Accounts</REPORTNAME>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>""".strip()

    try:
        raw_xml = self._post_xml(xml_body)
        root = etree.fromstring(raw_xml.encode("utf-8"))
    except Exception as exc:
        logger.warning("fetch_raw_ledgers failed: %s", exc)
        return []

    ledgers = []
    for l in root.xpath(".//LEDGER"):
        name_el = l.find("NAME")
        parent_el = l.find("PARENT")
        guid_el = l.find("GUID")
        ledgers.append({
            "GUID": guid_el.text.strip() if guid_el is not None and guid_el.text else "",
            "Name": name_el.text.strip() if name_el is not None and name_el.text else "",
            "Parent": parent_el.text.strip() if parent_el is not None and parent_el.text else "",
        })
    return ledgers

def health_check(self) -> bool:
    xml_body = """
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>List of Companies</REPORTNAME>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>""".strip()
    try:
        self._post_xml(xml_body)
        return True
    except Exception as exc:
        logger.warning("Tally health check failed: %s", exc)
        return False

